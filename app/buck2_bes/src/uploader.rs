/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! A client for `google.devtools.build.v1.PublishBuildEvent`.
//!
//! Events are delivered on one `PublishBuildToolEventStream` per invocation, numbered from 1.
//! The backend acknowledges each event by sequence number once it has persisted it. Events stay
//! buffered until they are acknowledged; when the stream breaks, a new stream is opened and
//! every unacknowledged event is sent again, which backends deduplicate by sequence number.
//! This is the protocol Bazel's BES client implements.
//!
//! The upload runs in a background task so that publishing never blocks the build, and it is
//! best effort: failures are reported in the [`UploadOutcome`], never as a command failure.

use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;

use buck2_bes_proto::google::devtools::build::v1::BuildEvent;
use buck2_bes_proto::google::devtools::build::v1::BuildStatus;
use buck2_bes_proto::google::devtools::build::v1::OrderedBuildEvent;
use buck2_bes_proto::google::devtools::build::v1::PublishBuildToolEventStreamRequest;
use buck2_bes_proto::google::devtools::build::v1::PublishLifecycleEventRequest;
use buck2_bes_proto::google::devtools::build::v1::StreamId;
use buck2_bes_proto::google::devtools::build::v1::build_event;
use buck2_bes_proto::google::devtools::build::v1::build_status;
use buck2_bes_proto::google::devtools::build::v1::publish_build_event_client::PublishBuildEventClient;
use buck2_bes_proto::google::devtools::build::v1::publish_lifecycle_event_request::ServiceLevel;
use buck2_bes_proto::google::devtools::build::v1::stream_id::BuildComponent;
use buck2_error::BuckErrorContext;
use buck2_error::ErrorTag;
use dupe::Dupe;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Code;
use tonic::Status;
use tonic::metadata::AsciiMetadataKey;
use tonic::metadata::AsciiMetadataValue;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Certificate;
use tonic::transport::Channel;
use tonic::transport::ClientTlsConfig;
use tonic::transport::Endpoint;
use tonic::transport::Identity;

use crate::config::BesConfig;

type Client = PublishBuildEventClient<InterceptedService<Channel, HeadersInterceptor>>;

/// How the uploader retries a failed RPC.
#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    /// Consecutive failed attempts, without any event being acknowledged in between, after which
    /// the upload is abandoned.
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(5),
        }
    }
}

impl RetryPolicy {
    fn backoff(&self, attempt: u32) -> Duration {
        self.initial_backoff
            .saturating_mul(1 << attempt.min(16))
            .min(self.max_backoff)
    }
}

/// Identity of one invocation's streams.
#[derive(Clone, Debug)]
pub struct StreamIds {
    /// Identifies the build request; lifecycle events of the build use it alone.
    pub build_id: String,
    /// Identifies this invocation; backends show the invocation under this ID.
    pub invocation_id: String,
}

/// How the invocation ended, reported in the lifecycle events.
#[derive(Clone, Copy, Debug)]
pub struct InvocationStatus {
    pub success: bool,
    pub exit_code: i32,
}

/// What happened to the events handed to the uploader.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct UploadOutcome {
    /// Events the backend acknowledged.
    pub acknowledged: u64,
    /// Events given to the uploader that the backend did not acknowledge.
    pub unacknowledged: u64,
    /// Why the upload did not complete, if it did not.
    pub error: Option<String>,
}

impl UploadOutcome {
    pub fn is_complete(&self) -> bool {
        self.error.is_none() && self.unacknowledged == 0
    }
}

enum Message {
    Event(prost_types::Any),
    Finish(InvocationStatus),
}

#[derive(Default)]
struct Counters {
    /// Events accepted by `send` and not yet acknowledged.
    pending: AtomicUsize,
    acknowledged: AtomicU64,
    accepted: AtomicU64,
}

/// Handle to a background upload of one invocation's events.
pub struct BesUploader {
    sender: mpsc::UnboundedSender<Message>,
    counters: Arc<Counters>,
    task: JoinHandle<Result<(), UploadError>>,
}

impl BesUploader {
    /// Starts uploading in a background task on the current tokio runtime. Connecting is lazy,
    /// so this does no network I/O.
    pub async fn start(
        config: &BesConfig,
        ids: StreamIds,
        retry: RetryPolicy,
    ) -> buck2_error::Result<Self> {
        let client = connect(config).await?;
        let (sender, receiver) = mpsc::unbounded_channel();
        let counters = Arc::new(Counters::default());
        let task = tokio::spawn(
            Upload {
                client,
                ids,
                project_id: config.instance_name.clone(),
                keywords: config.keywords.clone(),
                lifecycle_events: config.lifecycle_events,
                retry,
                receiver,
                counters: counters.dupe(),
                unacked: VecDeque::new(),
                next_sequence_number: 1,
                status: None,
            }
            .run(),
        );
        Ok(Self {
            sender,
            counters,
            task,
        })
    }

    /// Queues a `bazel_event` payload. Never blocks.
    pub fn send(&self, payload: prost_types::Any) {
        if self.sender.send(Message::Event(payload)).is_ok() {
            self.counters.accepted.fetch_add(1, Ordering::Relaxed);
            self.counters.pending.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Number of events queued or in flight that the backend has not acknowledged yet.
    pub fn pending(&self) -> usize {
        self.counters.pending.load(Ordering::Relaxed)
    }

    /// Ends the stream and waits up to `timeout` for the backend to acknowledge everything.
    pub async fn finish(self, status: InvocationStatus, timeout: Duration) -> UploadOutcome {
        let Self {
            sender,
            counters,
            mut task,
        } = self;
        // The task may already have stopped; then it reports why below.
        let _ignored = sender.send(Message::Finish(status));
        let result = match tokio::time::timeout(timeout, &mut task).await {
            Ok(Ok(result)) => result,
            Ok(Err(join_error)) => Err(UploadError::Internal(join_error.to_string())),
            Err(_) => {
                task.abort();
                Err(UploadError::Timeout(timeout))
            }
        };
        let acknowledged = counters.acknowledged.load(Ordering::Relaxed);
        UploadOutcome {
            acknowledged,
            unacknowledged: counters.accepted.load(Ordering::Relaxed) - acknowledged,
            error: result.err().map(|e| e.to_string()),
        }
    }
}

#[derive(Debug)]
enum UploadError {
    Rpc {
        method: &'static str,
        status: Status,
    },
    Protocol(String),
    Timeout(Duration),
    Internal(String),
}

impl fmt::Display for UploadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UploadError::Rpc { method, status } => write!(
                f,
                "{method} failed: {:?}: {}",
                status.code(),
                status.message()
            ),
            UploadError::Protocol(message) => write!(f, "{message}"),
            UploadError::Timeout(timeout) => write!(
                f,
                "the backend did not acknowledge all events within {}s of the command finishing",
                timeout.as_secs_f64()
            ),
            UploadError::Internal(message) => write!(f, "internal error: {message}"),
        }
    }
}

impl UploadError {
    fn is_retryable(&self) -> bool {
        match self {
            UploadError::Rpc { status, .. } => matches!(
                status.code(),
                Code::Unavailable
                    | Code::DeadlineExceeded
                    | Code::ResourceExhausted
                    | Code::Aborted
                    | Code::Internal
                    | Code::Unknown
                    | Code::Cancelled
            ),
            // A stream that ended before every event was acknowledged.
            UploadError::Protocol(_) => true,
            UploadError::Timeout(_) | UploadError::Internal(_) => false,
        }
    }
}

struct Upload {
    client: Client,
    ids: StreamIds,
    project_id: String,
    /// Sent with the first event of the tool event stream.
    keywords: Vec<String>,
    lifecycle_events: bool,
    retry: RetryPolicy,
    receiver: mpsc::UnboundedReceiver<Message>,
    counters: Arc<Counters>,
    /// Events sent on the tool event stream and not yet acknowledged, in sequence order.
    unacked: VecDeque<OrderedBuildEvent>,
    next_sequence_number: i64,
    /// Set once the command has finished and the final event is queued.
    status: Option<InvocationStatus>,
}

enum StreamEnd {
    /// The backend acknowledged every event including the final one.
    Complete,
    /// The uploader handle was dropped without finishing the stream.
    Abandoned,
}

impl Upload {
    async fn run(mut self) -> Result<(), UploadError> {
        let result = self.run_inner().await;
        if let Err(e) = &result {
            tracing::warn!("Build Event Service upload failed: {e}");
        }
        result
    }

    async fn run_inner(&mut self) -> Result<(), UploadError> {
        if self.lifecycle_events {
            self.publish_lifecycle_event(
                self.build_stream(),
                1,
                build_event::Event::BuildEnqueued(build_event::BuildEnqueued { details: None }),
            )
            .await?;
            self.publish_lifecycle_event(
                self.invocation_stream(BuildComponent::Controller),
                1,
                build_event::Event::InvocationAttemptStarted(
                    build_event::InvocationAttemptStarted {
                        attempt_number: 1,
                        details: None,
                    },
                ),
            )
            .await?;
        }

        match self.publish_tool_events().await? {
            StreamEnd::Complete => {}
            StreamEnd::Abandoned => return Ok(()),
        }

        if self.lifecycle_events
            && let Some(status) = self.status
        {
            let status = BuildStatus {
                result: if status.success {
                    build_status::Result::CommandSucceeded
                } else {
                    build_status::Result::CommandFailed
                } as i32,
                final_invocation_id: self.ids.invocation_id.clone(),
                build_tool_exit_code: Some(status.exit_code),
                error_message: String::new(),
                details: None,
            };
            self.publish_lifecycle_event(
                self.invocation_stream(BuildComponent::Controller),
                2,
                build_event::Event::InvocationAttemptFinished(
                    build_event::InvocationAttemptFinished {
                        invocation_status: Some(status.clone()),
                        details: None,
                    },
                ),
            )
            .await?;
            self.publish_lifecycle_event(
                self.build_stream(),
                2,
                build_event::Event::BuildFinished(build_event::BuildFinished {
                    status: Some(status),
                    details: None,
                }),
            )
            .await?;
        }
        Ok(())
    }

    fn build_stream(&self) -> StreamId {
        StreamId {
            build_id: self.ids.build_id.clone(),
            invocation_id: String::new(),
            component: BuildComponent::Controller as i32,
        }
    }

    fn invocation_stream(&self, component: BuildComponent) -> StreamId {
        StreamId {
            build_id: self.ids.build_id.clone(),
            invocation_id: self.ids.invocation_id.clone(),
            component: component as i32,
        }
    }

    async fn publish_lifecycle_event(
        &mut self,
        stream_id: StreamId,
        sequence_number: i64,
        event: build_event::Event,
    ) -> Result<(), UploadError> {
        let request = PublishLifecycleEventRequest {
            service_level: ServiceLevel::Interactive as i32,
            build_event: Some(OrderedBuildEvent {
                stream_id: Some(stream_id),
                sequence_number,
                event: Some(BuildEvent {
                    event_time: Some(SystemTime::now().into()),
                    event: Some(event),
                }),
            }),
            stream_timeout: None,
            notification_keywords: self.notification_keywords(),
            project_id: self.project_id.clone(),
            check_preceding_lifecycle_events_present: false,
        };
        let mut attempt = 0;
        loop {
            match self.client.publish_lifecycle_event(request.clone()).await {
                Ok(_) => return Ok(()),
                Err(status) => {
                    let error = UploadError::Rpc {
                        method: "PublishLifecycleEvent",
                        status,
                    };
                    self.backoff_or_fail(&mut attempt, error).await?;
                }
            }
        }
    }

    fn notification_keywords(&self) -> Vec<String> {
        let mut keywords = vec!["protocol_name=BEP".to_owned()];
        keywords.extend(self.keywords.iter().cloned());
        keywords
    }

    async fn backoff_or_fail(
        &self,
        attempt: &mut u32,
        error: UploadError,
    ) -> Result<(), UploadError> {
        *attempt += 1;
        if !error.is_retryable() || *attempt >= self.retry.max_attempts {
            return Err(error);
        }
        let backoff = self.retry.backoff(*attempt - 1);
        tracing::debug!(
            "Build Event Service: retrying in {}ms after: {error}",
            backoff.as_millis()
        );
        tokio::time::sleep(backoff).await;
        Ok(())
    }

    async fn publish_tool_events(&mut self) -> Result<StreamEnd, UploadError> {
        let mut attempt = 0;
        loop {
            let acked_before = self.counters.acknowledged.load(Ordering::Relaxed);
            match self.publish_tool_event_stream().await {
                Ok(end) => return Ok(end),
                Err(error) => {
                    if self.counters.acknowledged.load(Ordering::Relaxed) > acked_before {
                        // The stream made progress, so this is a new failure rather than a
                        // persistent one.
                        attempt = 0;
                    }
                    self.backoff_or_fail(&mut attempt, error).await?;
                }
            }
        }
    }

    /// One attempt at the tool event stream: resend what is unacknowledged, then forward new
    /// events as they arrive, until everything including the final event is acknowledged.
    async fn publish_tool_event_stream(&mut self) -> Result<StreamEnd, UploadError> {
        let rpc_error = |status| UploadError::Rpc {
            method: "PublishBuildToolEventStream",
            status,
        };

        // Do not open a stream before there is something to send: backends may time out idle
        // streams, and the first event carries the notification keywords.
        if self.unacked.is_empty() {
            match self.receiver.recv().await {
                Some(message) => {
                    self.enqueue(message);
                }
                None => return Ok(StreamEnd::Abandoned),
            }
        }

        let (requests, request_stream) = mpsc::unbounded_channel();
        for event in &self.unacked {
            let _ignored = requests.send(self.request(event.clone()));
        }
        let mut requests = Some(requests);
        let mut responses = self
            .client
            .publish_build_tool_event_stream(UnboundedReceiverStream::new(request_stream))
            .await
            .map_err(rpc_error)?
            .into_inner();

        loop {
            tokio::select! {
                response = responses.next() => match response {
                    Some(Ok(ack)) => {
                        self.acknowledge(ack.sequence_number)?;
                        if self.status.is_some() && self.unacked.is_empty() {
                            // Half-close the request stream so the backend can end the call.
                            requests = None;
                        }
                    }
                    Some(Err(status)) => return Err(rpc_error(status)),
                    None => {
                        return if self.status.is_some() && self.unacked.is_empty() {
                            Ok(StreamEnd::Complete)
                        } else {
                            Err(UploadError::Protocol(format!(
                                "the backend closed the event stream with {} events unacknowledged",
                                self.unacked.len()
                            )))
                        };
                    }
                },
                message = self.receiver.recv(), if self.status.is_none() => match message {
                    Some(message) => {
                        let event = self.enqueue(message);
                        if let Some(requests) = &requests {
                            let _ignored = requests.send(self.request(event));
                        }
                    }
                    None => return Ok(StreamEnd::Abandoned),
                },
            }
        }
    }

    /// Assigns the next sequence number to a message and buffers it until acknowledged.
    fn enqueue(&mut self, message: Message) -> OrderedBuildEvent {
        let event = match message {
            Message::Event(payload) => build_event::Event::BazelEvent(payload),
            Message::Finish(status) => {
                self.status = Some(status);
                // The final event is not counted in `pending`, so undo the decrement its
                // acknowledgement will cause.
                self.counters.pending.fetch_add(1, Ordering::Relaxed);
                self.counters.accepted.fetch_add(1, Ordering::Relaxed);
                build_event::Event::ComponentStreamFinished(
                    build_event::BuildComponentStreamFinished {
                        r#type: build_event::build_component_stream_finished::FinishType::Finished
                            as i32,
                    },
                )
            }
        };
        let event = OrderedBuildEvent {
            stream_id: Some(self.invocation_stream(BuildComponent::Tool)),
            sequence_number: self.next_sequence_number,
            event: Some(BuildEvent {
                event_time: Some(SystemTime::now().into()),
                event: Some(event),
            }),
        };
        self.next_sequence_number += 1;
        self.unacked.push_back(event.clone());
        event
    }

    fn request(&self, event: OrderedBuildEvent) -> PublishBuildToolEventStreamRequest {
        PublishBuildToolEventStreamRequest {
            notification_keywords: if event.sequence_number == 1 {
                self.notification_keywords()
            } else {
                Vec::new()
            },
            ordered_build_event: Some(event),
            project_id: self.project_id.clone(),
            check_preceding_lifecycle_events_present: false,
        }
    }

    /// Acknowledgements are cumulative: an acknowledgement for sequence number N covers every
    /// event up to N. Repeated acknowledgements, as after a retry, are ignored.
    fn acknowledge(&mut self, sequence_number: i64) -> Result<(), UploadError> {
        if sequence_number >= self.next_sequence_number {
            return Err(UploadError::Protocol(format!(
                "the backend acknowledged event {sequence_number}, which was never sent"
            )));
        }
        while let Some(front) = self.unacked.front() {
            if front.sequence_number > sequence_number {
                break;
            }
            self.unacked.pop_front();
            self.counters.pending.fetch_sub(1, Ordering::Relaxed);
            self.counters.acknowledged.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}

async fn connect(config: &BesConfig) -> buck2_error::Result<Client> {
    let mut endpoint = Endpoint::from_shared(config.endpoint.clone())
        .buck_error_context("Invalid `buck2_bes.backend`")?
        .connect_timeout(Duration::from_secs(10))
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_timeout(Duration::from_secs(10));
    if config.tls {
        let mut tls = ClientTlsConfig::new().with_enabled_roots();
        if let Some(path) = &config.tls_ca_certs {
            let pem = tokio::fs::read(path)
                .await
                .with_buck_error_context(|| format!("Error reading `{path}`"))?;
            tls = tls.ca_certificate(Certificate::from_pem(pem));
        }
        if let Some(path) = &config.tls_client_cert {
            let pem = tokio::fs::read(path)
                .await
                .with_buck_error_context(|| format!("Error reading `{path}`"))?;
            tls = tls.identity(Identity::from_pem(&pem, &pem));
        }
        endpoint = endpoint
            .tls_config(tls)
            .buck_error_context("Invalid TLS configuration for `buck2_bes`")?;
    }
    let interceptor = HeadersInterceptor::new(&config.http_headers)?;
    Ok(PublishBuildEventClient::with_interceptor(
        endpoint.connect_lazy(),
        interceptor,
    ))
}

#[derive(Clone, Dupe)]
struct HeadersInterceptor {
    headers: Arc<Vec<(AsciiMetadataKey, AsciiMetadataValue)>>,
}

impl HeadersInterceptor {
    fn new(headers: &[(String, String)]) -> buck2_error::Result<Self> {
        let headers = headers
            .iter()
            .map(|(key, value)| {
                // Do not include the value in errors: it usually holds a credential.
                let key_parsed = AsciiMetadataKey::from_bytes(key.as_bytes()).map_err(|_| {
                    buck2_error::buck2_error!(ErrorTag::Input, "Invalid header name `{key}`")
                })?;
                let value = AsciiMetadataValue::try_from(value.as_str()).map_err(|_| {
                    buck2_error::buck2_error!(ErrorTag::Input, "Invalid value for header `{key}`")
                })?;
                buck2_error::Ok((key_parsed, value))
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            headers: Arc::new(headers),
        })
    }
}

impl Interceptor for HeadersInterceptor {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        for (key, value) in self.headers.iter() {
            request.metadata_mut().insert(key.clone(), value.clone());
        }
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Mutex;

    use buck2_bes_proto::google::devtools::build::v1::PublishBuildToolEventStreamResponse;
    use buck2_bes_proto::google::devtools::build::v1::publish_build_event_server::PublishBuildEvent;
    use buck2_bes_proto::google::devtools::build::v1::publish_build_event_server::PublishBuildEventServer;
    use futures::Stream;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::Request;
    use tonic::Response;
    use tonic::Streaming;
    use tonic::transport::Server;

    use super::*;

    #[derive(Clone, Copy)]
    enum Behavior {
        Ack,
        /// The first tool event stream fails with `UNAVAILABLE` after acknowledging this many
        /// events.
        FailFirstStreamAfter(usize),
        /// Tool events are received but never acknowledged.
        NeverAck,
        /// Every lifecycle event is rejected with this code.
        RejectLifecycle(Code),
    }

    #[derive(Debug, PartialEq)]
    enum Received {
        Lifecycle {
            kind: &'static str,
            sequence_number: i64,
            invocation_id: String,
        },
        Tool {
            sequence_number: i64,
            event: build_event::Event,
        },
    }

    #[derive(Default)]
    struct Log {
        received: Vec<Received>,
        /// The first sequence number of each tool event stream.
        stream_starts: Vec<i64>,
        lifecycle_attempts: usize,
        auth_headers: Vec<Option<String>>,
    }

    struct FakeBes {
        behavior: Behavior,
        log: Arc<Mutex<Log>>,
    }

    fn auth<T>(request: &Request<T>) -> Option<String> {
        request
            .metadata()
            .get("x-test-auth")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.to_owned())
    }

    #[tonic::async_trait]
    impl PublishBuildEvent for FakeBes {
        async fn publish_lifecycle_event(
            &self,
            request: Request<PublishLifecycleEventRequest>,
        ) -> Result<Response<()>, Status> {
            let mut log = self.log.lock().unwrap();
            log.lifecycle_attempts += 1;
            log.auth_headers.push(auth(&request));
            if let Behavior::RejectLifecycle(code) = self.behavior {
                return Err(Status::new(code, "rejected"));
            }
            let event = request.into_inner().build_event.unwrap();
            let kind = match event.event.unwrap().event.unwrap() {
                build_event::Event::BuildEnqueued(_) => "BuildEnqueued",
                build_event::Event::InvocationAttemptStarted(_) => "InvocationAttemptStarted",
                build_event::Event::InvocationAttemptFinished(_) => "InvocationAttemptFinished",
                build_event::Event::BuildFinished(_) => "BuildFinished",
                _ => "other",
            };
            log.received.push(Received::Lifecycle {
                kind,
                sequence_number: event.sequence_number,
                invocation_id: event.stream_id.unwrap().invocation_id,
            });
            Ok(Response::new(()))
        }

        type PublishBuildToolEventStreamStream =
            Pin<Box<dyn Stream<Item = Result<PublishBuildToolEventStreamResponse, Status>> + Send>>;

        async fn publish_build_tool_event_stream(
            &self,
            request: Request<Streaming<PublishBuildToolEventStreamRequest>>,
        ) -> Result<Response<Self::PublishBuildToolEventStreamStream>, Status> {
            let first_stream = {
                let mut log = self.log.lock().unwrap();
                log.auth_headers.push(auth(&request));
                log.stream_starts.is_empty()
            };
            let mut requests = request.into_inner();
            let (acks, ack_stream) = mpsc::channel(100);
            let log = self.log.dupe();
            let behavior = self.behavior;
            tokio::spawn(async move {
                let mut acked = 0;
                while let Some(Ok(request)) = requests.next().await {
                    let event = request.ordered_build_event.unwrap();
                    {
                        let mut log = log.lock().unwrap();
                        if acked == 0 && !matches!(behavior, Behavior::NeverAck) {
                            log.stream_starts.push(event.sequence_number);
                        }
                        log.received.push(Received::Tool {
                            sequence_number: event.sequence_number,
                            event: event.event.unwrap().event.unwrap(),
                        });
                    }
                    match behavior {
                        Behavior::NeverAck => continue,
                        Behavior::FailFirstStreamAfter(n) if first_stream && acked == n => {
                            let _ignored = acks.send(Err(Status::unavailable("restarting"))).await;
                            return;
                        }
                        _ => {}
                    }
                    acked += 1;
                    let ack = PublishBuildToolEventStreamResponse {
                        stream_id: event.stream_id,
                        sequence_number: event.sequence_number,
                    };
                    if acks.send(Ok(ack)).await.is_err() {
                        return;
                    }
                }
            });
            Ok(Response::new(Box::pin(ReceiverStream::new(ack_stream))))
        }
    }

    async fn serve(behavior: Behavior) -> (String, Arc<Mutex<Log>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let log = Arc::new(Mutex::new(Log::default()));
        let service = FakeBes {
            behavior,
            log: log.dupe(),
        };
        tokio::spawn(async move {
            Server::builder()
                .add_service(PublishBuildEventServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        (format!("http://{address}"), log)
    }

    fn config(endpoint: String) -> BesConfig {
        BesConfig {
            endpoint,
            tls: false,
            tls_ca_certs: None,
            tls_client_cert: None,
            http_headers: vec![("x-test-auth".to_owned(), "Bearer t0k3n".to_owned())],
            results_url: None,
            instance_name: "project".to_owned(),
            keywords: Vec::new(),
            lifecycle_events: true,
            upload_timeout: Duration::from_secs(10),
            max_buffered_events: 100,
            publish_all_actions: true,
        }
    }

    fn ids() -> StreamIds {
        StreamIds {
            build_id: "build".to_owned(),
            invocation_id: "invocation".to_owned(),
        }
    }

    fn fast_retries() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(10),
        }
    }

    fn payload(i: u8) -> prost_types::Any {
        prost_types::Any {
            type_url: "test".to_owned(),
            value: vec![i],
        }
    }

    const SUCCESS: InvocationStatus = InvocationStatus {
        success: true,
        exit_code: 0,
    };

    /// The tool events the backend received, deduplicated by sequence number.
    fn tool_events(log: &Log) -> BTreeMap<i64, &build_event::Event> {
        log.received
            .iter()
            .filter_map(|r| match r {
                Received::Tool {
                    sequence_number,
                    event,
                } => Some((*sequence_number, event)),
                _ => None,
            })
            .collect()
    }

    use std::collections::BTreeMap;

    #[tokio::test]
    async fn test_delivers_ordered_events_between_lifecycle_events() {
        let (endpoint, log) = serve(Behavior::Ack).await;
        let uploader = BesUploader::start(&config(endpoint), ids(), fast_retries())
            .await
            .unwrap();
        for i in 0..5 {
            uploader.send(payload(i));
        }
        let outcome = uploader.finish(SUCCESS, Duration::from_secs(10)).await;
        assert_eq!(
            UploadOutcome {
                acknowledged: 6,
                unacknowledged: 0,
                error: None
            },
            outcome
        );

        let log = log.lock().unwrap();
        let lifecycle = |kind, sequence_number, invocation_id: &str| Received::Lifecycle {
            kind,
            sequence_number,
            invocation_id: invocation_id.to_owned(),
        };
        let mut expected = vec![
            lifecycle("BuildEnqueued", 1, ""),
            lifecycle("InvocationAttemptStarted", 1, "invocation"),
        ];
        for i in 0..5 {
            expected.push(Received::Tool {
                sequence_number: i as i64 + 1,
                event: build_event::Event::BazelEvent(payload(i)),
            });
        }
        expected.push(Received::Tool {
            sequence_number: 6,
            event: build_event::Event::ComponentStreamFinished(
                build_event::BuildComponentStreamFinished {
                    r#type: build_event::build_component_stream_finished::FinishType::Finished
                        as i32,
                },
            ),
        });
        expected.push(lifecycle("InvocationAttemptFinished", 2, "invocation"));
        expected.push(lifecycle("BuildFinished", 2, ""));
        assert_eq!(expected, log.received);
        assert!(
            log.auth_headers
                .iter()
                .all(|h| h.as_deref() == Some("Bearer t0k3n"))
        );
    }

    #[tokio::test]
    async fn test_resends_unacknowledged_events_on_a_new_stream() {
        let (endpoint, log) = serve(Behavior::FailFirstStreamAfter(2)).await;
        let uploader = BesUploader::start(&config(endpoint), ids(), fast_retries())
            .await
            .unwrap();
        for i in 0..5 {
            uploader.send(payload(i));
        }
        let outcome = uploader.finish(SUCCESS, Duration::from_secs(10)).await;
        assert!(outcome.is_complete(), "{outcome:?}");
        assert_eq!(6, outcome.acknowledged);

        let log = log.lock().unwrap();
        // The second stream starts from the first event the backend did not acknowledge.
        assert_eq!(vec![1, 3], log.stream_starts);
        let events = tool_events(&log);
        assert_eq!(
            (1..=6).collect::<Vec<_>>(),
            events.keys().copied().collect::<Vec<_>>()
        );
        for i in 0..5 {
            assert_eq!(
                &build_event::Event::BazelEvent(payload(i)),
                events[&(i as i64 + 1)]
            );
        }
    }

    #[tokio::test]
    async fn test_does_not_retry_permanent_errors() {
        let (endpoint, log) = serve(Behavior::RejectLifecycle(Code::Unauthenticated)).await;
        let uploader = BesUploader::start(&config(endpoint), ids(), fast_retries())
            .await
            .unwrap();
        uploader.send(payload(0));
        let outcome = uploader.finish(SUCCESS, Duration::from_secs(10)).await;
        assert_eq!(0, outcome.acknowledged);
        assert!(
            outcome
                .error
                .as_deref()
                .unwrap()
                .contains("Unauthenticated"),
            "{outcome:?}"
        );
        assert_eq!(1, log.lock().unwrap().lifecycle_attempts);
    }

    #[tokio::test]
    async fn test_retries_transient_errors_then_gives_up() {
        let (endpoint, log) = serve(Behavior::RejectLifecycle(Code::Unavailable)).await;
        let uploader = BesUploader::start(&config(endpoint), ids(), fast_retries())
            .await
            .unwrap();
        let outcome = uploader.finish(SUCCESS, Duration::from_secs(10)).await;
        assert!(!outcome.is_complete());
        assert_eq!(3, log.lock().unwrap().lifecycle_attempts);
    }

    #[tokio::test]
    async fn test_finish_is_bounded_by_the_timeout() {
        let (endpoint, log) = serve(Behavior::NeverAck).await;
        let uploader = BesUploader::start(&config(endpoint), ids(), fast_retries())
            .await
            .unwrap();
        for i in 0..3 {
            uploader.send(payload(i));
        }
        // Wait until the backend has the events, so that `pending` reflects missing
        // acknowledgements rather than events still in flight.
        while tool_events(&log.lock().unwrap()).len() < 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(3, uploader.pending());

        let start = std::time::Instant::now();
        let outcome = uploader.finish(SUCCESS, Duration::from_millis(200)).await;
        assert!(start.elapsed() < Duration::from_secs(5));
        assert_eq!(0, outcome.acknowledged);
        assert_eq!(4, outcome.unacknowledged);
        assert!(outcome.error.unwrap().contains("did not acknowledge"));
    }

    #[tokio::test]
    async fn test_unreachable_backend_fails_without_lifecycle_events() {
        // Bind and drop a listener to get a port nothing listens on.
        let address = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap()
            .local_addr()
            .unwrap();
        let mut config = config(format!("http://{address}"));
        config.lifecycle_events = false;
        let uploader = BesUploader::start(&config, ids(), fast_retries())
            .await
            .unwrap();
        uploader.send(payload(0));
        let outcome = uploader.finish(SUCCESS, Duration::from_secs(10)).await;
        assert_eq!(0, outcome.acknowledged);
        assert!(
            outcome.error.as_deref().unwrap().contains("Unavailable"),
            "{outcome:?}"
        );
    }
}

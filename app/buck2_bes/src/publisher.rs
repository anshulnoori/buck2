/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::sync::Arc;

use buck2_bes_proto::BAZEL_EVENT_TYPE_URL;
use buck2_bes_proto::build_event_stream as bep;
use buck2_error::ExitCode;
use buck2_events::BuckEvent;
use prost::Message;

use crate::bep::BepConverter;
use crate::bep::ConverterOptions;
use crate::bep::InvocationInfo;
use crate::config::BesConfig;
use crate::uploader::BesUploader;
use crate::uploader::InvocationStatus;
use crate::uploader::RetryPolicy;
use crate::uploader::StreamIds;

enum State {
    NotStarted,
    Started(BesUploader),
    Failed(String),
}

/// Publishes one invocation to a Build Event Service: converts its events to BEP and uploads
/// them in the background.
pub struct BesPublisher {
    config: BesConfig,
    ids: StreamIds,
    converter: BepConverter,
    state: State,
}

impl BesPublisher {
    pub fn new(config: BesConfig, info: InvocationInfo) -> Self {
        let ids = StreamIds {
            build_id: uuid::Uuid::new_v4().to_string(),
            invocation_id: info.invocation_id.clone(),
        };
        let converter = BepConverter::new(
            info,
            ConverterOptions {
                publish_all_actions: config.publish_all_actions,
            },
        );
        Self {
            config,
            ids,
            converter,
            state: State::NotStarted,
        }
    }

    /// Where the backend shows this invocation, if `buck2_bes.results_url` is set.
    pub fn invocation_url(&self) -> Option<String> {
        self.config.invocation_url(&self.ids.invocation_id)
    }

    /// Starts the upload on first use. Needs a tokio runtime, which is why it is not done in
    /// `new`.
    async fn uploader(&mut self) -> Option<&BesUploader> {
        if let State::NotStarted = self.state {
            self.state =
                match BesUploader::start(&self.config, self.ids.clone(), RetryPolicy::default())
                    .await
                {
                    Ok(uploader) => {
                        for event in self.converter.start() {
                            uploader.send(pack(&event));
                        }
                        State::Started(uploader)
                    }
                    Err(e) => State::Failed(format!("{e:#}")),
                };
        }
        match &self.state {
            State::Started(uploader) => Some(uploader),
            _ => None,
        }
    }

    pub async fn handle_events(&mut self, events: &[Arc<BuckEvent>]) {
        let max_buffered_events = self.config.max_buffered_events;
        let Some(uploader) = self.uploader().await else {
            return;
        };
        let shedding = uploader.pending() >= max_buffered_events;
        self.converter.set_shedding(shedding);
        for event in events {
            self.converter.handle_event(event);
        }
        self.send(BepConverter::flush);
    }

    pub async fn handle_command_result(&mut self, result: &buck2_cli_proto::CommandResult) {
        if self.uploader().await.is_none() {
            return;
        }
        self.converter.handle_command_result(result);
        self.send(BepConverter::flush);
    }

    fn send(&mut self, produce: impl FnOnce(&mut BepConverter) -> Vec<bep::BuildEvent>) {
        let events = produce(&mut self.converter);
        if let State::Started(uploader) = &self.state {
            for event in &events {
                uploader.send(pack(event));
            }
        }
    }

    /// Publishes the end of the invocation and waits, at most `buck2_bes.upload_timeout_secs`,
    /// for the backend to acknowledge everything. Returns a warning for the user if the upload
    /// is incomplete.
    pub async fn finish(mut self, exit_code: Option<ExitCode>) -> Option<String> {
        self.uploader().await;
        let events = self.converter.finish(exit_code);
        let uploader = match self.state {
            State::Started(uploader) => uploader,
            State::Failed(error) => {
                return Some(format!(
                    "Build events were not published to `{}`: {error}",
                    self.config.endpoint
                ));
            }
            State::NotStarted => return None,
        };
        for event in &events {
            uploader.send(pack(event));
        }
        let success = matches!(
            exit_code,
            None | Some(ExitCode::Success) | Some(ExitCode::TestRunner(0))
        );
        let outcome = uploader
            .finish(
                InvocationStatus {
                    success,
                    exit_code: exit_code.map_or(0, |c| c.exit_code() as i32),
                },
                self.config.upload_timeout,
            )
            .await;
        if outcome.is_complete() {
            return None;
        }
        Some(format!(
            "Build events were not fully published to `{}`: {} ({} of {} events acknowledged)",
            self.config.endpoint,
            outcome.error.as_deref().unwrap_or("unknown error"),
            outcome.acknowledged,
            outcome.acknowledged + outcome.unacknowledged,
        ))
    }
}

fn pack(event: &bep::BuildEvent) -> prost_types::Any {
    prost_types::Any {
        type_url: BAZEL_EVENT_TYPE_URL.to_owned(),
        value: event.encode_to_vec(),
    }
}

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Translation of a Buck2 invocation's events into Bazel's Build Event Protocol (BEP).
//!
//! Existing Build Event Service user interfaces understand BEP, not `BuckEvent`, so this maps the
//! parts of an invocation that BEP has a place for: the command line, top-level targets, actions,
//! test results, console errors, metrics and the outcome. Everything else is left out.
//!
//! BEP events form a tree: every event except `BuildStarted` must be announced as a child of an
//! earlier event. Bazel announces events it cannot predict from a chain of `Progress` events, each
//! of which announces the next `Progress`; this converter does the same. Each call to
//! [`BepConverter::flush`] posts one `Progress` that announces the events collected since the last
//! flush, followed by those events.
//!
//! Labels and configurations keep their Buck2 spelling, e.g. `root//foo:bar` and
//! `prelude//platforms:default#<hash>`.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;
use std::time::SystemTime;

use buck2_bes_proto::build_event_stream as bep;
use buck2_bes_proto::build_event_stream::build_event::Payload;
use buck2_bes_proto::build_event_stream::build_event_id;
use buck2_bes_proto::build_event_stream::build_event_id::Id;
use buck2_bes_proto::command_line;
use buck2_bes_proto::failure_details::FailureDetail;
use buck2_data::ActionExecutionKind;
use buck2_data::ConfiguredTargetLabel;
use buck2_data::TestStatus;
use buck2_data::action_key;
use buck2_error::ExitCode;
use buck2_event_observer::display::TargetDisplayOptions;
use buck2_event_observer::display::display_action_identity;
use buck2_event_observer::display::display_anon_target;
use buck2_event_observer::display::display_bxl_key;
use buck2_event_observer::display::get_action_error_reason;
use buck2_event_observer::unpack_event::UnpackedBuckEvent;
use buck2_event_observer::unpack_event::unpack_event;
use buck2_events::BuckEvent;

/// Largest action output or test log embedded in an event. BES backends commonly limit a
/// whole request to a few MiB.
const MAX_INLINE_OUTPUT_BYTES: usize = 64 * 1024;

/// What is known about the invocation before any event arrives.
#[derive(Clone, Debug, Default)]
pub struct InvocationInfo {
    pub invocation_id: String,
    /// The Buck2 subcommand, e.g. `build` or `test`.
    pub command: String,
    pub start_time: Option<SystemTime>,
    pub build_tool_version: String,
    /// The command line, starting with the executable.
    pub args: Vec<String>,
    pub working_directory: String,
    pub workspace_directory: String,
    pub user: String,
    pub host: String,
    /// `--client-metadata` values, published as BEP build metadata.
    pub metadata: Vec<(String, String)>,
}

#[derive(Clone, Copy, Debug)]
pub struct ConverterOptions {
    /// Publish `ActionExecuted` for successful actions that ran a command, not only for failed
    /// ones.
    pub publish_all_actions: bool,
}

/// A target and the configuration it was built in.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct ConfiguredTarget {
    label: String,
    configuration: Option<String>,
}

impl ConfiguredTarget {
    fn from_proto(target: &ConfiguredTargetLabel) -> Option<Self> {
        let label = target.label.as_ref()?;
        Some(Self {
            label: format!("{}:{}", label.package, label.name),
            configuration: target.configuration.as_ref().map(|c| c.full_name.clone()),
        })
    }

    fn configuration_id(&self) -> Option<build_event_id::ConfigurationId> {
        self.configuration
            .as_ref()
            .map(|id| build_event_id::ConfigurationId { id: id.clone() })
    }
}

#[derive(Default)]
struct TestTarget {
    start: Option<SystemTime>,
    end: Option<SystemTime>,
    exit_code: Option<i32>,
    strategy: String,
    cases: Vec<TestCase>,
}

struct TestCase {
    name: String,
    status: TestStatus,
    details: String,
}

#[derive(Default)]
struct ActionData {
    executed: i64,
    first_started_ms: i64,
    last_ended_ms: i64,
}

#[derive(Default)]
struct Metrics {
    actions_executed: i64,
    by_mnemonic: BTreeMap<String, ActionData>,
    /// Keyed by runner name; values are the kind of execution and the count.
    runners: BTreeMap<&'static str, (&'static str, i32)>,
}

/// Converts the events of one invocation to BEP. See the module documentation.
pub struct BepConverter {
    info: InvocationInfo,
    options: ConverterOptions,
    /// The `Progress` event that has been announced but not posted yet.
    next_progress: i32,
    /// Events to post after the next `Progress`, with whether that `Progress` must announce them.
    pending: Vec<(bep::BuildEvent, bool)>,
    stderr: String,
    configurations_posted: HashSet<String>,
    constraints: HashMap<String, Vec<(String, String)>>,
    patterns_posted: bool,
    rule_types: HashMap<String, String>,
    /// Failure reasons from `ActionError` events, by action identity, until the action ends.
    action_error_reasons: HashMap<String, String>,
    /// How often each action, by label and name, has been published.
    action_runs: HashMap<(String, String), u32>,
    /// The first failure of each target's own actions.
    failures: HashMap<ConfiguredTarget, String>,
    tests: BTreeMap<ConfiguredTarget, TestTarget>,
    targets_posted: HashSet<ConfiguredTarget>,
    metrics: Metrics,
    shedding: bool,
    shed: u64,
    finished: bool,
}

impl BepConverter {
    pub fn new(info: InvocationInfo, options: ConverterOptions) -> Self {
        Self {
            info,
            options,
            next_progress: 0,
            pending: Vec::new(),
            stderr: String::new(),
            configurations_posted: HashSet::new(),
            constraints: HashMap::new(),
            patterns_posted: false,
            rule_types: HashMap::new(),
            action_error_reasons: HashMap::new(),
            action_runs: HashMap::new(),
            failures: HashMap::new(),
            tests: BTreeMap::new(),
            targets_posted: HashSet::new(),
            metrics: Metrics::default(),
            shedding: false,
            shed: 0,
            finished: false,
        }
    }

    /// The events that open the stream: `BuildStarted` and what it announces about the command.
    pub fn start(&mut self) -> Vec<bep::BuildEvent> {
        let info = &self.info;
        let started = bep::BuildEvent {
            id: Some(id(Id::Started(build_event_id::BuildStartedId {}))),
            children: vec![
                id(Id::UnstructuredCommandLine(
                    build_event_id::UnstructuredCommandLineId {},
                )),
                original_command_line_id(),
                id(Id::WorkspaceStatus(build_event_id::WorkspaceStatusId {})),
                id(Id::BuildMetadata(build_event_id::BuildMetadataId {})),
                progress_id(0),
                id(Id::BuildFinished(build_event_id::BuildFinishedId {})),
            ],
            last_message: false,
            payload: Some(Payload::Started(bep::BuildStarted {
                uuid: info.invocation_id.clone(),
                start_time: info.start_time.map(Into::into),
                build_tool_version: info.build_tool_version.clone(),
                command: info.command.clone(),
                working_directory: info.working_directory.clone(),
                workspace_directory: info.workspace_directory.clone(),
                host: info.host.clone(),
                user: info.user.clone(),
                ..Default::default()
            })),
        };

        let mut chunks = info.args.iter().cloned();
        let executable = chunks.next().unwrap_or_else(|| "buck2".to_owned());
        let options: Vec<String> = chunks.skip(1).collect();
        let structured = command_line::CommandLine {
            command_line_label: "original".to_owned(),
            sections: vec![
                chunk_section("executable", vec![executable]),
                chunk_section("command", vec![info.command.clone()]),
                chunk_section("command options", options),
            ],
        };

        let mut workspace_status = vec![
            ("BUILD_USER", info.user.clone()),
            ("BUILD_HOST", info.host.clone()),
        ];
        workspace_status.retain(|(_, v)| !v.is_empty());

        vec![
            started,
            event(
                Id::UnstructuredCommandLine(build_event_id::UnstructuredCommandLineId {}),
                Payload::UnstructuredCommandLine(bep::UnstructuredCommandLine {
                    args: info.args.clone(),
                }),
            ),
            bep::BuildEvent {
                id: Some(original_command_line_id()),
                children: Vec::new(),
                last_message: false,
                payload: Some(Payload::StructuredCommandLine(structured)),
            },
            event(
                Id::WorkspaceStatus(build_event_id::WorkspaceStatusId {}),
                Payload::WorkspaceStatus(bep::WorkspaceStatus {
                    item: workspace_status
                        .into_iter()
                        .map(|(key, value)| bep::workspace_status::Item {
                            key: key.to_owned(),
                            value,
                        })
                        .collect(),
                }),
            ),
            event(
                Id::BuildMetadata(build_event_id::BuildMetadataId {}),
                Payload::BuildMetadata(bep::BuildMetadata {
                    metadata: info.metadata.iter().cloned().collect(),
                }),
            ),
        ]
    }

    /// While set, successful actions and console output are not published, so that a backend
    /// that falls behind does not make the client buffer an unbounded number of events.
    pub fn set_shedding(&mut self, shedding: bool) {
        self.shedding = shedding;
    }

    /// Number of events not published because of [`Self::set_shedding`].
    pub fn shed(&self) -> u64 {
        self.shed
    }

    pub fn handle_event(&mut self, event: &BuckEvent) {
        if self.finished {
            return;
        }
        // Events that cannot be unpacked carry nothing this converter uses.
        let Ok(unpacked) = unpack_event(event) else {
            return;
        };
        match unpacked {
            UnpackedBuckEvent::SpanEnd(_, end, data) => {
                let duration = end
                    .duration
                    .and_then(|d| Duration::try_from(d).ok())
                    .unwrap_or_default();
                match data {
                    buck2_data::span_end_event::Data::ActionExecution(action) => {
                        self.handle_action(event.timestamp(), duration, action)
                    }
                    buck2_data::span_end_event::Data::TestRun(run) => {
                        self.handle_test_run(event.timestamp(), duration, run)
                    }
                    _ => {}
                }
            }
            UnpackedBuckEvent::Instant(_, _, data) => {
                use buck2_data::instant_event::Data;
                match data {
                    Data::TargetPatterns(patterns) => self.handle_patterns(patterns),
                    Data::ConfigurationCreated(created) => {
                        if let Some(cfg) = &created.cfg {
                            self.constraints.insert(
                                cfg.full_name.clone(),
                                cfg.constraint
                                    .iter()
                                    .map(|c| (c.setting.clone(), c.value.clone()))
                                    .collect(),
                            );
                        }
                    }
                    Data::ActionError(error) => self.handle_action_error(error),
                    Data::TestResult(result) => self.handle_test_result(event.timestamp(), result),
                    Data::EndOfTestResults(_) => self.post_test_targets(),
                    Data::ConsoleMessage(message) => self.log(&message.message),
                    Data::ConsoleWarning(warning) => self.log(&warning.message),
                    Data::StructuredError(error)
                        if !error.quiet && error.soft_error_category.is_none() =>
                    {
                        self.log(&error.payload)
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    /// Takes the top-level targets of a build from the command's result.
    pub fn handle_command_result(&mut self, result: &buck2_cli_proto::CommandResult) {
        use buck2_cli_proto::command_result::Result;
        if self.finished {
            return;
        }
        if let Some(Result::BuildResponse(response)) = &result.result {
            for target in &response.build_targets {
                let configured = ConfiguredTarget {
                    label: target.target.clone(),
                    configuration: Some(target.configuration.clone()).filter(|c| !c.is_empty()),
                };
                if let Some(rule) = &target.target_rule_type_name {
                    self.rule_types
                        .insert(configured.label.clone(), rule.clone());
                }
                self.post_target(&configured, !target.failed, Vec::new());
            }
        }
    }

    /// Posts what has been collected since the last call, announced by one `Progress` event.
    pub fn flush(&mut self) -> Vec<bep::BuildEvent> {
        if self.finished || (self.pending.is_empty() && self.stderr.is_empty()) {
            return Vec::new();
        }
        self.post_progress(true)
    }

    /// Ends the stream. `exit_code` is Buck2's exit code for the command, if it has one.
    pub fn finish(&mut self, exit_code: Option<ExitCode>) -> Vec<bep::BuildEvent> {
        if self.finished {
            return Vec::new();
        }
        self.post_test_targets();

        let (name, code) = bazel_exit_code(exit_code);
        let buck2_code = exit_code.map_or(0, |c| c.exit_code());
        self.log(&format!(
            "Buck2 exited with code {buck2_code} ({}); reported as {name}.",
            exit_code.map_or("SUCCESS", |c| c.name())
        ));
        if self.shed > 0 {
            self.log(&format!(
                "{} events were not published because the Build Event Service backend fell behind.",
                self.shed
            ));
        }

        let mut events = self.post_progress(false);
        self.finished = true;

        let finish_time = SystemTime::now();
        events.push(bep::BuildEvent {
            id: Some(id(Id::BuildFinished(build_event_id::BuildFinishedId {}))),
            children: vec![id(Id::BuildMetrics(build_event_id::BuildMetricsId {}))],
            last_message: false,
            payload: Some(Payload::Finished(
                #[allow(deprecated)]
                bep::BuildFinished {
                    overall_success: code == 0,
                    exit_code: Some(bep::build_finished::ExitCode {
                        name: name.to_owned(),
                        code,
                    }),
                    finish_time: Some(finish_time.into()),
                    ..Default::default()
                },
            )),
        });

        let wall_time = self
            .info
            .start_time
            .and_then(|start| finish_time.duration_since(start).ok())
            .unwrap_or_default();
        let metrics = std::mem::take(&mut self.metrics);
        events.push(bep::BuildEvent {
            id: Some(id(Id::BuildMetrics(build_event_id::BuildMetricsId {}))),
            children: Vec::new(),
            last_message: true,
            payload: Some(Payload::BuildMetrics(bep::BuildMetrics {
                action_summary: Some(bep::build_metrics::ActionSummary {
                    actions_executed: metrics.actions_executed,
                    action_data: metrics
                        .by_mnemonic
                        .into_iter()
                        .map(
                            |(mnemonic, data)| bep::build_metrics::action_summary::ActionData {
                                mnemonic,
                                actions_executed: data.executed,
                                first_started_ms: data.first_started_ms,
                                last_ended_ms: data.last_ended_ms,
                                ..Default::default()
                            },
                        )
                        .collect(),
                    runner_count: metrics
                        .runners
                        .into_iter()
                        .map(|(name, (exec_kind, count))| {
                            bep::build_metrics::action_summary::RunnerCount {
                                name: name.to_owned(),
                                count,
                                exec_kind: exec_kind.to_owned(),
                            }
                        })
                        .collect(),
                    ..Default::default()
                }),
                timing_metrics: Some(bep::build_metrics::TimingMetrics {
                    wall_time_in_ms: wall_time.as_millis() as i64,
                    ..Default::default()
                }),
                ..Default::default()
            })),
        });
        events
    }

    fn log(&mut self, message: &str) {
        if message.is_empty() {
            return;
        }
        self.stderr.push_str(message);
        if !message.ends_with('\n') {
            self.stderr.push('\n');
        }
    }

    fn push(&mut self, event: bep::BuildEvent) {
        self.pending.push((event, true));
    }

    fn post_progress(&mut self, announce_next: bool) -> Vec<bep::BuildEvent> {
        let mut children = Vec::new();
        if announce_next {
            children.push(progress_id(self.next_progress + 1));
        }
        children.extend(
            self.pending
                .iter()
                .filter(|(_, announce)| *announce)
                .filter_map(|(e, _)| e.id.clone()),
        );
        let progress = bep::BuildEvent {
            id: Some(progress_id(self.next_progress)),
            children,
            last_message: false,
            payload: Some(Payload::Progress(bep::Progress {
                stdout: String::new(),
                stderr: std::mem::take(&mut self.stderr),
            })),
        };
        self.next_progress += 1;
        let mut events = vec![progress];
        events.extend(self.pending.drain(..).map(|(e, _)| e));
        events
    }

    fn ensure_configuration(&mut self, target: &ConfiguredTarget) {
        let Some(configuration) = &target.configuration else {
            return;
        };
        if !self.configurations_posted.insert(configuration.clone()) {
            return;
        }
        let mnemonic = configuration
            .split_once('#')
            .map_or(configuration.as_str(), |(name, _)| name)
            .to_owned();
        let make_variable = self
            .constraints
            .get(configuration)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();
        self.push(event(
            Id::Configuration(build_event_id::ConfigurationId {
                id: configuration.clone(),
            }),
            Payload::Configuration(bep::Configuration {
                platform_name: mnemonic.clone(),
                mnemonic,
                make_variable,
                ..Default::default()
            }),
        ));
    }

    fn handle_patterns(&mut self, patterns: &buck2_data::ParsedTargetPatterns) {
        if self.patterns_posted {
            return;
        }
        self.patterns_posted = true;
        self.push(event(
            Id::Pattern(build_event_id::PatternExpandedId {
                pattern: patterns
                    .target_patterns
                    .iter()
                    .map(|p| p.value.clone())
                    .collect(),
            }),
            Payload::Expanded(bep::PatternExpanded::default()),
        ));
    }

    fn handle_action(
        &mut self,
        end: SystemTime,
        duration: Duration,
        action: &buck2_data::ActionExecutionEnd,
    ) {
        let start = end.checked_sub(duration).unwrap_or(end);
        let target = action.key.as_ref().and_then(action_owner);
        let category = action
            .name
            .as_ref()
            .map_or("unknown", |n| n.category.as_str())
            .to_owned();
        if let (Some(target), Some(rule)) = (&target, &action.target_rule_type_name) {
            self.rule_types
                .entry(target.label.clone())
                .or_insert_with(|| rule.clone());
        }

        let kind = ActionExecutionKind::try_from(action.execution_kind)
            .unwrap_or(ActionExecutionKind::NotSet);
        if let Some((runner, exec_kind)) = runner(kind) {
            self.metrics.actions_executed += 1;
            self.metrics
                .runners
                .entry(runner)
                .or_insert((exec_kind, 0))
                .1 += 1;
            let data = self
                .metrics
                .by_mnemonic
                .entry(category.clone())
                .or_default();
            let start_ms = epoch_millis(start);
            let end_ms = epoch_millis(end);
            if data.executed == 0 || start_ms < data.first_started_ms {
                data.first_started_ms = start_ms;
            }
            data.last_ended_ms = data.last_ended_ms.max(end_ms);
            data.executed += 1;
        }

        let failure = if action.failed {
            // The `ActionError` for an action precedes its end, and explains a failed command.
            let reason = display_action_identity(
                action.key.as_ref(),
                action.name.as_ref(),
                TargetDisplayOptions::for_log(),
            )
            .ok()
            .and_then(|identity| self.action_error_reasons.remove(&identity))
            .unwrap_or_else(|| match &action.error {
                Some(buck2_data::action_execution_end::Error::Unknown(e)) => e.clone(),
                Some(buck2_data::action_execution_end::Error::MissingOutputs(m)) => {
                    format!("Required outputs are missing: {}", m.message)
                }
                _ => format!("Action `{category}` failed"),
            });
            if let Some(target) = &target {
                self.failures
                    .entry(target.clone())
                    .or_insert_with(|| reason.clone());
            }
            Some(reason)
        } else if !(self.options.publish_all_actions && !action.commands.is_empty()) {
            return;
        } else if self.shedding {
            self.shed += 1;
            return;
        } else {
            None
        };

        let mut primary_output = action.name.as_ref().map_or(category.clone(), |n| {
            if n.identifier.is_empty() {
                n.category.clone()
            } else {
                format!("{} {}", n.category, n.identifier)
            }
        });
        let label = match (&target, &action.key) {
            (Some(target), _) => target.label.clone(),
            (None, Some(key)) => {
                display_action_identity(Some(key), None, TargetDisplayOptions::for_log())
                    .unwrap_or_default()
            }
            (None, None) => String::new(),
        };
        if let Some(target) = &target {
            self.ensure_configuration(target);
        }
        // An event ID must be unique in the stream, but an action can run more than once in a
        // command, e.g. when its outputs had to be rebuilt.
        let runs = self
            .action_runs
            .entry((label.clone(), primary_output.clone()))
            .or_insert(0);
        *runs += 1;
        if *runs > 1 {
            primary_output = format!("{primary_output} (run {runs})");
        }

        let details = action.commands.last().and_then(|c| c.details.as_ref());
        let command_line = details
            .and_then(|d| d.command_kind.as_ref())
            .and_then(|k| k.command.as_ref())
            .map(|c| {
                use buck2_data::command_execution_kind::Command;
                match c {
                    Command::LocalCommand(c) => c.argv.clone(),
                    Command::WorkerCommand(c) => c.argv.clone(),
                    Command::WorkerInitCommand(c) => c.argv.clone(),
                    Command::RemoteCommand(_) | Command::OmittedLocalCommand(_) => Vec::new(),
                }
            })
            .unwrap_or_default();
        self.push(event(
            Id::ActionCompleted(build_event_id::ActionCompletedId {
                primary_output,
                label,
                configuration: target.as_ref().and_then(|t| t.configuration_id()),
            }),
            Payload::Action(
                #[allow(deprecated)]
                bep::ActionExecuted {
                    success: !action.failed,
                    r#type: category,
                    exit_code: details.and_then(|d| d.signed_exit_code).unwrap_or(0),
                    stdout: details.and_then(|d| inline_file("stdout", &d.cmd_stdout)),
                    stderr: details.and_then(|d| inline_file("stderr", &d.cmd_stderr)),
                    command_line,
                    failure_detail: failure.map(|message| FailureDetail {
                        message,
                        category: None,
                    }),
                    start_time: Some(start.into()),
                    end_time: Some(end.into()),
                    ..Default::default()
                },
            ),
        ));
    }

    fn handle_action_error(&mut self, error: &buck2_data::ActionError) {
        let identity = display_action_identity(
            error.key.as_ref(),
            error.name.as_ref(),
            TargetDisplayOptions::for_log(),
        )
        .unwrap_or_else(|_| "unknown action".to_owned());
        let reason = get_action_error_reason(error).unwrap_or_else(|_| "unknown reason".to_owned());
        let mut message = format!("Action failed: {identity}\n{reason}\n");
        self.action_error_reasons.insert(identity, reason);
        if let Some(details) = error.last_command.as_ref().and_then(|c| c.details.as_ref()) {
            for (name, output) in [
                ("Stdout", &details.cmd_stdout),
                ("Stderr", &details.cmd_stderr),
            ] {
                if !output.is_empty() {
                    message.push_str(&format!("{name}:\n{}\n", tail(output)));
                }
            }
        }
        // Errors are always published: they are why someone opens a failed invocation.
        self.log(&message);
    }

    fn handle_test_run(
        &mut self,
        end: SystemTime,
        duration: Duration,
        run: &buck2_data::TestRunEnd,
    ) {
        let Some(target) = run
            .suite
            .as_ref()
            .and_then(|s| s.target_label.as_ref())
            .and_then(ConfiguredTarget::from_proto)
        else {
            return;
        };
        let test = self.tests.entry(target).or_default();
        let start = end.checked_sub(duration).unwrap_or(end);
        test.start = Some(test.start.map_or(start, |s| s.min(start)));
        test.end = Some(test.end.map_or(end, |e| e.max(end)));
        if let Some(details) = run.command_report.as_ref().and_then(|r| r.details.as_ref()) {
            test.exit_code = details.signed_exit_code;
            use buck2_data::command_execution_kind::Command;
            test.strategy = match details
                .command_kind
                .as_ref()
                .and_then(|k| k.command.as_ref())
            {
                Some(Command::RemoteCommand(_)) => "remote",
                Some(_) => "local",
                None => "",
            }
            .to_owned();
        }
    }

    fn handle_test_result(&mut self, timestamp: SystemTime, result: &buck2_data::TestResult) {
        let Some(target) = result
            .target_label
            .as_ref()
            .and_then(ConfiguredTarget::from_proto)
        else {
            return;
        };
        let duration = result.duration.and_then(|d| Duration::try_from(d).ok());
        let test = self.tests.entry(target).or_default();
        // Without a `TestRun` span, as with external test runners, time the target by its
        // results.
        if test.start.is_none()
            && let Some(duration) = duration
        {
            test.start = timestamp.checked_sub(duration);
            test.end = Some(timestamp);
        }
        test.cases.push(TestCase {
            name: result.name.clone(),
            status: TestStatus::try_from(result.status).unwrap_or(TestStatus::Unknown),
            details: result.details.clone(),
        });
    }

    /// Posts `TargetConfigured`, `TargetComplete`, `TestResult` and `TestSummary` for every test
    /// target that has results and has not been posted.
    fn post_test_targets(&mut self) {
        for (target, test) in std::mem::take(&mut self.tests) {
            if test.cases.is_empty() {
                continue;
            }
            let test_result_id = id(Id::TestResult(build_event_id::TestResultId {
                label: target.label.clone(),
                configuration: target.configuration_id(),
                run: 1,
                shard: 1,
                attempt: 1,
            }));
            let test_summary_id = id(Id::TestSummary(build_event_id::TestSummaryId {
                label: target.label.clone(),
                configuration: target.configuration_id(),
            }));
            if !self.post_target(
                &target,
                true,
                vec![test_result_id.clone(), test_summary_id.clone()],
            ) {
                continue;
            }

            let status = test_status(&test);
            let failing: Vec<&str> = test
                .cases
                .iter()
                .filter(|c| !is_passing(c.status))
                .map(|c| c.name.as_str())
                .collect();
            let log: String = test
                .cases
                .iter()
                .map(|c| format!("==== {} ({:?}) ====\n{}\n", c.name, c.status, c.details))
                .collect();
            let duration = match (test.start, test.end) {
                (Some(start), Some(end)) => end.duration_since(start).unwrap_or_default(),
                _ => Duration::ZERO,
            };
            let duration: Option<prost_types::Duration> = duration.try_into().ok();
            self.pending.push((
                bep::BuildEvent {
                    id: Some(test_result_id),
                    children: Vec::new(),
                    last_message: false,
                    payload: Some(Payload::TestResult(
                        #[allow(deprecated)]
                        bep::TestResult {
                            status: status as i32,
                            status_details: if failing.is_empty() {
                                String::new()
                            } else {
                                format!("Failed: {}", failing.join(", "))
                            },
                            test_attempt_start: test.start.map(Into::into),
                            test_attempt_duration: duration,
                            test_action_output: inline_file("test.log", &log).into_iter().collect(),
                            execution_info: Some(bep::test_result::ExecutionInfo {
                                strategy: test.strategy.clone(),
                                exit_code: test.exit_code.unwrap_or_default(),
                                hostname: self.info.host.clone(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    )),
                },
                false,
            ));
            self.pending.push((
                bep::BuildEvent {
                    id: Some(test_summary_id),
                    children: Vec::new(),
                    last_message: false,
                    payload: Some(Payload::TestSummary(bep::TestSummary {
                        overall_status: status as i32,
                        total_run_count: 1,
                        run_count: 1,
                        attempt_count: 1,
                        shard_count: 1,
                        first_start_time: test.start.map(Into::into),
                        last_stop_time: test.end.map(Into::into),
                        total_run_duration: duration,
                        ..Default::default()
                    })),
                },
                false,
            ));
        }
    }

    /// Posts `TargetConfigured` and `TargetComplete` once per target. Returns whether it posted.
    fn post_target(
        &mut self,
        target: &ConfiguredTarget,
        built: bool,
        children: Vec<bep::BuildEventId>,
    ) -> bool {
        if !self.targets_posted.insert(target.clone()) {
            return false;
        }
        self.ensure_configuration(target);
        let is_test = !children.is_empty();
        let rule = self
            .rule_types
            .get(&target.label)
            .map_or(if is_test { "test" } else { "unknown" }, |r| r.as_str());
        let failure = self.failures.get(target).cloned();
        let success = built && failure.is_none();
        let completed_id = id(Id::TargetCompleted(build_event_id::TargetCompletedId {
            label: target.label.clone(),
            configuration: target.configuration_id(),
            aspect: String::new(),
        }));
        self.push(event(
            Id::TargetConfigured(build_event_id::TargetConfiguredId {
                label: target.label.clone(),
                aspect: String::new(),
            }),
            Payload::Configured(bep::TargetConfigured {
                target_kind: format!("{rule} rule"),
                ..Default::default()
            }),
        ));
        self.push(bep::BuildEvent {
            id: Some(completed_id),
            children,
            last_message: false,
            payload: Some(Payload::Completed(
                #[allow(deprecated)]
                bep::TargetComplete {
                    success,
                    failure_detail: (!success).then(|| FailureDetail {
                        message: failure.unwrap_or_else(|| {
                            "Failed to build; the failure is in a dependency".to_owned()
                        }),
                        category: None,
                    }),
                    ..Default::default()
                },
            )),
        });
        true
    }
}

fn id(id: Id) -> bep::BuildEventId {
    bep::BuildEventId { id: Some(id) }
}

fn progress_id(opaque_count: i32) -> bep::BuildEventId {
    id(Id::Progress(build_event_id::ProgressId { opaque_count }))
}

fn original_command_line_id() -> bep::BuildEventId {
    id(Id::StructuredCommandLine(
        build_event_id::StructuredCommandLineId {
            command_line_label: "original".to_owned(),
        },
    ))
}

fn event(event_id: Id, payload: Payload) -> bep::BuildEvent {
    bep::BuildEvent {
        id: Some(id(event_id)),
        children: Vec::new(),
        last_message: false,
        payload: Some(payload),
    }
}

fn chunk_section(label: &str, chunk: Vec<String>) -> command_line::CommandLineSection {
    command_line::CommandLineSection {
        section_label: label.to_owned(),
        section_type: Some(command_line::command_line_section::SectionType::ChunkList(
            command_line::ChunkList { chunk },
        )),
    }
}

fn action_owner(key: &buck2_data::ActionKey) -> Option<ConfiguredTarget> {
    match key.owner.as_ref()? {
        action_key::Owner::TargetLabel(t)
        | action_key::Owner::TestTargetLabel(t)
        | action_key::Owner::LocalResourceSetup(t) => ConfiguredTarget::from_proto(t),
        action_key::Owner::AnonTarget(t) => Some(ConfiguredTarget {
            label: display_anon_target(t).ok()?,
            configuration: None,
        }),
        action_key::Owner::BxlKey(k) => Some(ConfiguredTarget {
            label: display_bxl_key(k).ok()?,
            configuration: None,
        }),
    }
}

/// The runner name Bazel would report for an action, and whether it ran locally or remotely.
/// `None` for actions that did no work.
fn runner(kind: ActionExecutionKind) -> Option<(&'static str, &'static str)> {
    Some(match kind {
        ActionExecutionKind::Local => ("local", "Local"),
        ActionExecutionKind::LocalWorker => ("worker", "Local"),
        ActionExecutionKind::Remote
        | ActionExecutionKind::RemoteWorker
        | ActionExecutionKind::RemoteDeduplicated => ("remote", "Remote"),
        ActionExecutionKind::ActionCache
        | ActionExecutionKind::RemoteDepFileCache
        | ActionExecutionKind::RemoteServedFromCache => ("remote cache hit", "Remote"),
        ActionExecutionKind::LocalDepFile | ActionExecutionKind::LocalActionCache => {
            ("local cache hit", "Local")
        }
        ActionExecutionKind::Simple => ("internal", ""),
        ActionExecutionKind::Deferred | ActionExecutionKind::NotSet => return None,
    })
}

fn is_passing(status: TestStatus) -> bool {
    matches!(
        status,
        TestStatus::Pass
            | TestStatus::Skip
            | TestStatus::Omitted
            | TestStatus::ListingSuccess
            | TestStatus::Rerun
    )
}

fn test_status(test: &TestTarget) -> bep::TestStatus {
    let statuses = || test.cases.iter().map(|c| c.status);
    if statuses().any(|s| s == TestStatus::Timeout) {
        bep::TestStatus::Timeout
    } else if statuses().any(|s| s == TestStatus::InfraFailure) {
        bep::TestStatus::RemoteFailure
    } else if statuses().any(|s| !is_passing(s)) {
        bep::TestStatus::Failed
    } else {
        bep::TestStatus::Passed
    }
}

/// Maps Buck2's exit code to the Bazel exit code with the same meaning, since BEP consumers
/// interpret the numbers the way Bazel defines them (Bazel's 3 means tests failed, Buck2's 3 a
/// user error).
fn bazel_exit_code(exit_code: Option<ExitCode>) -> (&'static str, i32) {
    match exit_code {
        None | Some(ExitCode::Success) | Some(ExitCode::TestRunner(0)) => ("SUCCESS", 0),
        Some(ExitCode::UserError) => ("BUILD_FAILURE", 1),
        Some(ExitCode::TestRunner(_)) => ("TESTS_FAILED", 3),
        Some(ExitCode::SignalInterrupt)
        | Some(ExitCode::ClientIoBrokenPipe)
        | Some(ExitCode::Timeout) => ("INTERRUPTED", 8),
        Some(ExitCode::FatalOom) => ("OOM_ERROR", 33),
        Some(ExitCode::DaemonIsBusy)
        | Some(ExitCode::DaemonPreempted)
        | Some(ExitCode::ConnectError) => ("LOCAL_ENVIRONMENTAL_ERROR", 36),
        Some(ExitCode::InfraError) | Some(ExitCode::UnknownFailure) => ("BLAZE_INTERNAL_ERROR", 37),
    }
}

/// Keeps the end of long output, where errors usually are.
fn tail(s: &str) -> &str {
    if s.len() <= MAX_INLINE_OUTPUT_BYTES {
        return s;
    }
    let mut start = s.len() - MAX_INLINE_OUTPUT_BYTES;
    while !s.is_char_boundary(start) {
        start += 1;
    }
    &s[start..]
}

fn inline_file(name: &str, contents: &str) -> Option<bep::File> {
    if contents.is_empty() {
        return None;
    }
    let contents = tail(contents);
    Some(bep::File {
        name: name.to_owned(),
        length: contents.len() as i64,
        file: Some(bep::file::File::Contents(contents.as_bytes().to_vec())),
        ..Default::default()
    })
}

fn epoch_millis(time: SystemTime) -> i64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use buck2_cli_proto::BuildResponse;
    use buck2_cli_proto::CommandResult;
    use buck2_cli_proto::command_result;
    use buck2_data::buck_event;
    use buck2_data::command_execution;
    use buck2_data::command_execution_kind::Command;
    use buck2_wrapper_common::invocation_id::TraceId;
    use prost::Message;

    use super::*;

    const CFG: &str = "prelude//platforms:default#abc";

    fn converter(publish_all_actions: bool) -> BepConverter {
        BepConverter::new(
            InvocationInfo {
                invocation_id: "inv".to_owned(),
                command: "build".to_owned(),
                start_time: Some(SystemTime::now()),
                build_tool_version: "buck2 test".to_owned(),
                args: vec!["buck2".to_owned(), "build".to_owned(), "//:all".to_owned()],
                user: "me".to_owned(),
                host: "box".to_owned(),
                metadata: vec![("ROLE".to_owned(), "CI".to_owned())],
                ..Default::default()
            },
            ConverterOptions {
                publish_all_actions,
            },
        )
    }

    fn buck_event(data: impl Into<buck_event::Data>) -> BuckEvent {
        BuckEvent::new(SystemTime::now(), TraceId::new(), None, None, data.into())
    }

    fn instant(data: impl Into<buck2_data::instant_event::Data>) -> BuckEvent {
        buck_event(buck2_data::InstantEvent {
            data: Some(data.into()),
        })
    }

    fn span_end(
        duration: Duration,
        data: impl Into<buck2_data::span_end_event::Data>,
    ) -> BuckEvent {
        buck_event(buck2_data::SpanEndEvent {
            duration: Some(duration.try_into().unwrap()),
            data: Some(data.into()),
            ..Default::default()
        })
    }

    fn target(name: &str) -> ConfiguredTargetLabel {
        ConfiguredTargetLabel {
            label: Some(buck2_data::TargetLabel {
                package: "root//".to_owned(),
                name: name.to_owned(),
            }),
            configuration: Some(buck2_data::Configuration {
                full_name: CFG.to_owned(),
            }),
            execution_configuration: None,
        }
    }

    fn action(name: &str, failed: bool, kind: ActionExecutionKind) -> BuckEvent {
        let status = if failed {
            command_execution::Status::Failure(command_execution::Failure::default())
        } else {
            command_execution::Status::Success(command_execution::Success {})
        };
        span_end(
            Duration::from_millis(1500),
            Box::new(buck2_data::ActionExecutionEnd {
                key: Some(buck2_data::ActionKey {
                    owner: Some(action_key::Owner::TargetLabel(target(name))),
                    ..Default::default()
                }),
                name: Some(buck2_data::ActionName {
                    category: "genrule".to_owned(),
                    identifier: String::new(),
                }),
                failed,
                error: failed.then_some(
                    buck2_data::action_execution_end::Error::CommandExecutionError(
                        buck2_data::CommandExecutionError {},
                    ),
                ),
                execution_kind: kind as i32,
                target_rule_type_name: Some("genrule".to_owned()),
                // Simple actions, like writing a file, do not run a command.
                commands: if kind == ActionExecutionKind::Simple {
                    Vec::new()
                } else {
                    vec![buck2_data::CommandExecution {
                        details: Some(buck2_data::CommandExecutionDetails {
                            signed_exit_code: Some(if failed { 3 } else { 0 }),
                            cmd_stderr: if failed {
                                "boom".to_owned()
                            } else {
                                String::new()
                            },
                            command_kind: Some(buck2_data::CommandExecutionKind {
                                command: Some(Command::LocalCommand(buck2_data::LocalCommand {
                                    argv: vec![
                                        "sh".to_owned(),
                                        "-c".to_owned(),
                                        "exit 3".to_owned(),
                                    ],
                                    ..Default::default()
                                })),
                            }),
                            ..Default::default()
                        }),
                        status: Some(status),
                        ..Default::default()
                    }]
                },
                ..Default::default()
            }),
        )
    }

    fn key(id: &bep::BuildEventId) -> Vec<u8> {
        id.encode_to_vec()
    }

    /// Checks the rules BEP consumers rely on: the stream starts with `BuildStarted`, every
    /// other event was announced as a child of an earlier one, every announced event is posted
    /// exactly once, and only the final event is marked last.
    fn assert_valid_stream(events: &[bep::BuildEvent]) {
        assert!(matches!(events[0].payload, Some(Payload::Started(_))));
        let mut announced = HashSet::new();
        let mut posted = HashSet::new();
        for (i, event) in events.iter().enumerate() {
            let id = key(event.id.as_ref().unwrap());
            assert!(
                i == 0 || announced.contains(&id),
                "{:?} was not announced",
                event.id
            );
            assert!(posted.insert(id), "{:?} was posted twice", event.id);
            for child in &event.children {
                assert!(
                    announced.insert(key(child)),
                    "{child:?} was announced twice"
                );
            }
            assert_eq!(i == events.len() - 1, event.last_message);
        }
        let unposted: Vec<_> = announced.difference(&posted).collect();
        assert!(
            unposted.is_empty(),
            "announced but not posted: {unposted:?}"
        );
    }

    fn payloads(events: &[bep::BuildEvent]) -> impl Iterator<Item = &Payload> {
        events.iter().filter_map(|e| e.payload.as_ref())
    }

    fn target_complete<'a>(events: &'a [bep::BuildEvent], label: &str) -> &'a bep::TargetComplete {
        events
            .iter()
            .find_map(|e| match (&e.id.as_ref()?.id, &e.payload) {
                (Some(Id::TargetCompleted(id)), Some(Payload::Completed(c)))
                    if id.label == label =>
                {
                    Some(c)
                }
                _ => None,
            })
            .unwrap_or_else(|| panic!("no TargetComplete for {label}"))
    }

    fn run_build(converter: &mut BepConverter) -> Vec<bep::BuildEvent> {
        let mut events = converter.start();
        converter.handle_event(&instant(buck2_data::ParsedTargetPatterns {
            target_patterns: vec![buck2_data::TargetPattern {
                value: "root//:all".to_owned(),
            }],
        }));
        converter.handle_event(&action("hello", false, ActionExecutionKind::Local));
        converter.handle_event(&action("hello", false, ActionExecutionKind::Simple));
        // The same action running again needs a distinct event ID.
        converter.handle_event(&action("hello", false, ActionExecutionKind::Local));
        events.extend(converter.flush());
        converter.handle_event(&action("broken", true, ActionExecutionKind::Local));
        converter.handle_event(&instant(buck2_data::ConsoleMessage {
            message: "Action failed: root//:broken".to_owned(),
        }));
        events.extend(converter.flush());
        // Nothing new: no events.
        assert!(converter.flush().is_empty());
        converter.handle_command_result(&CommandResult {
            result: Some(command_result::Result::BuildResponse(BuildResponse {
                build_targets: ["hello", "broken", "needs_broken"]
                    .into_iter()
                    .map(|name| buck2_cli_proto::BuildTarget {
                        target: format!("root//:{name}"),
                        configuration: CFG.to_owned(),
                        failed: name != "hello",
                        target_rule_type_name: Some("genrule".to_owned()),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })),
        });
        events.extend(converter.finish(Some(ExitCode::UserError)));
        assert!(converter.finish(Some(ExitCode::UserError)).is_empty());
        events
    }

    #[test]
    fn test_failed_build() {
        let events = run_build(&mut converter(true));
        assert_valid_stream(&events);

        let actions: Vec<&bep::ActionExecuted> = payloads(&events)
            .filter_map(|p| match p {
                Payload::Action(a) => Some(a),
                _ => None,
            })
            .collect();
        // The simple action did not run a command, so it is not published.
        assert_eq!(3, actions.len());
        assert!(actions[0].success && actions[1].success);
        let failed = actions[2];
        assert!(!failed.success);
        assert_eq!(3, failed.exit_code);
        assert_eq!("genrule", failed.r#type);
        assert_eq!(vec!["sh", "-c", "exit 3"], failed.command_line);
        assert_eq!(
            Some(bep::file::File::Contents(b"boom".to_vec())),
            failed.stderr.as_ref().unwrap().file
        );
        let start = SystemTime::try_from(failed.start_time.unwrap()).unwrap();
        let end = SystemTime::try_from(failed.end_time.unwrap()).unwrap();
        assert_eq!(
            Duration::from_millis(1500),
            end.duration_since(start).unwrap()
        );

        assert!(target_complete(&events, "root//:hello").success);
        let broken = target_complete(&events, "root//:broken");
        assert!(!broken.success);
        assert!(
            broken
                .failure_detail
                .as_ref()
                .unwrap()
                .message
                .contains("genrule")
        );
        let needs_broken = target_complete(&events, "root//:needs_broken");
        assert!(!needs_broken.success);
        assert!(
            needs_broken
                .failure_detail
                .as_ref()
                .unwrap()
                .message
                .contains("dependency")
        );

        let stderr: String = payloads(&events)
            .filter_map(|p| match p {
                Payload::Progress(p) => Some(p.stderr.as_str()),
                _ => None,
            })
            .collect();
        assert!(stderr.contains("Action failed: root//:broken"), "{stderr}");

        let finished = payloads(&events)
            .find_map(|p| match p {
                Payload::Finished(f) => Some(f),
                _ => None,
            })
            .unwrap();
        // Buck2 reports a build failure as 3, which in BEP would mean that tests failed.
        assert_eq!(
            Some(bep::build_finished::ExitCode {
                name: "BUILD_FAILURE".to_owned(),
                code: 1
            }),
            finished.exit_code
        );

        let Some(Payload::BuildMetrics(metrics)) = &events.last().unwrap().payload else {
            panic!("the stream does not end with BuildMetrics");
        };
        let summary = metrics.action_summary.as_ref().unwrap();
        assert_eq!(4, summary.actions_executed);
        let runners: Vec<(&str, i32)> = summary
            .runner_count
            .iter()
            .map(|r| (r.name.as_str(), r.count))
            .collect();
        assert_eq!(vec![("internal", 1), ("local", 3)], runners);
    }

    #[test]
    fn test_shedding_drops_only_successful_actions() {
        let mut converter = converter(true);
        converter.set_shedding(true);
        let events = run_build(&mut converter);
        assert_valid_stream(&events);
        let actions: Vec<bool> = payloads(&events)
            .filter_map(|p| match p {
                Payload::Action(a) => Some(a.success),
                _ => None,
            })
            .collect();
        assert_eq!(vec![false], actions);
        assert_eq!(2, converter.shed());
    }

    #[test]
    fn test_tests() {
        let mut converter = converter(false);
        let mut events = converter.start();
        for name in ["passing", "failing"] {
            converter.handle_event(&span_end(
                Duration::from_secs(2),
                buck2_data::TestRunEnd {
                    suite: Some(buck2_data::TestSuite {
                        suite_name: name.to_owned(),
                        test_names: Vec::new(),
                        target_label: Some(target(name)),
                    }),
                    ..Default::default()
                },
            ));
        }
        events.extend(converter.flush());
        // Results arrive after the runs have ended.
        for (name, status) in [
            ("passing", TestStatus::Pass),
            ("failing", TestStatus::Pass),
            ("failing", TestStatus::Fail),
        ] {
            converter.handle_event(&instant(buck2_data::TestResult {
                name: format!("{name} case {status:?}"),
                status: status as i32,
                details: "output".to_owned(),
                target_label: Some(target(name)),
                ..Default::default()
            }));
        }
        converter.handle_event(&instant(buck2_data::EndOfTestResults { exit_code: 32 }));
        events.extend(converter.flush());
        events.extend(converter.finish(Some(ExitCode::TestRunner(32))));
        assert_valid_stream(&events);

        let summaries: BTreeMap<String, &bep::TestSummary> = events
            .iter()
            .filter_map(|e| match (&e.id.as_ref()?.id, &e.payload) {
                (Some(Id::TestSummary(id)), Some(Payload::TestSummary(s))) => {
                    Some((id.label.clone(), s))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            bep::TestStatus::Passed as i32,
            summaries["root//:passing"].overall_status
        );
        let failing = summaries["root//:failing"];
        assert_eq!(bep::TestStatus::Failed as i32, failing.overall_status);
        assert_eq!(
            Some(Duration::from_secs(2)),
            failing
                .total_run_duration
                .and_then(|d| Duration::try_from(d).ok())
        );
        let result = events
            .iter()
            .find_map(|e| match (&e.id.as_ref()?.id, &e.payload) {
                (Some(Id::TestResult(id)), Some(Payload::TestResult(r)))
                    if id.label == "root//:failing" =>
                {
                    Some(r)
                }
                _ => None,
            })
            .unwrap();
        assert_eq!("Failed: failing case Fail", result.status_details);
        // A test target that built is complete even when its tests fail.
        assert!(target_complete(&events, "root//:failing").success);

        let Some(Payload::Finished(finished)) = &events[events.len() - 2].payload else {
            panic!("expected BuildFinished");
        };
        assert_eq!("TESTS_FAILED", finished.exit_code.as_ref().unwrap().name);
    }
}

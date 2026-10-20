/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Publishes the command to a Build Event Service when `buck2_bes.backend` is configured.
//!
//! This runs in the client rather than the daemon: the client sees every event of the command,
//! reads configuration with the command's own `-c` overrides, substitutes credentials from the
//! environment of the command rather than of the daemon, and can flush the upload before the
//! process exits, as ephemeral CI machines require.

use std::sync::Arc;

use async_trait::async_trait;
use buck2_bes::bep::InvocationInfo;
use buck2_bes::config::BesConfig;
use buck2_bes::publisher::BesPublisher;
use buck2_cli_proto::ConfigOverride;
use buck2_common::legacy_configs::cells::BuckConfigBasedCells;
use buck2_error::ExitCode;
use buck2_events::BuckEvent;

use crate::client_ctx::ClientCommandContext;
use crate::exit_result::ExitResult;
use crate::subscribers::subscriber::EventSubscriber;
use crate::version::BuckVersion;

pub(crate) struct BesSubscriber {
    publisher: Option<BesPublisher>,
    exit_code: Option<ExitCode>,
}

impl BesSubscriber {
    /// Returns `None`, after printing a warning if the configuration is invalid, unless a
    /// backend is configured.
    pub(crate) fn new(
        ctx: &ClientCommandContext,
        command: &str,
        config_overrides: &[ConfigOverride],
        args: Vec<String>,
    ) -> Option<Self> {
        let paths = ctx.paths().ok()?;
        let cells = match futures::executor::block_on(BuckConfigBasedCells::parse_with_config_args(
            paths.project_root(),
            config_overrides,
        )) {
            Ok(cells) => cells,
            Err(e) => {
                // An invalid buckconfig fails the command in the daemon, with a better message.
                tracing::debug!("Not publishing build events: {e:#}");
                return None;
            }
        };
        let config = match BesConfig::from_legacy_config(&cells.root_config) {
            Ok(config) => config?,
            Err(e) => {
                // Publishing is best effort, so a bad `buck2_bes` section does not fail the
                // command either.
                crate::eprintln!("Warning: not publishing build events: {:#}", e).ok()?;
                return None;
            }
        };

        let publisher = BesPublisher::new(
            config,
            InvocationInfo {
                invocation_id: ctx.trace_id.to_string(),
                command: command.to_owned(),
                start_time: Some(ctx.start_time),
                build_tool_version: format!(
                    "buck2 {}",
                    BuckVersion::get_version().unwrap_or("unknown")
                ),
                args,
                working_directory: ctx.working_dir.to_string(),
                workspace_directory: paths.project_root().root().to_string(),
                user: buck2_events::metadata::username()
                    .ok()
                    .flatten()
                    .or_else(|| std::env::var("USER").ok())
                    .or_else(|| std::env::var("USERNAME").ok())
                    .unwrap_or_default(),
                host: buck2_events::metadata::hostname().unwrap_or_default(),
                metadata: ctx
                    .client_metadata
                    .iter()
                    .map(|m| (m.key.clone(), m.value.clone()))
                    .collect(),
            },
        );
        if let Some(url) = publisher.invocation_url() {
            crate::eprintln!("Streaming build results to: {}", url).ok()?;
        }
        Some(Self {
            publisher: Some(publisher),
            exit_code: None,
        })
    }
}

#[async_trait]
impl EventSubscriber for BesSubscriber {
    fn name(&self) -> &'static str {
        "build event service"
    }

    async fn handle_events(&mut self, events: &[Arc<BuckEvent>]) -> buck2_error::Result<()> {
        if let Some(publisher) = &mut self.publisher {
            publisher.handle_events(events).await;
        }
        Ok(())
    }

    async fn handle_command_result(
        &mut self,
        result: &buck2_cli_proto::CommandResult,
    ) -> buck2_error::Result<()> {
        if let Some(publisher) = &mut self.publisher {
            publisher.handle_command_result(result).await;
        }
        Ok(())
    }

    fn handle_exit_result(&mut self, result: &ExitResult) {
        self.exit_code = result.exit_code();
    }

    async fn finalize(mut self: Box<Self>) -> buck2_error::Result<()> {
        let Some(publisher) = self.publisher.take() else {
            return Ok(());
        };
        let url = publisher.invocation_url();
        // Publishing is best effort: a failed upload is a warning, never a failed command.
        match publisher.finish(self.exit_code).await {
            Some(warning) => crate::eprintln!("Warning: {}", warning)?,
            None => {
                if let Some(url) = url {
                    crate::eprintln!("Build results: {}", url)?;
                }
            }
        }
        Ok(())
    }
}

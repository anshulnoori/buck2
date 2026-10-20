/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Publishing of invocations to a Build Event Service (BES) backend, such as the ones behind
//! BuildBuddy, EngFlow or Namespace.
//!
//! * [`config`] reads the `[buck2_bes]` buckconfig section.
//! * [`bep`] converts Buck2's events to Bazel's Build Event Protocol, which those backends display.
//! * [`uploader`] delivers events with the `PublishBuildEvent` gRPC protocol: ordered,
//!   acknowledged, and retried, in the background and on a best-effort basis.
//! * [`publisher`] ties them together for one invocation.

pub mod bep;
pub mod config;
pub mod publisher;
pub mod uploader;

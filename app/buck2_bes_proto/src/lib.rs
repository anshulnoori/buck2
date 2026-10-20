/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Protocols for publishing build events to a Build Event Service (BES).
//!
//! * `google::devtools::build::v1` is the transport: the `PublishBuildEvent`
//!   gRPC service from googleapis that BES backends implement.
//! * `build_event_stream` is Bazel's Build Event Protocol (BEP), the payload
//!   that existing BES user interfaces know how to display.
//!
//! The `.proto` files are vendored unmodified from their upstream repositories.

#![allow(clippy::doc_lazy_continuation)]
#![allow(clippy::doc_overindented_list_items)]
#![allow(clippy::enum_variant_names)]

pub mod google {
    pub mod api {
        tonic::include_proto!("google.api");
    }
    pub mod devtools {
        pub mod build {
            pub mod v1 {
                tonic::include_proto!("google.devtools.build.v1");
            }
        }
    }
}

pub mod build_event_stream {
    tonic::include_proto!("build_event_stream");
}

pub mod command_line {
    tonic::include_proto!("command_line");
}

pub mod failure_details {
    tonic::include_proto!("failure_details");
}

pub mod options {
    tonic::include_proto!("options");
}

pub mod blaze {
    tonic::include_proto!("blaze");

    pub mod invocation_policy {
        tonic::include_proto!("blaze.invocation_policy");
    }
    pub mod strategy_policy {
        tonic::include_proto!("blaze.strategy_policy");
    }
}

pub mod devtools {
    pub mod build {
        pub mod lib {
            pub mod packages {
                pub mod metrics {
                    tonic::include_proto!("devtools.build.lib.packages.metrics");
                }
            }
        }
    }
}

/// The `type_url` that BES backends expect on a `bazel_event`.
pub const BAZEL_EVENT_TYPE_URL: &str = "type.googleapis.com/build_event_stream.BuildEvent";

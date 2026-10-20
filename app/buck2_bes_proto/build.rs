/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::io;

fn main() -> io::Result<()> {
    // The vendored files keep their upstream paths so that their `import`
    // statements resolve unchanged, which keeps updating them a plain copy.
    let proto_files = &[
        "proto/google/devtools/build/v1/publish_build_event.proto",
        "proto/src/main/java/com/google/devtools/build/lib/buildeventstream/proto/build_event_stream.proto",
    ];

    let builder = buck2_protoc_dev::configure();
    unsafe { builder.setup_protoc() }
        .type_attribute(
            "build_event_stream.BuildEvent.payload",
            "#[allow(clippy::large_enum_variant)]",
        )
        .compile(proto_files, &["./proto/"])
}

---
id: build_event_service
title: Build Event Service
---

Buck2 can stream `buck2 build` and `buck2 test` invocations to a
[Build Event Service](https://bazel.build/remote/bep#build-event-service)
(BES) backend, such as BuildBuddy, EngFlow or Namespace. The backend
then shows the invocation in its web UI: the command line, the targets
and whether they built, failed actions with their output, test results
and timings.

Buck2 sends the events with the gRPC
`google.devtools.build.v1.PublishBuildEvent` API. Each event carries a
[Bazel Build Event Protocol](https://bazel.build/remote/bep) (BEP)
message, the format existing BES user interfaces understand. Buck2's own
event log is still available locally with `buck2 log`.

## Configuration

Publishing is enabled when `buck2_bes.backend` is set, for example in
`.buckconfig` or `.buckconfig.local`:

```ini
[buck2_bes]
backend = grpcs://bes.example.com
results_url = https://app.example.com/invocation/
http_headers = x-api-key: $BES_API_KEY
```

| Key                   | Default | Description                                                                                                                                                                                                      |
| --------------------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `backend`             |         | The backend, as `grpcs://host[:port]` (TLS), `grpc://host:port` (plaintext) or `host[:port]` (TLS). Publishing is disabled when unset.                                                                           |
| `results_url`         |         | Prefix of the URL at which the backend shows an invocation. Buck2 appends the invocation ID and prints the URL at the start and end of the command.                                                              |
| `http_headers`        |         | Comma-separated `name: value` headers sent with every request, typically for authentication. `$VAR` is replaced with the environment variable `VAR` of the `buck2` client; if `VAR` is not set, nothing is sent. |
| `tls_ca_certs`        |         | Path to a PEM file of CA certificates to trust in addition to the system roots.                                                                                                                                  |
| `tls_client_cert`     |         | Path to a PEM file with a client certificate and its private key, for mutual TLS.                                                                                                                                |
| `instance_name`       |         | Sent as the `project_id` of every request. Some backends require it.                                                                                                                                             |
| `keywords`            |         | Comma-separated notification keywords added to the start of the stream.                                                                                                                                          |
| `lifecycle_events`    | `true`  | Whether to send `PublishLifecycleEvent` calls around the event stream. Some backends need them to show the invocation as started and finished.                                                                   |
| `upload_timeout_secs` | `10`    | How long `buck2` waits, after the command has finished, for the backend to acknowledge the remaining events.                                                                                                     |
| `max_buffered_events` | `10000` | When more events than this wait for acknowledgement, Buck2 stops sending successful actions and console output, and keeps sending failures, targets and test results.                                            |
| `publish_all_actions` | `true`  | Whether to send every action that ran a command, rather than only failed actions.                                                                                                                                |

Keys can also be set for one command with `-c`, for example
`buck2 build -c buck2_bes.backend= //...` to disable publishing.

`--client-metadata key=value` values are sent as BEP build metadata,
which some backends use to group invocations, for example
`--client-metadata role=ci`.

## Delivery

Publishing never fails or slows down the command:

- Events are sent in the background while the command runs, in order and
  with sequence numbers. Buck2 keeps each event until the backend
  acknowledges it. When the stream fails, Buck2 opens a new stream with
  exponential backoff and sends the unacknowledged events again. It does
  not retry errors that will not go away, such as `UNAUTHENTICATED` or
  `INVALID_ARGUMENT`.
- When the command has finished, `buck2` waits up to
  `upload_timeout_secs` for the remaining acknowledgements.
- If the backend cannot be reached, rejects the events or does not
  acknowledge them in time, `buck2` prints a warning. The exit code of
  the command does not change.

## What is sent

| BEP event                                        | Content                                                                                     |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------- |
| `BuildStarted`                                   | Invocation ID, command, Buck2 version, working directory, start time.                       |
| `UnstructuredCommandLine`, `OriginalCommandLine` | The command line, with the same sanitization as the event log.                              |
| `WorkspaceStatus`, `BuildMetadata`               | User and host; `--client-metadata` values.                                                  |
| `Pattern`                                        | The target patterns of the command.                                                         |
| `Configuration`                                  | Each configuration, with its constraints.                                                   |
| `TargetConfigured`, `TargetComplete`             | Each requested target, its rule type, and whether it built.                                 |
| `ActionExecuted`                                 | Actions that ran a command: timings, exit code, argv, the last 64 KiB of stdout and stderr. |
| `TestResult`, `TestSummary`                      | Each test target's status, duration and the last 64 KiB of its output.                      |
| `Progress`                                       | Errors and warnings printed to the console.                                                 |
| `BuildFinished`, `BuildMetrics`                  | The outcome, as a Bazel exit code, and action counts by runner and category.                |

Labels keep their Buck2 spelling, for example `root//foo:bar`, and
configurations are named by their Buck2 name, for example
`prelude//platforms:default#<hash>`.

Buck2's exit codes are mapped to the Bazel exit codes UIs expect: a
build failure is `BUILD_FAILURE` (1), failed tests are `TESTS_FAILED`
(3), and an interrupted command is `INTERRUPTED` (8).

Not sent: output artifacts and their digests, analysis and DICE events,
per-action remote execution details, and commands other than `build` and
`test`.

## Examples

BuildBuddy:

```ini
[buck2_bes]
backend = grpcs://remote.buildbuddy.io
results_url = https://app.buildbuddy.io/invocation/
http_headers = x-buildbuddy-api-key: $BUILDBUDDY_API_KEY
```

Namespace (the values come from `nsc bazel setup`):

```ini
[buck2_bes]
backend = grpcs://<instance>.namespaced.app:443
results_url = https://cloud.namespace.so/<workspace>/bazel/invocation/
http_headers = x-nsc-ingress-auth: Bearer $NSC_TOKEN
```

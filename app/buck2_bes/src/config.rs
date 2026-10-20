/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Configuration of the Build Event Service publisher, read from the
//! `[buck2_bes]` buckconfig section.

use std::time::Duration;

use buck2_common::legacy_configs::configs::LegacyBuckConfig;
use buck2_common::legacy_configs::key::BuckconfigKeyRef;
use buck2_error::BuckErrorContext;
use buck2_re_configuration::HttpHeader;

const SECTION: &str = "buck2_bes";

const DEFAULT_UPLOAD_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_MAX_BUFFERED_EVENTS: usize = 10_000;

#[derive(Debug, buck2_error::Error)]
#[buck2(tag = Input)]
enum BesConfigError {
    #[error(
        "Invalid `{SECTION}.backend` `{0}`: use `grpcs://host[:port]` for TLS or `grpc://host:port` for plaintext"
    )]
    InvalidBackend(String),
    #[error("Environment variable `{0}` referenced in `{SECTION}` is not set")]
    MissingEnvVar(String),
}

/// Where and how to publish build events. Present only when `buck2_bes.backend` is set.
#[derive(Clone, Debug, PartialEq)]
pub struct BesConfig {
    /// The backend as an `http(s)://` URI suitable for tonic.
    pub endpoint: String,
    /// Whether to connect with TLS.
    pub tls: bool,
    /// Path to a PEM bundle of CA certificates to trust in addition to the system roots.
    pub tls_ca_certs: Option<String>,
    /// Path to a PEM file holding a client certificate and its private key.
    pub tls_client_cert: Option<String>,
    /// Headers sent with every request, typically for authentication. `$VAR`
    /// references are already substituted from the client's environment.
    pub http_headers: Vec<(String, String)>,
    /// Prefix of the URL at which the backend shows an invocation; the invocation ID is appended.
    pub results_url: Option<String>,
    /// Sent as the `project_id` of every request.
    pub instance_name: String,
    /// Extra notification keywords attached to the start of the stream.
    pub keywords: Vec<String>,
    /// Whether to publish `PublishLifecycleEvent`s around the build tool event stream.
    pub lifecycle_events: bool,
    /// How long the client waits, once the command has finished, for the backend to
    /// acknowledge the remaining events.
    pub upload_timeout: Duration,
    /// Number of unacknowledged events above which low-value events (successful actions and
    /// console output) are no longer published.
    pub max_buffered_events: usize,
    /// Whether to publish an `ActionExecuted` event for every action that ran a command, rather
    /// than only for failed ones.
    pub publish_all_actions: bool,
}

impl BesConfig {
    pub fn from_legacy_config(config: &LegacyBuckConfig) -> buck2_error::Result<Option<Self>> {
        Self::from_legacy_config_with_env(config, |var| std::env::var(var).ok())
    }

    fn from_legacy_config_with_env(
        config: &LegacyBuckConfig,
        env: impl Fn(&str) -> Option<String>,
    ) -> buck2_error::Result<Option<Self>> {
        let key = |property| BuckconfigKeyRef {
            section: SECTION,
            property,
        };

        let Some(backend) = config.get(key("backend")).filter(|b| !b.is_empty()) else {
            return Ok(None);
        };
        let (endpoint, tls) = parse_backend(backend)?;

        let http_headers = config
            .parse_list::<HttpHeader>(key("http_headers"))?
            .unwrap_or_default()
            .into_iter()
            .map(|h| {
                Ok((
                    substitute_env_vars(&h.key, &env)?,
                    substitute_env_vars(&h.value, &env)?,
                ))
            })
            .collect::<buck2_error::Result<Vec<_>>>()
            .buck_error_context("Invalid `buck2_bes.http_headers`")?;

        let substitute_path = |property| -> buck2_error::Result<Option<String>> {
            config
                .get(key(property))
                .map(|p| substitute_env_vars(p, &env))
                .transpose()
        };

        Ok(Some(Self {
            endpoint,
            tls,
            tls_ca_certs: substitute_path("tls_ca_certs")?,
            tls_client_cert: substitute_path("tls_client_cert")?,
            http_headers,
            results_url: config
                .get(key("results_url"))
                .filter(|u| !u.is_empty())
                .map(|u| u.to_owned()),
            instance_name: config
                .get(key("instance_name"))
                .unwrap_or_default()
                .to_owned(),
            keywords: config
                .parse_list::<String>(key("keywords"))?
                .unwrap_or_default()
                .into_iter()
                .map(|k| k.trim().to_owned())
                .filter(|k| !k.is_empty())
                .collect(),
            lifecycle_events: config.parse(key("lifecycle_events"))?.unwrap_or(true),
            upload_timeout: config
                .parse(key("upload_timeout_secs"))?
                .map(Duration::from_secs)
                .unwrap_or(DEFAULT_UPLOAD_TIMEOUT),
            max_buffered_events: config
                .parse(key("max_buffered_events"))?
                .unwrap_or(DEFAULT_MAX_BUFFERED_EVENTS),
            publish_all_actions: config.parse(key("publish_all_actions"))?.unwrap_or(true),
        }))
    }

    /// The URL at which the backend shows the given invocation, if `results_url` is set.
    pub fn invocation_url(&self, invocation_id: &str) -> Option<String> {
        self.results_url
            .as_ref()
            .map(|prefix| format!("{prefix}{invocation_id}"))
    }
}

/// Accepts the same forms as Bazel's `--bes_backend`: `grpcs://` (or no scheme) means TLS,
/// `grpc://` means plaintext.
fn parse_backend(backend: &str) -> buck2_error::Result<(String, bool)> {
    let invalid = || BesConfigError::InvalidBackend(backend.to_owned());
    let (rest, tls) = if let Some(rest) = backend.strip_prefix("grpcs://") {
        (rest, true)
    } else if let Some(rest) = backend.strip_prefix("grpc://") {
        (rest, false)
    } else if backend.contains("://") {
        return Err(invalid().into());
    } else {
        (backend, true)
    };
    let authority = rest.trim_end_matches('/');
    if authority.is_empty() || authority.contains('/') {
        return Err(invalid().into());
    }
    let scheme = if tls { "https" } else { "http" };
    Ok((format!("{scheme}://{authority}"), tls))
}

/// Replaces `$VAR` with the value of the environment variable `VAR`, matching the
/// `buck2_re_client` settings. A missing variable is an error rather than an empty
/// string, so that a forgotten token does not silently send an empty credential.
fn substitute_env_vars(
    s: &str,
    env: &impl Fn(&str) -> Option<String>,
) -> buck2_error::Result<String> {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find('$') {
        out.push_str(&rest[..start]);
        let after = &rest[start + 1..];
        let len = after
            .char_indices()
            .find(|&(i, c)| !(c == '_' || c.is_ascii_alphabetic() || (i > 0 && c.is_ascii_digit())))
            .map_or(after.len(), |(i, _)| i);
        if len == 0 {
            out.push('$');
        } else {
            let var = &after[..len];
            out.push_str(&env(var).ok_or_else(|| BesConfigError::MissingEnvVar(var.to_owned()))?);
        }
        rest = &after[len..];
    }
    out.push_str(rest);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use buck2_common::legacy_configs::configs::testing::parse;

    use super::*;

    fn config(text: &str) -> LegacyBuckConfig {
        parse(&[("config", text)], "config").unwrap()
    }

    fn env(var: &str) -> Option<String> {
        match var {
            "TOKEN" => Some("s3cr3t".to_owned()),
            "HOME" => Some("/home/me".to_owned()),
            _ => None,
        }
    }

    #[test]
    fn test_absent_backend_disables_publishing() {
        let c = config("[buck2_bes]\nresults_url = https://example.com/\n");
        assert_eq!(
            None,
            BesConfig::from_legacy_config_with_env(&c, env).unwrap()
        );
    }

    #[test]
    fn test_full_config() {
        let c = config(
            "[buck2_bes]\n\
             backend = grpcs://bes.example.com:443\n\
             results_url = https://example.com/invocation/\n\
             http_headers = x-nsc-ingress-auth: Bearer $TOKEN, x-other: v\n\
             tls_ca_certs = $HOME/ca.pem\n\
             instance_name = proj\n\
             keywords = a, b\n\
             lifecycle_events = false\n\
             upload_timeout_secs = 3\n\
             max_buffered_events = 7\n\
             publish_all_actions = false\n",
        );
        let c = BesConfig::from_legacy_config_with_env(&c, env)
            .unwrap()
            .unwrap();
        assert_eq!(
            BesConfig {
                endpoint: "https://bes.example.com:443".to_owned(),
                tls: true,
                tls_ca_certs: Some("/home/me/ca.pem".to_owned()),
                tls_client_cert: None,
                http_headers: vec![
                    ("x-nsc-ingress-auth".to_owned(), "Bearer s3cr3t".to_owned()),
                    ("x-other".to_owned(), "v".to_owned()),
                ],
                results_url: Some("https://example.com/invocation/".to_owned()),
                instance_name: "proj".to_owned(),
                keywords: vec!["a".to_owned(), "b".to_owned()],
                lifecycle_events: false,
                upload_timeout: Duration::from_secs(3),
                max_buffered_events: 7,
                publish_all_actions: false,
            },
            c
        );
        assert_eq!(
            Some("https://example.com/invocation/abc".to_owned()),
            c.invocation_url("abc")
        );
    }

    #[test]
    fn test_backend_schemes() {
        assert_eq!(
            ("http://localhost:1985".to_owned(), false),
            parse_backend("grpc://localhost:1985").unwrap()
        );
        assert_eq!(
            ("https://bes.example.com".to_owned(), true),
            parse_backend("bes.example.com").unwrap()
        );
        assert!(parse_backend("https://bes.example.com").is_err());
        assert!(parse_backend("grpcs://").is_err());
        assert!(parse_backend("grpcs://host/path").is_err());
    }

    #[test]
    fn test_substitute_env_vars() {
        assert_eq!(
            "Bearer s3cr3t",
            substitute_env_vars("Bearer $TOKEN", &env).unwrap()
        );
        assert_eq!("s3cr3t/x", substitute_env_vars("$TOKEN/x", &env).unwrap());
        // `$` not followed by a name, and names that start with a digit, are left alone.
        assert_eq!("a $ b $1", substitute_env_vars("a $ b $1", &env).unwrap());
        assert!(substitute_env_vars("$TOKENX", &env).is_err());
    }
}

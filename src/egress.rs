use std::{net::IpAddr, str, time::Duration};

use futures_util::StreamExt;
use reqwest::redirect::Policy;
use url::Url;

use crate::{
    config,
    monitor::{EgressState, MonitorHandle},
    providers::codex::{auth::constants::CODEX_API_ENDPOINT, client::ProxyEnvironment},
};

const CODEX_PROVIDER: &str = "codex";
const REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_TRACE_BODY_BYTES: usize = 4 * 1024;

/// Refreshes the Codex egress address without joining the model request path.
///
/// The caller must run this future as an independent task. Probe failures only update monitor
/// state and never propagate to the proxy server.
pub async fn refresh_codex_egress(monitor: MonitorHandle) {
    monitor.provider_egress_updated(CODEX_PROVIDER, EgressState::Resolving);
    let Some(probe) = EgressProbe::from_environment() else {
        monitor.provider_egress_updated(CODEX_PROVIDER, EgressState::Unavailable);
        return;
    };

    loop {
        let state = probe
            .lookup()
            .await
            .map(EgressState::Available)
            .unwrap_or(EgressState::Unavailable);
        monitor.provider_egress_updated(CODEX_PROVIDER, state);
        tokio::time::sleep(REFRESH_INTERVAL).await;
    }
}

struct EgressProbe {
    client: reqwest::Client,
    url: Url,
}

impl EgressProbe {
    fn from_environment() -> Option<Self> {
        let url = probe_url(&config::codex_base_url(CODEX_API_ENDPOINT))?;
        let client = ProxyEnvironment::try_from_env()
            .ok()?
            .apply(
                reqwest::Client::builder()
                    .connect_timeout(PROBE_TIMEOUT)
                    .timeout(PROBE_TIMEOUT)
                    .redirect(Policy::none()),
            )
            .build()
            .ok()?;
        Some(Self { client, url })
    }

    async fn lookup(&self) -> Option<String> {
        let response = self.client.get(self.url.clone()).send().await.ok()?;
        if !response.status().is_success() {
            return None;
        }

        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.ok()?;
            if body.len().saturating_add(chunk.len()) > MAX_TRACE_BODY_BYTES {
                return None;
            }
            body.extend_from_slice(&chunk);
        }
        parse_trace_ip(str::from_utf8(&body).ok()?)
    }
}

fn probe_url(base_url: &str) -> Option<Url> {
    let mut url = Url::parse(base_url).ok()?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return None;
    }
    url.set_path("/cdn-cgi/trace");
    url.set_query(None);
    url.set_fragment(None);
    Some(url)
}

fn parse_trace_ip(body: &str) -> Option<String> {
    body.lines()
        .find_map(|line| line.strip_prefix("ip="))
        .and_then(|value| value.trim().parse::<IpAddr>().ok())
        .map(|address| address.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_url_keeps_the_configured_codex_origin() {
        let url = probe_url("https://proxy.example:8443/custom/responses?mode=test").unwrap();

        assert_eq!(url.as_str(), "https://proxy.example:8443/cdn-cgi/trace");
    }

    #[test]
    fn trace_parser_accepts_ipv4_and_ipv6_addresses() {
        assert_eq!(
            parse_trace_ip("fl=1\nip=178.249.214.12\nloc=CA\n"),
            Some("178.249.214.12".to_string())
        );
        assert_eq!(
            parse_trace_ip("ip=2401:fae0:184a::ca55\nloc=TW\n"),
            Some("2401:fae0:184a::ca55".to_string())
        );
    }

    #[test]
    fn trace_parser_rejects_missing_or_invalid_addresses() {
        assert_eq!(parse_trace_ip("loc=CA\ncolo=YYZ\n"), None);
        assert_eq!(parse_trace_ip("ip=not-an-address\n"), None);
    }
}

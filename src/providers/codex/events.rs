use std::time::{Duration, SystemTime};

use serde_json::Value;

use crate::monitor::{MonitorHandle, ProviderCredits, ProviderQuotaWindow};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CodexFailureKind {
    RateLimit,
    Overloaded,
    Transient,
    Permanent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CodexEventFailure {
    pub kind: CodexFailureKind,
    pub explicit_status: Option<u16>,
    pub status: u16,
    pub message: String,
    pub retry_after: Option<String>,
}

impl CodexEventFailure {
    pub fn retryable(&self) -> bool {
        !matches!(self.kind, CodexFailureKind::Permanent)
    }
}

pub(crate) fn record_rate_limit_snapshots_from_sse(monitor: Option<&MonitorHandle>, body: &[u8]) {
    for event in crate::anthropic::sse::parse_sse_events(body) {
        if let Ok(payload) = serde_json::from_str::<Value>(&event.data) {
            record_rate_limit_snapshot(monitor, &payload);
        }
    }
}

pub(crate) fn record_rate_limit_snapshot(monitor: Option<&MonitorHandle>, payload: &Value) {
    let Some((windows, limit_reached, credits)) = rate_limit_snapshot(payload) else {
        return;
    };
    if let Some(monitor) = monitor {
        monitor.provider_quota_updated("codex", windows, limit_reached, credits);
    }
}

fn rate_limit_snapshot(
    payload: &Value,
) -> Option<(Vec<ProviderQuotaWindow>, bool, ProviderCredits)> {
    if payload.get("type").and_then(Value::as_str) != Some("codex.rate_limits") {
        return None;
    }
    let rate_limits = payload.get("rate_limits")?;
    let windows = ["primary", "secondary"]
        .into_iter()
        .filter_map(|name| rate_limit_window(rate_limits.get(name)?))
        .collect::<Vec<_>>();
    if windows.is_empty() {
        return None;
    }
    let credits = payload.get("credits");
    Some((
        windows,
        rate_limits
            .get("limit_reached")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        ProviderCredits {
            has_credits: credits
                .and_then(|value| value.get("has_credits"))
                .and_then(Value::as_bool),
            unlimited: credits
                .and_then(|value| value.get("unlimited"))
                .and_then(Value::as_bool),
            balance: credits
                .and_then(|value| value.get("balance"))
                .and_then(Value::as_f64),
        },
    ))
}

fn rate_limit_window(window: &Value) -> Option<ProviderQuotaWindow> {
    let used_percent = window.get("used_percent")?.as_f64()?;
    let window_minutes = window.get("window_minutes")?.as_u64()?;
    let resets_at = window
        .get("reset_at")
        .and_then(Value::as_u64)
        .and_then(|timestamp| SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(timestamp)))
        .or_else(|| {
            window
                .get("reset_after_seconds")
                .and_then(Value::as_f64)
                .and_then(|seconds| Duration::try_from_secs_f64(seconds).ok())
                .and_then(|duration| SystemTime::now().checked_add(duration))
        });
    Some(ProviderQuotaWindow {
        used_percent,
        window_minutes,
        resets_at,
    })
}

pub(crate) fn is_terminal_rate_limit_event(payload: &Value) -> bool {
    payload.get("type").and_then(Value::as_str) == Some("codex.rate_limits")
        && payload
            .pointer("/rate_limits/limit_reached")
            .and_then(Value::as_bool)
            == Some(true)
        && payload
            .pointer("/credits/has_credits")
            .and_then(Value::as_bool)
            != Some(true)
        && payload
            .pointer("/credits/unlimited")
            .and_then(Value::as_bool)
            != Some(true)
}

pub(crate) fn event_error(payload: &Value) -> Option<&Value> {
    payload
        .get("error")
        .or_else(|| payload.pointer("/response/error"))
}

pub(crate) fn classify_event_failure(payload: &Value) -> Option<CodexEventFailure> {
    let event_type = payload.get("type").and_then(Value::as_str)?;
    if event_type == "codex.rate_limits" {
        if !is_terminal_rate_limit_event(payload) {
            return None;
        }
        return Some(CodexEventFailure {
            kind: CodexFailureKind::RateLimit,
            explicit_status: Some(429),
            status: 429,
            message: "rate limit reached".to_string(),
            retry_after: scalar_string(payload.pointer("/rate_limits/primary/reset_after_seconds")),
        });
    }
    if !matches!(event_type, "response.failed" | "response.error" | "error") {
        return None;
    }

    let error = event_error(payload);
    let explicit_status = numeric_status(payload)
        .or_else(|| {
            error
                .and_then(|value| value.get("status"))
                .and_then(Value::as_u64)
        })
        .and_then(|status| u16::try_from(status).ok());
    let message = error
        .and_then(|value| value.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("Upstream error")
        .to_string();
    let code = error
        .and_then(|value| value.get("code"))
        .and_then(Value::as_str);
    let error_type = error
        .and_then(|value| value.get("type"))
        .and_then(Value::as_str);
    let lower = message.to_ascii_lowercase();

    let kind = if explicit_status == Some(429) || lower.contains("rate limit") {
        CodexFailureKind::RateLimit
    } else if explicit_status == Some(529)
        || code == Some("overloaded_error")
        || error_type == Some("overloaded_error")
        || lower.contains("overloaded")
    {
        CodexFailureKind::Overloaded
    } else if explicit_status.is_some_and(|status| matches!(status, 500 | 502 | 503 | 504))
        || matches!(
            code,
            Some("server_error" | "internal_server_error" | "internal_error")
        )
        || matches!(
            error_type,
            Some("server_error" | "internal_server_error" | "internal_error")
        )
        || retryable_message(&lower)
    {
        CodexFailureKind::Transient
    } else {
        CodexFailureKind::Permanent
    };
    let status = explicit_status.unwrap_or(match kind {
        CodexFailureKind::RateLimit => 429,
        CodexFailureKind::Overloaded => 529,
        CodexFailureKind::Transient => 503,
        CodexFailureKind::Permanent => 500,
    });
    let retry_after = error
        .and_then(|value| value.get("retry_after"))
        .and_then(scalar_string_value)
        .or_else(|| {
            error
                .and_then(|value| value.get("retry_after_seconds"))
                .and_then(scalar_string_value)
        })
        .or_else(|| scalar_string(payload.get("retry_after_seconds")))
        .or_else(|| scalar_string(payload.pointer("/headers/retry-after")))
        .or_else(|| scalar_string(payload.pointer("/headers/Retry-After")));

    Some(CodexEventFailure {
        kind,
        explicit_status,
        status,
        message,
        retry_after,
    })
}

pub(crate) fn first_retryable_failure(body: &[u8]) -> Option<CodexEventFailure> {
    for event in crate::anthropic::sse::parse_sse_events(body) {
        if event.data == "[DONE]" {
            continue;
        }
        let Ok(payload) = serde_json::from_str::<Value>(&event.data) else {
            continue;
        };
        if let Some(failure) = classify_event_failure(&payload)
            && failure.retryable()
        {
            return Some(failure);
        }
    }
    None
}

pub(crate) fn numeric_status(payload: &Value) -> Option<u64> {
    payload
        .get("status")
        .and_then(Value::as_u64)
        .or_else(|| payload.get("status_code").and_then(Value::as_u64))
}

fn scalar_string(value: Option<&Value>) -> Option<String> {
    value.and_then(scalar_string_value)
}

fn scalar_string_value(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn retryable_message(message: &str) -> bool {
    [
        "server error",
        "internal server error",
        "service unavailable",
        "bad gateway",
        "gateway timeout",
        "temporarily unavailable",
        "you can retry your request",
        "socket connection was closed unexpectedly",
        "connection closed unexpectedly",
        "operation timed out",
        "connection reset",
        "connection closed",
        "timed out",
        "timeout",
        "econnreset",
        "epipe",
        "etimedout",
        "und_err_socket",
        "fetch failed",
        "unexpected eof",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_account_quota_windows_and_credits() {
        let reset_at = 4_102_444_800_u64;
        let (windows, limit_reached, credits) = rate_limit_snapshot(&serde_json::json!({
            "type": "codex.rate_limits",
            "rate_limits": {
                "limit_reached": false,
                "primary": {
                    "used_percent": 28.5,
                    "window_minutes": 300,
                    "reset_at": reset_at
                },
                "secondary": {
                    "used_percent": 59,
                    "window_minutes": 10080,
                    "reset_after_seconds": 3600
                }
            },
            "credits": {
                "has_credits": true,
                "unlimited": false,
                "balance": 12.5
            }
        }))
        .unwrap();

        assert!(!limit_reached);
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].used_percent, 28.5);
        assert_eq!(windows[0].window_minutes, 300);
        assert_eq!(
            windows[0].resets_at,
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(reset_at))
        );
        assert_eq!(windows[1].window_minutes, 10_080);
        assert!(windows[1].resets_at.is_some());
        assert_eq!(credits.has_credits, Some(true));
        assert_eq!(credits.unlimited, Some(false));
        assert_eq!(credits.balance, Some(12.5));
    }

    #[test]
    fn ignores_non_quota_and_incomplete_windows() {
        assert!(rate_limit_snapshot(&serde_json::json!({"type": "keepalive"})).is_none());
        assert!(
            rate_limit_snapshot(&serde_json::json!({
                "type": "codex.rate_limits",
                "rate_limits": {
                    "primary": {"used_percent": 25},
                    "secondary": {"window_minutes": 10080}
                }
            }))
            .is_none()
        );
    }

    #[test]
    fn ignores_invalid_reset_durations_without_panicking() {
        let window = rate_limit_window(&serde_json::json!({
            "used_percent": 25,
            "window_minutes": 300,
            "reset_after_seconds": 1e300
        }))
        .unwrap();

        assert_eq!(window.resets_at, None);
    }

    #[test]
    fn classifies_retryable_failure_kinds() {
        let rate = classify_event_failure(&serde_json::json!({
            "type": "codex.rate_limits",
            "rate_limits": {"limit_reached": true, "primary": {"reset_after_seconds": 1.5}}
        }))
        .unwrap();
        assert_eq!(rate.kind, CodexFailureKind::RateLimit);
        assert_eq!(rate.retry_after.as_deref(), Some("1.5"));

        let overload = classify_event_failure(&serde_json::json!({
            "type": "response.failed",
            "response": {"error": {"type": "overloaded_error", "message": "busy"}}
        }))
        .unwrap();
        assert_eq!(overload.status, 529);
        assert!(overload.retryable());
    }

    #[test]
    fn terminal_rate_limit_honors_credits() {
        // No credits field at all: legacy payload stays terminal.
        assert!(is_terminal_rate_limit_event(&serde_json::json!({
            "type": "codex.rate_limits",
            "rate_limits": {"limit_reached": true}
        })));

        // Credits exhausted: terminal.
        assert!(is_terminal_rate_limit_event(&serde_json::json!({
            "type": "codex.rate_limits",
            "rate_limits": {"limit_reached": true},
            "credits": {"has_credits": false, "unlimited": false}
        })));

        // Usable credits remain: informational.
        assert!(!is_terminal_rate_limit_event(&serde_json::json!({
            "type": "codex.rate_limits",
            "rate_limits": {"limit_reached": true},
            "credits": {"has_credits": true, "unlimited": false}
        })));

        // Unlimited plan: informational.
        assert!(!is_terminal_rate_limit_event(&serde_json::json!({
            "type": "codex.rate_limits",
            "rate_limits": {"limit_reached": true},
            "credits": {"has_credits": false, "unlimited": true}
        })));

        // Limit not reached: never terminal, credits irrelevant.
        assert!(!is_terminal_rate_limit_event(&serde_json::json!({
            "type": "codex.rate_limits",
            "rate_limits": {"limit_reached": false},
            "credits": {"has_credits": false, "unlimited": false}
        })));

        // Wrong event type never matches.
        assert!(!is_terminal_rate_limit_event(&serde_json::json!({
            "type": "response.completed",
            "rate_limits": {"limit_reached": true}
        })));
    }

    #[test]
    fn classifier_skips_credited_rate_limit_snapshots() {
        assert!(
            classify_event_failure(&serde_json::json!({
                "type": "codex.rate_limits",
                "rate_limits": {"limit_reached": true},
                "credits": {"has_credits": true, "unlimited": false}
            }))
            .is_none()
        );
    }

    #[test]
    fn ignores_informational_and_permanent_events() {
        assert!(
            classify_event_failure(&serde_json::json!({
                "type": "codex.rate_limits",
                "rate_limits": {"limit_reached": false}
            }))
            .is_none()
        );
        let failure = classify_event_failure(&serde_json::json!({
            "type": "error",
            "error": {"status": 400, "message": "bad request"}
        }))
        .unwrap();
        assert!(!failure.retryable());
    }
}

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use serde_json::Value;

use crate::monitor::{MonitorHandle, ProviderCredits, ProviderQuotaWindow};

use super::translate::reasoning_signature::{PendingReasoning, encode_reasoning_signature};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CodexFailureKind {
    RateLimit,
    Overloaded,
    Transient,
    Permanent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CodexStreamEventKind {
    Control,
    Structural,
    Semantic,
    TerminalSuccess,
    TerminalFailure,
}

#[derive(Debug, Default)]
pub(crate) struct CodexStreamForwardingClassifier {
    buffering_hosted_search_output: bool,
    reasoning_by_output_index: HashMap<usize, PendingReasoning>,
}

impl CodexStreamForwardingClassifier {
    pub(crate) fn classify(&mut self, payload: &Value) -> CodexStreamEventKind {
        let kind = classify_stream_event(payload);
        if matches!(
            kind,
            CodexStreamEventKind::TerminalSuccess | CodexStreamEventKind::TerminalFailure
        ) {
            self.buffering_hosted_search_output = false;
            self.reasoning_by_output_index.clear();
            return kind;
        }

        let event_type = payload.get("type").and_then(Value::as_str);
        if matches!(
            event_type,
            Some("response.output_item.added" | "response.output_item.done")
        ) && let Some(item) = payload
            .get("item")
            .filter(|item| item.get("type").and_then(Value::as_str) == Some("reasoning"))
        {
            let output_index = payload
                .get("output_index")
                .and_then(Value::as_u64)
                .unwrap_or(0) as usize;
            let pending = self
                .reasoning_by_output_index
                .entry(output_index)
                .or_default();
            pending.capture(item);
            if event_type == Some("response.output_item.done") {
                let emits_signature = pending
                    .replay()
                    .and_then(|replay| encode_reasoning_signature(&replay))
                    .is_some();
                self.reasoning_by_output_index.remove(&output_index);
                if emits_signature {
                    self.buffering_hosted_search_output = false;
                    self.reasoning_by_output_index.clear();
                    return CodexStreamEventKind::Semantic;
                }
            }
        }

        let completed_hosted_search = event_type == Some("response.output_item.done")
            && payload.pointer("/item/type").and_then(Value::as_str) == Some("web_search_call");
        if completed_hosted_search {
            self.buffering_hosted_search_output = true;
            return CodexStreamEventKind::Structural;
        }

        if self.buffering_hosted_search_output && event_type == Some("response.output_text.delta") {
            return CodexStreamEventKind::Structural;
        }

        if kind == CodexStreamEventKind::Semantic {
            self.buffering_hosted_search_output = false;
            self.reasoning_by_output_index.clear();
        }
        kind
    }
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

    pub fn client_status(&self) -> u16 {
        self.status
    }

    pub fn client_error_type(&self) -> &'static str {
        match self.client_status() {
            400 | 422 => "invalid_request_error",
            401 => "authentication_error",
            403 => "permission_error",
            413 => "request_too_large",
            429 => "rate_limit_error",
            529 => "overloaded_error",
            _ => "api_error",
        }
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

pub(crate) fn classify_stream_event(payload: &Value) -> CodexStreamEventKind {
    if classify_event_failure(payload).is_some() {
        return CodexStreamEventKind::TerminalFailure;
    }
    match payload.get("type").and_then(Value::as_str) {
        Some("response.completed" | "response.done") => CodexStreamEventKind::TerminalSuccess,
        Some(
            "keepalive"
            | "response.created"
            | "response.in_progress"
            | "codex.rate_limits"
            | "response.web_search_call.in_progress"
            | "response.web_search_call.searching"
            | "response.web_search_call.completed",
        ) => CodexStreamEventKind::Control,
        Some(
            "response.reasoning_summary_text.delta"
            | "response.output_text.delta"
            | "response.function_call_arguments.delta",
        ) if payload
            .get("delta")
            .and_then(Value::as_str)
            .is_some_and(|delta| !delta.is_empty()) =>
        {
            CodexStreamEventKind::Semantic
        }
        Some("response.output_item.done")
            if matches!(
                payload.pointer("/item/type").and_then(Value::as_str),
                Some("function_call" | "web_search_call")
            ) =>
        {
            CodexStreamEventKind::Semantic
        }
        _ => CodexStreamEventKind::Structural,
    }
}

pub(crate) fn event_is_terminal(payload: &Value) -> bool {
    matches!(
        classify_stream_event(payload),
        CodexStreamEventKind::TerminalSuccess | CodexStreamEventKind::TerminalFailure
    )
}

pub(crate) fn event_is_success_terminal(payload: &Value) -> bool {
    classify_stream_event(payload) == CodexStreamEventKind::TerminalSuccess
}

pub(crate) fn event_error(payload: &Value) -> Option<&Value> {
    payload
        .get("error")
        .filter(|error| !error.is_null())
        .or_else(|| {
            payload
                .pointer("/response/error")
                .filter(|error| !error.is_null())
        })
}

/// Extracts an exhausted-quota hint without turning a rate-limit snapshot into
/// a terminal event. Callers may use the hint only when the stream later ends
/// without an explicit terminal response event.
pub(crate) fn quota_failure_hint(payload: &Value) -> Option<CodexEventFailure> {
    if payload.get("type").and_then(Value::as_str) != Some("codex.rate_limits")
        || payload
            .pointer("/rate_limits/limit_reached")
            .and_then(Value::as_bool)
            != Some(true)
        || payload
            .pointer("/credits/has_credits")
            .and_then(Value::as_bool)
            == Some(true)
        || payload
            .pointer("/credits/unlimited")
            .and_then(Value::as_bool)
            == Some(true)
    {
        return None;
    }

    Some(CodexEventFailure {
        kind: CodexFailureKind::RateLimit,
        explicit_status: Some(429),
        status: 429,
        message: "rate limit reached".to_string(),
        retry_after: scalar_string(payload.pointer("/rate_limits/primary/reset_after_seconds")),
    })
}

pub(crate) fn response_is_incomplete_terminal(payload: &Value) -> bool {
    let event_type = payload.get("type").and_then(Value::as_str);
    let incomplete_details = payload.pointer("/response/incomplete_details");
    matches!(
        event_type,
        Some("response.completed" | "response.incomplete" | "response.done")
    ) && (event_type == Some("response.incomplete")
        || payload.pointer("/response/status").and_then(Value::as_str) == Some("incomplete")
        || incomplete_details.is_some_and(|details| !details.is_null()))
}

pub(crate) fn is_standard_max_output_tokens_incomplete(payload: &Value) -> bool {
    let status = payload.pointer("/response/status");
    payload.get("type").and_then(Value::as_str) == Some("response.incomplete")
        && (status.is_none() || status.and_then(Value::as_str) == Some("incomplete"))
        && payload
            .pointer("/response/incomplete_details/reason")
            .and_then(Value::as_str)
            == Some("max_output_tokens")
        && event_error(payload).is_none()
}

pub(crate) fn classify_event_failure(payload: &Value) -> Option<CodexEventFailure> {
    let event_type = payload.get("type").and_then(Value::as_str)?;
    let response_status = payload.pointer("/response/status").and_then(Value::as_str);
    if event_type == "codex.rate_limits" {
        return None;
    }
    if response_is_incomplete_terminal(payload) {
        let reason = payload
            .pointer("/response/incomplete_details/reason")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        return Some(CodexEventFailure {
            kind: CodexFailureKind::Transient,
            explicit_status: None,
            status: 503,
            message: format!("Incomplete response returned, reason: {reason}"),
            retry_after: None,
        });
    }
    let error = event_error(payload);
    if !matches!(event_type, "response.failed" | "response.error" | "error")
        && response_status != Some("failed")
        && error.is_none()
    {
        return None;
    }

    let explicit_status = numeric_status(payload)
        .or_else(|| {
            error
                .and_then(|value| value.get("status"))
                .and_then(Value::as_u64)
        })
        .and_then(|status| u16::try_from(status).ok());
    let code = error
        .and_then(|value| value.get("code"))
        .and_then(Value::as_str);
    let error_type = error
        .and_then(|value| value.get("type"))
        .and_then(Value::as_str);
    let raw_message = error
        .and_then(|value| value.get("message"))
        .and_then(Value::as_str);
    let message = match (code, raw_message) {
        (Some("cyber_policy"), None | Some("")) => {
            "This request has been flagged for possible cybersecurity risk.".to_string()
        }
        (Some("cyber_policy"), Some(message)) if message.trim().is_empty() => {
            "This request has been flagged for possible cybersecurity risk.".to_string()
        }
        (Some("invalid_prompt" | "bio_policy"), None) => "Invalid request.".to_string(),
        (_, Some(message)) => message.to_string(),
        _ => "Upstream error".to_string(),
    };
    let lower = message.to_ascii_lowercase();
    let context_window = code == Some("context_length_exceeded")
        || lower.contains("context window")
        || lower.contains("context length exceeded");

    let kind = if context_window
        || matches!(
            code,
            Some(
                "context_length_exceeded"
                    | "insufficient_quota"
                    | "usage_not_included"
                    | "cyber_policy"
                    | "invalid_prompt"
                    | "bio_policy"
            )
        ) {
        CodexFailureKind::Permanent
    } else if matches!(code, Some("server_is_overloaded" | "slow_down")) {
        CodexFailureKind::Overloaded
    } else if explicit_status == Some(429) || lower.contains("rate limit") {
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
    } else if explicit_status.is_some() || matches!(event_type, "response.error" | "error") {
        CodexFailureKind::Permanent
    } else {
        // Native Codex treats an unclassified response.failed event as
        // retryable instead of silently converting it into a successful turn.
        CodexFailureKind::Transient
    };
    let status = if context_window {
        413
    } else {
        explicit_status.unwrap_or(match code {
            Some("cyber_policy" | "invalid_prompt" | "bio_policy") => 400,
            Some("insufficient_quota") => 429,
            Some("usage_not_included") => 403,
            _ => match kind {
                CodexFailureKind::RateLimit => 429,
                CodexFailureKind::Overloaded => 529,
                CodexFailureKind::Transient => 503,
                CodexFailureKind::Permanent => 500,
            },
        })
    };
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

/// Returns the first retryable terminal failure, or the latest exhausted-quota
/// hint when an otherwise valid event stream ends without any terminal event.
pub(crate) fn first_retryable_failure(body: &[u8]) -> Option<CodexEventFailure> {
    let mut quota_hint = None;
    for event in crate::anthropic::sse::parse_sse_events(body) {
        if event.data == "[DONE]" {
            continue;
        }
        let Ok(payload) = serde_json::from_str::<Value>(&event.data) else {
            continue;
        };
        if payload.get("type").and_then(Value::as_str) == Some("codex.rate_limits") {
            quota_hint = quota_failure_hint(&payload);
        }
        if let Some(failure) = classify_event_failure(&payload) {
            return failure.retryable().then_some(failure);
        }
        if event_is_terminal(&payload) {
            return None;
        }
    }
    quota_hint
}

pub(crate) fn first_event_failure(body: &[u8]) -> Option<CodexEventFailure> {
    for event in crate::anthropic::sse::parse_sse_events(body) {
        if event.data == "[DONE]" {
            continue;
        }
        let Ok(payload) = serde_json::from_str::<Value>(&event.data) else {
            continue;
        };
        if let Some(failure) = classify_event_failure(&payload) {
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
        let overload = classify_event_failure(&serde_json::json!({
            "type": "response.failed",
            "response": {"error": {"type": "overloaded_error", "message": "busy"}}
        }))
        .unwrap();
        assert_eq!(overload.status, 529);
        assert!(overload.retryable());
    }

    #[test]
    fn rate_limit_snapshots_are_telemetry_with_terminal_fallback_hints() {
        let exhausted = serde_json::json!({
            "type": "codex.rate_limits",
            "rate_limits": {
                "limit_reached": true,
                "primary": {"reset_after_seconds": 518773}
            },
            "credits": {"has_credits": false, "unlimited": false}
        });
        assert!(classify_event_failure(&exhausted).is_none());
        let hint = quota_failure_hint(&exhausted).unwrap();
        assert_eq!(hint.status, 429);
        assert_eq!(hint.retry_after.as_deref(), Some("518773"));

        let legacy_exhausted = serde_json::json!({
            "type": "codex.rate_limits",
            "rate_limits": {"limit_reached": true}
        });
        assert!(quota_failure_hint(&legacy_exhausted).is_some());

        for usable in [
            serde_json::json!({
                "type": "codex.rate_limits",
                "rate_limits": {"limit_reached": true},
                "credits": {"has_credits": true, "unlimited": false}
            }),
            serde_json::json!({
                "type": "codex.rate_limits",
                "rate_limits": {"limit_reached": true},
                "credits": {"has_credits": false, "unlimited": true}
            }),
            serde_json::json!({
                "type": "codex.rate_limits",
                "rate_limits": {"limit_reached": false}
            }),
        ] {
            assert!(classify_event_failure(&usable).is_none());
            assert!(quota_failure_hint(&usable).is_none());
        }
    }

    #[test]
    fn buffered_quota_hint_applies_only_without_terminal_event() {
        let snapshot = "data: {\"type\":\"codex.rate_limits\",\"rate_limits\":{\"limit_reached\":true,\"primary\":{\"reset_after_seconds\":31}},\"credits\":{\"has_credits\":false,\"unlimited\":false}}\n\n";
        let hint = first_retryable_failure(snapshot.as_bytes()).unwrap();
        assert_eq!(hint.status, 429);
        assert_eq!(hint.retry_after.as_deref(), Some("31"));

        let completed = format!(
            "{snapshot}data: {{\"type\":\"response.completed\",\"response\":{{\"status\":\"completed\"}}}}\n\n"
        );
        assert!(first_retryable_failure(completed.as_bytes()).is_none());
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

    #[test]
    fn forwarding_classifier_commits_signature_only_reasoning() {
        let mut classifier = CodexStreamForwardingClassifier::default();
        assert_eq!(
            classifier.classify(&serde_json::json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {
                    "type": "reasoning",
                    "id": "rs_1",
                    "encrypted_content": "opaque"
                }
            })),
            CodexStreamEventKind::Structural
        );
        assert_eq!(
            classifier.classify(&serde_json::json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {"type": "reasoning", "id": "rs_1"}
            })),
            CodexStreamEventKind::Semantic
        );
    }

    #[test]
    fn forwarding_classifier_keeps_unencodable_reasoning_structural() {
        let mut classifier = CodexStreamForwardingClassifier::default();
        assert_eq!(
            classifier.classify(&serde_json::json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {"type": "reasoning", "id": "rs_1"}
            })),
            CodexStreamEventKind::Structural
        );
        assert_eq!(
            classifier.classify(&serde_json::json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {"type": "reasoning", "id": "rs_1"}
            })),
            CodexStreamEventKind::Structural
        );
    }

    #[test]
    fn forwarding_classifier_buffers_hosted_search_until_terminal() {
        let mut classifier = CodexStreamForwardingClassifier::default();
        assert_eq!(
            classifier.classify(&serde_json::json!({
                "type": "response.output_item.done",
                "item": {"type": "web_search_call", "id": "ws_1"}
            })),
            CodexStreamEventKind::Structural
        );
        assert_eq!(
            classifier.classify(&serde_json::json!({
                "type": "response.output_text.delta",
                "delta": "deferred answer"
            })),
            CodexStreamEventKind::Structural
        );
        assert_eq!(
            classifier.classify(&serde_json::json!({
                "type": "response.completed",
                "response": {"status": "completed"}
            })),
            CodexStreamEventKind::TerminalSuccess
        );
    }

    #[test]
    fn forwarding_classifier_commits_after_a_recovery_anchor_opens() {
        let mut classifier = CodexStreamForwardingClassifier::default();
        assert_eq!(
            classifier.classify(&serde_json::json!({
                "type": "response.output_item.done",
                "item": {"type": "web_search_call", "id": "ws_1"}
            })),
            CodexStreamEventKind::Structural
        );
        assert_eq!(
            classifier.classify(&serde_json::json!({
                "type": "response.reasoning_summary_text.delta",
                "delta": "plan"
            })),
            CodexStreamEventKind::Semantic
        );
        assert_eq!(
            classifier.classify(&serde_json::json!({
                "type": "response.output_text.delta",
                "delta": "deferred answer"
            })),
            CodexStreamEventKind::Semantic
        );
    }

    #[test]
    fn classifies_progress_and_terminal_semantics() {
        assert_eq!(
            classify_stream_event(&serde_json::json!({"type":"response.created"})),
            CodexStreamEventKind::Control
        );
        assert_eq!(
            classify_stream_event(&serde_json::json!({
                "type":"response.output_item.added",
                "item":{"type":"function_call"}
            })),
            CodexStreamEventKind::Structural
        );
        assert_eq!(
            classify_stream_event(&serde_json::json!({
                "type":"response.function_call_arguments.delta",
                "delta":"{}"
            })),
            CodexStreamEventKind::Semantic
        );
        assert_eq!(
            classify_stream_event(&serde_json::json!({"type":"response.incomplete"})),
            CodexStreamEventKind::TerminalFailure
        );
        assert_eq!(
            classify_stream_event(&serde_json::json!({
                "type":"response.completed",
                "error": null,
                "response": {
                    "status":"failed",
                    "error":{"status":400,"code":"invalid_prompt","message":"rejected"}
                }
            })),
            CodexStreamEventKind::TerminalFailure
        );
    }

    #[test]
    fn nested_response_error_survives_null_top_level_error() {
        let failure = classify_event_failure(&serde_json::json!({
            "type":"response.completed",
            "error": null,
            "response": {
                "status":"failed",
                "error":{"status":400,"code":"invalid_prompt","message":"nested rejection"}
            }
        }))
        .unwrap();
        assert_eq!(failure.client_status(), 400);
        assert_eq!(failure.client_error_type(), "invalid_request_error");
        assert_eq!(failure.message, "nested rejection");
        assert!(!failure.retryable());
    }

    #[test]
    fn completed_event_with_nested_error_is_not_success() {
        let payload = serde_json::json!({
            "type":"response.completed",
            "error": null,
            "response": {
                "status":"completed",
                "error":{"status":502,"code":"server_error","message":"late failure"}
            }
        });
        let failure = classify_event_failure(&payload).unwrap();
        assert_eq!(failure.client_status(), 502);
        assert!(failure.retryable());
        assert_eq!(
            classify_stream_event(&payload),
            CodexStreamEventKind::TerminalFailure
        );
    }

    #[test]
    fn completed_event_with_nested_null_error_is_success() {
        let payload = serde_json::json!({
            "type":"response.completed",
            "error": null,
            "response": {"status":"completed", "error":null}
        });
        assert!(classify_event_failure(&payload).is_none());
        assert_eq!(
            classify_stream_event(&payload),
            CodexStreamEventKind::TerminalSuccess
        );
    }

    #[test]
    fn native_fatal_codes_are_not_retried_without_numeric_status() {
        for (code, status, error_type) in [
            ("context_length_exceeded", 413, "request_too_large"),
            ("cyber_policy", 400, "invalid_request_error"),
            ("invalid_prompt", 400, "invalid_request_error"),
            ("bio_policy", 400, "invalid_request_error"),
            ("insufficient_quota", 429, "rate_limit_error"),
            ("usage_not_included", 403, "permission_error"),
        ] {
            let failure = classify_event_failure(&serde_json::json!({
                "type":"response.failed",
                "response":{"status":"failed","error":{"code":code,"message":"rejected"}}
            }))
            .unwrap();
            assert!(!failure.retryable(), "{code}");
            assert_eq!(failure.client_status(), status, "{code}");
            assert_eq!(failure.client_error_type(), error_type, "{code}");
        }
    }

    #[test]
    fn native_overload_codes_remain_retryable_without_numeric_status() {
        for code in ["server_is_overloaded", "slow_down"] {
            let failure = classify_event_failure(&serde_json::json!({
                "type":"response.failed",
                "response":{"status":"failed","error":{"code":code,"message":"busy"}}
            }))
            .unwrap();
            assert!(failure.retryable(), "{code}");
            assert_eq!(failure.client_status(), 529, "{code}");
            assert_eq!(failure.client_error_type(), "overloaded_error", "{code}");
        }
    }

    #[test]
    fn context_window_message_overrides_generic_upstream_400() {
        let failure = classify_event_failure(&serde_json::json!({
            "type":"error",
            "status":400,
            "error":{"type":"invalid_request_error","message":"input exceeds context window"}
        }))
        .unwrap();
        assert!(!failure.retryable());
        assert_eq!(failure.explicit_status, Some(400));
        assert_eq!(failure.client_status(), 413);
        assert_eq!(failure.client_error_type(), "request_too_large");
    }

    #[test]
    fn incomplete_policy_only_accepts_consistent_max_output_terminal() {
        let allowed = serde_json::json!({
            "type":"response.incomplete",
            "response": {
                "status":"incomplete",
                "error":null,
                "incomplete_details":{"reason":"max_output_tokens"}
            }
        });
        assert!(response_is_incomplete_terminal(&allowed));
        assert!(is_standard_max_output_tokens_incomplete(&allowed));
        let allowed_without_status = serde_json::json!({
            "type":"response.incomplete",
            "response": {
                "incomplete_details":{"reason":"max_output_tokens"}
            }
        });
        assert!(is_standard_max_output_tokens_incomplete(
            &allowed_without_status
        ));

        for rejected in [
            serde_json::json!({
                "type":"response.incomplete",
                "response":{"status":"incomplete","incomplete_details":{"reason":"content_filter"}}
            }),
            serde_json::json!({
                "type":"response.incomplete",
                "response":{"status":"incomplete","incomplete_details":{}}
            }),
            serde_json::json!({
                "type":"response.completed",
                "response":{"status":"completed","incomplete_details":{"reason":"max_output_tokens"}}
            }),
            serde_json::json!({
                "type":"response.incomplete",
                "response":{"status":null,"incomplete_details":{"reason":"max_output_tokens"}}
            }),
            serde_json::json!({
                "type":"response.incomplete",
                "response":{"status":123,"incomplete_details":{"reason":"max_output_tokens"}}
            }),
            serde_json::json!({
                "type":"response.incomplete",
                "response":{"status":"completed","incomplete_details":{"reason":"max_output_tokens"}}
            }),
            serde_json::json!({
                "type":"response.completed",
                "response":{"status":"completed","incomplete_details":{}}
            }),
        ] {
            assert!(response_is_incomplete_terminal(&rejected));
            assert!(!is_standard_max_output_tokens_incomplete(&rejected));
            assert!(classify_event_failure(&rejected).is_some());
        }
    }
}

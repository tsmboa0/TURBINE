//! Tier-1 LLM analyst (plan §8.3).
//!
//! The analyst only *proposes*: it returns a strict-schema [`AnalystVerdict`] that
//! the governor then validates. Providers are behind an enum so the engine is
//! generic over real vs. mock, and tests never hit the network.

use serde_json::json;
use tracing::debug;

use turbine_core::config::AiConfig;
use turbine_core::error::{Result, TurbineError};

use crate::contract::{AnalystVerdict, FailureContext};

/// System prompt: constrains the model to our JSON schema and to *proposing*.
const SYSTEM_PROMPT: &str = "You are a Solana MEV bundle failure analyst. \
Given a failed Jito bundle's structured context, classify the root cause and \
propose corrected parameters. You PROPOSE ONLY — a deterministic governor applies \
guardrails. Never suggest exceeding caps. \
\
CLASSIFICATION (pick exactly one, snake_case): \
tip_too_low | blockhash_expired | auction_timeout | bundle_dropped | unknown. \
\
RULES: \
- If tip_below_floor is true OR params.tip_lamports < params.tip_floor_lamports → \
classification MUST be tip_too_low (never auction_timeout). \
- If blockhash_likely_stale or params.blockhash_forced_stale → \
classification MUST be blockhash_expired (never auction_timeout). \
- Do NOT use auction_timeout when tip_below_floor or blockhash_likely_stale is true. \
\
ADJUSTMENTS: \
- tip_too_low: set tip_bump_pct between 0.10 and 0.50 (max +50% per retry), \
rebuild=true, fresh_blockhash=false. Resulting tip must be >= params.tip_floor_lamports. \
- blockhash_expired: fresh_blockhash=true, rebuild=true, tip_bump_pct=null (do NOT bump tip). \
- compute_budget_exceeded: rebuild=true; omit cu_limit (the engine simulates real CUs on rebuild). \
- auction_timeout (only when tip is adequate and blockhash is fresh): may set \
tip_bump_pct up to 0.30 OR fresh_blockhash=true based on raw_reason / Jito logs. \
\
params.tip_floor_lamports is the percentile EMA floor; paid tip must never go below it. \
Use params.blockhash, blockhash_age_ms, blockhash_last_valid_height for blockhash age. \
Respond with STRICT JSON matching: \
{\"classification\":string,\"root_cause\":string,\"adjustments\":{\"tip_bump_pct\":number|null,\
\"slippage_bps\":integer|null,\"cu_limit\":integer|null,\"fresh_blockhash\":bool,\"rebuild\":bool},\
\"should_retry\":bool,\"confidence\":number}. No prose outside the JSON.";

/// A pluggable analyst backend.
pub enum Analyst {
    /// No LLM configured/available — Tier-1 will abort gracefully.
    Disabled,
    /// Deterministic canned verdict (tests, and a safe offline default).
    Mock(Box<AnalystVerdict>),
    /// OpenAI-compatible chat-completions endpoint with JSON response.
    OpenAi(OpenAiAnalyst),
}

impl Analyst {
    /// Build from config: OpenAI when enabled + API key present; otherwise a
    /// conservative Mock when enabled-without-key; Disabled when AI is off.
    pub fn from_config(cfg: &AiConfig) -> Self {
        if !cfg.enabled {
            return Analyst::Disabled;
        }
        match cfg.resolve_api_key() {
            Some(key) => Analyst::OpenAi(OpenAiAnalyst::new(cfg, key)),
            None => {
                tracing::warn!(
                    env = %cfg.api_key_env,
                    "AI enabled but no API key (set ai.api_key or export env); using conservative mock analyst"
                );
                Analyst::Mock(Box::new(AnalystVerdict {
                    classification: "unknown".into(),
                    root_cause: "no LLM available".into(),
                    adjustments: Default::default(),
                    should_retry: false,
                    confidence: 0.0,
                }))
            }
        }
    }

    pub async fn analyze(&self, ctx: &FailureContext) -> Result<AnalystVerdict> {
        match self {
            Analyst::Disabled => Err(TurbineError::Ai("analyst unavailable".into())),
            Analyst::Mock(v) => Ok((**v).clone()),
            Analyst::OpenAi(a) => a.analyze(ctx).await,
        }
    }
}

/// OpenAI-compatible analyst over HTTP.
pub struct OpenAiAnalyst {
    client: reqwest::Client,
    endpoint: String,
    model: String,
    api_key: String,
}

impl OpenAiAnalyst {
    pub fn new(cfg: &AiConfig, api_key: String) -> Self {
        // Default to OpenAI; any compatible base could be added to config later.
        let endpoint = "https://api.openai.com/v1/chat/completions".to_string();
        Self {
            client: reqwest::Client::new(),
            endpoint,
            model: cfg.model.clone(),
            api_key,
        }
    }

    async fn analyze(&self, ctx: &FailureContext) -> Result<AnalystVerdict> {
        let user = serde_json::to_string(ctx)
            .map_err(|e| TurbineError::Ai(format!("serialize context: {e}")))?;
        let body = json!({
            "model": self.model,
            "temperature": 0,
            "response_format": { "type": "json_object" },
            "messages": [
                { "role": "system", "content": SYSTEM_PROMPT },
                { "role": "user", "content": user },
            ],
        });

        let resp: serde_json::Value = self
            .client
            .post(&self.endpoint)
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| TurbineError::Ai(format!("llm request: {e}")))?
            .json()
            .await
            .map_err(|e| TurbineError::Ai(format!("llm decode: {e}")))?;

        let content = resp["choices"][0]["message"]["content"]
            .as_str()
            .ok_or_else(|| TurbineError::Ai("llm: missing message content".into()))?;
        debug!(%content, "llm verdict raw");

        let verdict: AnalystVerdict = serde_json::from_str(content)
            .map_err(|e| TurbineError::Ai(format!("llm: invalid verdict schema: {e}")))?;
        Ok(verdict)
    }
}

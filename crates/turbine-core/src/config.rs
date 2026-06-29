//! Single typed configuration for TURBINE, loaded from a TOML file with optional
//! environment-variable overrides for secrets/endpoints (plan §10).
//!
//! Secrets (Geyser x-token, wallet keypair path, AI API key) can be supplied via
//! env so they need not live in the committed config file.

use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;

use crate::error::{Result, TurbineError};

/// Serde helper: (de)serialize `Vec<Pubkey>` as base58 strings in config files.
/// `solana-pubkey`'s own serde impl encodes a raw 32-byte array, which is not what
/// we want in human-readable TOML, so we override it here.
mod pubkey_vec_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use solana_pubkey::Pubkey;
    use std::str::FromStr;

    pub fn serialize<S: Serializer>(v: &[Pubkey], s: S) -> Result<S::Ok, S::Error> {
        let strs: Vec<String> = v.iter().map(|p| p.to_string()).collect();
        strs.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Pubkey>, D::Error> {
        let strs = Vec::<String>::deserialize(d)?;
        strs.into_iter()
            .map(|s| Pubkey::from_str(&s).map_err(serde::de::Error::custom))
            .collect()
    }
}

/// Serde helper: (de)serialize `Option<Pubkey>` as an optional base58 string.
mod pubkey_opt_serde {
    use serde::{Deserialize, Deserializer, Serializer};
    use solana_pubkey::Pubkey;
    use std::str::FromStr;

    pub fn serialize<S: Serializer>(v: &Option<Pubkey>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(p) => s.serialize_some(&p.to_string()),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Pubkey>, D::Error> {
        match Option::<String>::deserialize(d)? {
            Some(s) => Ok(Some(Pubkey::from_str(&s).map_err(serde::de::Error::custom)?)),
            None => Ok(None),
        }
    }
}

/// Top-level configuration. Required sections must be present in the TOML;
/// tuning sections default to sane values when omitted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub geyser: GeyserConfig,
    pub rpc: RpcConfig,
    pub jito: JitoConfig,
    pub wallet: WalletConfig,
    #[serde(default)]
    pub targets: TargetsConfig,
    #[serde(default)]
    pub strategy: StrategyConfig,
    #[serde(default)]
    pub execution: ExecutionConfig,
    #[serde(default)]
    pub ai: AiConfig,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub ipc: IpcConfig,
}

/// Yellowstone/Geyser gRPC ingestion endpoint settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeyserConfig {
    pub endpoint: String,
    #[serde(default)]
    pub x_token: Option<String>,
    #[serde(default = "default_commitment")]
    pub commitment: String,
    #[serde(default = "default_max_decoding")]
    pub max_decoding_message_size_bytes: usize,
}
fn default_commitment() -> String {
    "processed".to_string()
}
fn default_ema_slow_half_life_ms() -> u64 {
    30_000
}
fn default_max_decoding() -> usize {
    64 * 1024 * 1024
}

/// Solana JSON-RPC used for warm blockhash + leader-schedule cross-reference.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcConfig {
    pub http_url: String,
    /// How often to refresh the warm cached blockhash (ms). Default 2s — well
    /// inside the ~60s blockhash validity window (plan §6).
    #[serde(default = "default_blockhash_refresh_ms")]
    pub blockhash_refresh_ms: u64,
}
fn default_blockhash_refresh_ms() -> u64 {
    2_000
}

/// Jito endpoints: gRPC block engine (primary submit + leader), JSON-RPC fallback,
/// and the tip percentile sources (REST seed + WS stream).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JitoConfig {
    pub block_engine_url: String,
    #[serde(default = "default_jito_json_rpc")]
    pub json_rpc_url: String,
    #[serde(default = "default_tip_floor")]
    pub tip_floor_url: String,
    #[serde(default = "default_tip_stream")]
    pub tip_stream_url: String,
    /// Optional Block Engine auth keypair. Default sends are keyless.
    #[serde(default)]
    pub block_engine_keypair_path: Option<PathBuf>,
    /// Optional JSON-RPC UUID auth.
    #[serde(default)]
    pub auth_uuid: Option<String>,
    /// Identities of Jito-enabled validators. Unioned with the set auto-fetched
    /// from `kobe_validators_url`; used to build the local epoch leader schedule.
    /// If both are empty the gate uses a synthetic next-leader fallback (plan §7.3).
    #[serde(default, with = "pubkey_vec_serde")]
    pub validator_identities: Vec<Pubkey>,
    /// Jito "kobe" endpoint listing all Jito-enabled validators. Fetched at boot
    /// and per epoch to build the leader schedule locally (no per-slot RPC).
    #[serde(default = "default_kobe_validators")]
    pub kobe_validators_url: String,
}
fn default_kobe_validators() -> String {
    "https://kobe.mainnet.jito.network/api/v1/validators".to_string()
}
fn default_jito_json_rpc() -> String {
    "https://mainnet.block-engine.jito.wtf/api/v1".to_string()
}
fn default_tip_floor() -> String {
    "https://bundles.jito.wtf/api/v1/bundles/tip_floor".to_string()
}
fn default_tip_stream() -> String {
    "wss://bundles.jito.wtf/api/v1/bundles/tip_stream".to_string()
}

/// Our trading wallet (signs trade + tip transactions).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletConfig {
    /// Path to the signing keypair (used from Phase 4 onward for signing).
    pub keypair_path: PathBuf,
    /// The wallet's public key, used to filter our own transactions on the Geyser
    /// stream for lifecycle tracking. If omitted, own-tx tracking is disabled.
    #[serde(default, with = "pubkey_opt_serde")]
    pub pubkey: Option<Pubkey>,
}

/// What to observe and what we ourselves write-lock.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TargetsConfig {
    /// Programs to filter the Geyser transaction stream on (e.g. Raydium, Pump.fun).
    #[serde(default, with = "pubkey_vec_serde")]
    pub programs: Vec<Pubkey>,
    /// Specific WRITABLE state accounts our strategy contends on (pools/vaults/curves).
    /// Contention is measured against these, not the read-only program IDs (plan §5.2).
    #[serde(default, with = "pubkey_vec_serde")]
    pub watched_accounts: Vec<Pubkey>,
}

/// Tuning knobs for the contention model, fee matrix, and submission gate.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StrategyConfig {
    /// Half-life for the fast contention EMA (ms) — current heat.
    pub ema_half_life_ms: u64,
    /// Half-life for the slow contention EMA (ms) — per-account baseline for z-score.
    #[serde(default = "default_ema_slow_half_life_ms")]
    pub ema_slow_half_life_ms: u64,
    /// Half-life for the tip-percentile EMA (ms).
    pub tip_ema_half_life_ms: u64,
    /// Z-score below which an account is "Quiet".
    pub quiet_z: f64,
    /// Z-score at/above which an account is "Hot".
    pub hot_z: f64,
    /// Lookahead gate window: fire when next Jito leader is in [gate_min, gate_max] slots.
    pub gate_min: u64,
    pub gate_max: u64,
    /// Fractional bump applied on top of the smoothed percentile tip (e.g. 0.10 = +10%).
    pub tip_bump_pct: f64,
    /// Extra fractional bump per unit of z-score *above* `hot_z`, scaling the tip with
    /// how contended/volatile the bottleneck account is (0 = disable, flat bump only).
    #[serde(default = "default_contention_bump_slope")]
    pub contention_bump_slope: f64,
    /// Cap on the contention-scaled bump so a z spike can't blow past the budget
    /// (e.g. 0.50 = at most +50% on top of the flat bump, before the hard clamp).
    #[serde(default = "default_max_contention_bump_pct")]
    pub max_contention_bump_pct: f64,
    /// Hard floor (Jito minimum is 1000 lamports) and ceiling (financial guardrail).
    pub min_tip_lamports: u64,
    pub max_tip_lamports: u64,
    /// Max trade transactions per bundle (Jito caps total at 5, leaving 1 for the tip tx).
    pub max_trades_per_bundle: usize,
}
impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            ema_half_life_ms: 2_000,
            ema_slow_half_life_ms: default_ema_slow_half_life_ms(),
            tip_ema_half_life_ms: 5_000,
            quiet_z: 0.5,
            hot_z: 2.0,
            gate_min: 2,
            gate_max: 3,
            tip_bump_pct: 0.10,
            contention_bump_slope: default_contention_bump_slope(),
            max_contention_bump_pct: default_max_contention_bump_pct(),
            min_tip_lamports: 1_000,
            max_tip_lamports: 10_000_000,
            max_trades_per_bundle: 4,
        }
    }
}
fn default_contention_bump_slope() -> f64 {
    0.10
}
fn default_max_contention_bump_pct() -> f64 {
    0.50
}

/// Execution-engine settings (plan §7).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    /// When true, the engine builds/prices/gates bundles but does NOT submit to
    /// Jito (no funds move). Default true — opt in to live submission explicitly.
    #[serde(default = "default_true")]
    pub dry_run: bool,
    /// Max time the lookahead gate waits for its window before giving up (ms).
    #[serde(default = "default_gate_max_wait_ms")]
    pub gate_max_wait_ms: u64,
    /// Reject submission if the cached blockhash is older than this (ms).
    #[serde(default = "default_blockhash_max_age_ms")]
    pub blockhash_max_age_ms: u64,
    /// How often the leader poller recomputes the next Jito leader slot (ms).
    #[serde(default = "default_leader_refresh_ms")]
    pub leader_refresh_ms: u64,
}
impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            dry_run: default_true(),
            gate_max_wait_ms: default_gate_max_wait_ms(),
            blockhash_max_age_ms: default_blockhash_max_age_ms(),
            leader_refresh_ms: default_leader_refresh_ms(),
        }
    }
}
fn default_true() -> bool {
    true
}
fn default_gate_max_wait_ms() -> u64 {
    2_000
}
fn default_blockhash_max_age_ms() -> u64 {
    45_000
}
fn default_leader_refresh_ms() -> u64 {
    5_000
}

/// AI failure-analyst + retry governor settings. The API key can be set inline
/// (`api_key`) or read at runtime from the env var named in `api_key_env`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AiConfig {
    pub enabled: bool,
    pub provider: String,
    pub model: String,
    /// Inline OpenAI key (optional). Prefer env var in shared environments.
    #[serde(default)]
    pub api_key: Option<String>,
    pub api_key_env: String,
    pub max_attempts: u8,
    pub max_slippage_bps: u32,
    pub spend_cap_lamports_per_min: u64,
    /// A submitted bundle that neither lands nor gets a result within this window
    /// is treated as a (timeout) failure and routed to the AI.
    pub retry_timeout_ms: u64,
}

impl AiConfig {
    /// Resolve the OpenAI API key: inline `api_key` wins, then `std::env::var(api_key_env)`.
    pub fn resolve_api_key(&self) -> Option<String> {
        if let Some(k) = &self.api_key {
            let k = k.trim();
            if !k.is_empty() {
                return Some(k.to_string());
            }
        }
        match std::env::var(&self.api_key_env) {
            Ok(k) if !k.trim().is_empty() => Some(k),
            _ => None,
        }
    }
}

impl Default for AiConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: "openai".to_string(),
            model: "gpt-4o-mini".to_string(),
            api_key: None,
            api_key_env: "TURBINE_AI_API_KEY".to_string(),
            max_attempts: 3,
            max_slippage_bps: 500,
            spend_cap_lamports_per_min: 50_000_000,
            retry_timeout_ms: 8_000,
        }
    }
}

/// Local web telemetry server bind address.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub web_bind: String,
}
impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            web_bind: "127.0.0.1:9000".to_string(),
        }
    }
}

/// Client/daemon IPC socket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpcConfig {
    pub socket_path: PathBuf,
}
impl Default for IpcConfig {
    fn default() -> Self {
        Self {
            socket_path: PathBuf::from("/tmp/turbine.sock"),
        }
    }
}

impl Config {
    /// Load, apply env overrides, and validate a config from a TOML path.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path).map_err(|e| {
            TurbineError::Config(format!("cannot read config '{}': {e}", path.display()))
        })?;
        Self::from_toml_str(&raw)
    }

    /// Parse from a TOML string, applying env overrides and validation. Useful
    /// for tests and embedding without touching the filesystem.
    pub fn from_toml_str(s: &str) -> Result<Self> {
        let mut cfg: Config = toml::from_str(s)?;
        cfg.apply_env_overrides();
        cfg.validate()?;
        Ok(cfg)
    }

    /// Override select fields from environment variables (secrets/endpoints).
    pub fn apply_env_overrides(&mut self) {
        if let Ok(v) = std::env::var("TURBINE_GEYSER_ENDPOINT") {
            self.geyser.endpoint = v;
        }
        if let Ok(v) = std::env::var("TURBINE_GEYSER_X_TOKEN") {
            self.geyser.x_token = Some(v);
        }
        if let Ok(v) = std::env::var("TURBINE_RPC_URL") {
            self.rpc.http_url = v;
        }
        if let Ok(v) = std::env::var("TURBINE_WALLET_KEYPAIR") {
            self.wallet.keypair_path = PathBuf::from(v);
        }
        if let Ok(v) = std::env::var("TURBINE_WALLET_PUBKEY") {
            if let Ok(pk) = solana_pubkey::Pubkey::from_str(&v) {
                self.wallet.pubkey = Some(pk);
            }
        }
        if let Ok(v) = std::env::var("TURBINE_JITO_BLOCK_ENGINE_URL") {
            self.jito.block_engine_url = v;
        }
        if let Ok(v) = std::env::var("TURBINE_AI_API_KEY") {
            if !v.trim().is_empty() {
                self.ai.api_key = Some(v);
            }
        }
    }

    /// Fail fast on inconsistent configuration before any network work begins.
    pub fn validate(&self) -> Result<()> {
        if self.geyser.endpoint.trim().is_empty() {
            return Err(TurbineError::Config("geyser.endpoint is empty".into()));
        }
        match self.geyser.commitment.as_str() {
            "processed" | "confirmed" | "finalized" => {}
            other => {
                return Err(TurbineError::Config(format!(
                    "invalid geyser.commitment '{other}' (expected processed|confirmed|finalized)"
                )))
            }
        }
        if self.rpc.http_url.trim().is_empty() {
            return Err(TurbineError::Config("rpc.http_url is empty".into()));
        }
        if self.jito.block_engine_url.trim().is_empty() {
            return Err(TurbineError::Config("jito.block_engine_url is empty".into()));
        }
        let s = &self.strategy;
        if s.gate_min > s.gate_max {
            return Err(TurbineError::Config(format!(
                "strategy.gate_min ({}) > gate_max ({})",
                s.gate_min, s.gate_max
            )));
        }
        if s.min_tip_lamports < 1_000 {
            return Err(TurbineError::Config(
                "strategy.min_tip_lamports must be >= 1000 (Jito minimum)".into(),
            ));
        }
        if s.min_tip_lamports > s.max_tip_lamports {
            return Err(TurbineError::Config(format!(
                "strategy.min_tip_lamports ({}) > max_tip_lamports ({})",
                s.min_tip_lamports, s.max_tip_lamports
            )));
        }
        if s.quiet_z > s.hot_z {
            return Err(TurbineError::Config(format!(
                "strategy.quiet_z ({}) > hot_z ({})",
                s.quiet_z, s.hot_z
            )));
        }
        if s.max_trades_per_bundle == 0 || s.max_trades_per_bundle > 4 {
            return Err(TurbineError::Config(
                "strategy.max_trades_per_bundle must be in 1..=4 (5th slot is the tip tx)".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
[geyser]
endpoint = "https://example-geyser:443"
commitment = "processed"

[rpc]
http_url = "https://api.mainnet-beta.solana.com"

[jito]
block_engine_url = "https://mainnet.block-engine.jito.wtf"

[wallet]
keypair_path = "/tmp/wallet.json"

[targets]
programs = ["675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"]
watched_accounts = []
"#;

    #[test]
    fn parses_and_validates_minimal_config() {
        let cfg: Config = toml::from_str(SAMPLE).expect("parse");
        cfg.validate().expect("validate");
        assert_eq!(cfg.strategy.gate_min, 2);
        assert_eq!(cfg.strategy.min_tip_lamports, 1_000);
        assert_eq!(cfg.targets.programs.len(), 1);
    }

    #[test]
    fn rejects_bad_commitment() {
        let bad = SAMPLE.replace("processed", "instant");
        let cfg: Config = toml::from_str(&bad).expect("parse");
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_tip_below_jito_minimum() {
        let cfg: Config = toml::from_str(SAMPLE).expect("parse");
        let mut cfg = cfg;
        cfg.strategy.min_tip_lamports = 500;
        assert!(cfg.validate().is_err());
    }
}

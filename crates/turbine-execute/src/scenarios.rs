//! Live mainnet test scenarios (Level 1).
//!
//! Minimal real bundles: a 1-lamport self-transfer trade leg plus a Jito tip tx.
//! No swap builder — validates the full pipeline under mainnet conditions at
//! negligible cost (tips + base fee only).

use rand::Rng;
use solana_pubkey::Pubkey;

use turbine_core::types::Percentile;

use crate::tx;
use crate::TradeIntent;

/// IPC scenario name: correct bundle, normal tip, fresh blockhash.
pub const HAPPY_PATH: &str = "happy-path";
/// IPC scenario name: memo-only trade leg (no lamport transfer) + P75 tip.
pub const HAPPY_PATH_MEMO: &str = "happy-path-memo";
/// IPC scenario name: self-transfer + tip in one tx + P75 (Jito basic_bundle style).
pub const HAPPY_PATH_SINGLE_TX: &str = "happy-path-single-tx";
/// IPC scenario name: deliberately failing bundle (real on-wire / on-chain failure).
pub const FAIL_PATH: &str = "fail-path";
/// IPC scenario name: stale blockhash + high tip (isolates blockhash failure).
pub const FAIL_PATH_BLOCKHASH: &str = "fail-path-blockhash";
/// IPC scenario name: sub-minimum tip + fresh blockhash (isolates auction reject).
pub const FAIL_PATH_TIP: &str = "fail-path-tip";
/// IPC scenario name: 10 random happy/fail actions in a background loop.
pub const AUTOPILOT: &str = "autopilot";

/// Which real failure mode was selected for a `fail-path` run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailMode {
    TipTooLow,
    StaleBlockhash,
}

impl FailMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::TipTooLow => "tip-too-low",
            Self::StaleBlockhash => "stale-blockhash",
        }
    }
}

/// A blockhash string guaranteed not to be recent — produces a real failure when
/// the bundle reaches a validator.
const STALE_BLOCKHASH: &str = "EhZ9TRfhwg274jnrEhNgrfugZaevSJo3zW1EgF6MJdTB";

/// Lamport amount for the self-transfer trade leg (negligible).
const SELF_TRANSFER_LAMPORTS: u64 = 1;

/// Memo payload for the happy-path-memo scenario.
const HAPPY_PATH_MEMO_DATA: &[u8] = b"turbine happy-path-memo";

/// Tip forced below the Jito minimum to trigger a real block-engine reject.
const FAIL_TIP_LAMPORTS: u64 = 1;

fn apply_happy_path_tip(intent: &mut TradeIntent) {
    intent.force_percentile = Some(Percentile::P75);
}

/// High tip on attempt 0 so blockhash is the only deliberate failure axis.
fn apply_fail_path_high_tip(intent: &mut TradeIntent) {
    intent.force_percentile = Some(Percentile::P75);
}

fn self_transfer_intent(label: impl Into<String>, payer: Pubkey) -> TradeIntent {
    let ix = tx::system_transfer(&payer, &payer, SELF_TRANSFER_LAMPORTS);
    TradeIntent {
        label: label.into(),
        trade_ix_groups: vec![vec![ix]],
        write_accounts: vec![payer],
        force_blockhash: None,
        force_tip_lamports: None,
        force_percentile: None,
        single_tx_bundle: false,
    }
}

/// Happy path: valid self-transfer + live P75 tip (EMA + bump) + warm blockhash.
pub fn happy_path(payer: Pubkey) -> TradeIntent {
    let mut intent = self_transfer_intent(HAPPY_PATH, payer);
    apply_happy_path_tip(&mut intent);
    intent
}

/// Happy path single-tx: trade + tip in one signed tx + live P75 tip (EMA + bump).
pub fn happy_path_single_tx(payer: Pubkey) -> TradeIntent {
    let mut intent = self_transfer_intent(HAPPY_PATH_SINGLE_TX, payer);
    apply_happy_path_tip(&mut intent);
    intent.single_tx_bundle = true;
    intent
}

fn memo_intent(label: impl Into<String>, payer: Pubkey) -> TradeIntent {
    let ix = tx::memo(&payer, HAPPY_PATH_MEMO_DATA);
    TradeIntent {
        label: label.into(),
        trade_ix_groups: vec![vec![ix]],
        write_accounts: vec![],
        force_blockhash: None,
        force_tip_lamports: None,
        force_percentile: None,
        single_tx_bundle: false,
    }
}

/// Happy path memo: memo trade leg only + live P75 tip (EMA + bump).
pub fn happy_path_memo(payer: Pubkey) -> TradeIntent {
    let mut intent = memo_intent(HAPPY_PATH_MEMO, payer);
    apply_happy_path_tip(&mut intent);
    intent
}

fn fail_path_with_mode(payer: Pubkey, mode: FailMode) -> (TradeIntent, FailMode) {
    let mut intent = self_transfer_intent(format!("{FAIL_PATH}:{}", mode.label()), payer);
    match mode {
        FailMode::TipTooLow => intent.force_tip_lamports = Some(FAIL_TIP_LAMPORTS),
        FailMode::StaleBlockhash => {
            apply_fail_path_high_tip(&mut intent);
            intent.force_blockhash = Some(STALE_BLOCKHASH.into());
        }
    }
    (intent, mode)
}

/// Stale blockhash + live P75 tip — only blockhash should cause failure.
pub fn fail_path_blockhash(payer: Pubkey) -> TradeIntent {
    let mut intent = self_transfer_intent(FAIL_PATH_BLOCKHASH, payer);
    apply_fail_path_high_tip(&mut intent);
    intent.force_blockhash = Some(STALE_BLOCKHASH.into());
    intent
}

/// Sub-minimum tip + warm blockhash — only auction/tip reject should cause failure.
pub fn fail_path_tip(payer: Pubkey) -> TradeIntent {
    let mut intent = self_transfer_intent(FAIL_PATH_TIP, payer);
    intent.force_tip_lamports = Some(FAIL_TIP_LAMPORTS);
    intent
}

/// Fail path with a caller-supplied RNG (`StdRng` in async tasks — `ThreadRng` is
/// not `Send` and must not be held across `.await`).
pub fn fail_path_with_rng(payer: Pubkey, rng: &mut impl Rng) -> (TradeIntent, FailMode) {
    let mode = if rng.gen_bool(0.5) {
        FailMode::TipTooLow
    } else {
        FailMode::StaleBlockhash
    };
    fail_path_with_mode(payer, mode)
}

/// Fail path: valid trade leg but randomly forces either a sub-minimum tip or a
/// stale blockhash on attempt 0 so a *real* failure is produced and routed to AI.
pub fn fail_path(payer: Pubkey) -> (TradeIntent, FailMode) {
    fail_path_with_rng(payer, &mut rand::thread_rng())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payer() -> Pubkey {
        Pubkey::new_from_array([9u8; 32])
    }

    #[test]
    fn happy_path_forces_p75_on_first_attempt() {
        let i = happy_path(payer());
        assert_eq!(i.label, HAPPY_PATH);
        assert_eq!(i.trade_ix_groups.len(), 1);
        assert_eq!(i.force_percentile, Some(Percentile::P75));
        assert!(i.force_tip_lamports.is_none());
        assert!(i.force_blockhash.is_none());
    }

    #[test]
    fn happy_path_memo_forces_p75_and_has_memo_trade() {
        let i = happy_path_memo(payer());
        assert_eq!(i.label, HAPPY_PATH_MEMO);
        assert_eq!(i.trade_ix_groups.len(), 1);
        assert_eq!(i.trade_ix_groups[0].len(), 1);
        assert_eq!(i.trade_ix_groups[0][0].program_id, tx::memo_program_id());
        assert_eq!(i.write_accounts, Vec::<Pubkey>::new());
        assert_eq!(i.force_percentile, Some(Percentile::P75));
        assert!(i.force_tip_lamports.is_none());
        assert!(i.force_blockhash.is_none());
    }

    #[test]
    fn happy_path_single_tx_forces_p75_and_single_tx_flag() {
        let i = happy_path_single_tx(payer());
        assert_eq!(i.label, HAPPY_PATH_SINGLE_TX);
        assert!(i.single_tx_bundle);
        assert_eq!(i.force_percentile, Some(Percentile::P75));
        assert!(i.force_tip_lamports.is_none());
    }

    #[test]
    fn fail_path_blockhash_forces_stale_hash_and_p75() {
        let i = fail_path_blockhash(payer());
        assert_eq!(i.label, FAIL_PATH_BLOCKHASH);
        assert_eq!(i.force_blockhash.as_deref(), Some(STALE_BLOCKHASH));
        assert_eq!(i.force_percentile, Some(Percentile::P75));
        assert!(i.force_tip_lamports.is_none());
    }

    #[test]
    fn fail_path_tip_forces_sub_min_tip_only() {
        let i = fail_path_tip(payer());
        assert_eq!(i.label, FAIL_PATH_TIP);
        assert_eq!(i.force_tip_lamports, Some(FAIL_TIP_LAMPORTS));
        assert!(i.force_blockhash.is_none());
        assert!(i.force_percentile.is_none());
    }

    #[test]
    fn fail_path_sets_exactly_one_force() {
        let (i, mode) = fail_path(payer());
        assert!(i.label.starts_with("fail-path:"));
        match mode {
            FailMode::TipTooLow => {
                assert_eq!(i.force_tip_lamports, Some(FAIL_TIP_LAMPORTS));
                assert!(i.force_blockhash.is_none());
            }
            FailMode::StaleBlockhash => {
                assert_eq!(i.force_blockhash.as_deref(), Some(STALE_BLOCKHASH));
                assert_eq!(i.force_percentile, Some(Percentile::P75));
                assert!(i.force_tip_lamports.is_none());
            }
        }
    }
}

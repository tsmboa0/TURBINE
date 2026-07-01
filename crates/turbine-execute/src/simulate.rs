//! RPC `simulateTransaction` for compute-unit limits on the AI retry path.
//!
//! The LLM may guess `cu_limit`; this module replaces that with measured
//! `unitsConsumed` (+ headroom) before a sanctioned resubmit is compiled.

use std::str::FromStr;

use base64::Engine;
use solana_pubkey::Pubkey;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::Instruction;
use solana_sdk::message::{v0, VersionedMessage};
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::VersionedTransaction;
use tracing::debug;

use turbine_core::error::{Result, TurbineError};

use crate::compute::limit_from_consumed;
use crate::compiler::{MAX_BUNDLE_TXS, MAX_TRADE_TXS};
use crate::tx::{set_compute_unit_limit, system_transfer};
use crate::TradeIntent;

/// High ceiling so simulation measures real work, not an under-set limit.
const SIMULATION_CU_CEILING: u32 = 1_400_000;

/// Simulate each transaction body in compile order and return per-tx CU limits.
pub async fn bundle_cu_limits(
    rpc_url: &str,
    payer: &Keypair,
    intent: &TradeIntent,
    tip_lamports: u64,
    tip_accounts: &[Pubkey],
    blockhash: &str,
    max_trades: usize,
) -> Result<Vec<u32>> {
    if tip_accounts.is_empty() {
        return Err(TurbineError::Execute("no Jito tip accounts for CU simulation".into()));
    }
    let bh = Hash::from_str(blockhash)
        .map_err(|e| TurbineError::Execute(format!("bad blockhash for simulation: {e}")))?;
    let payer_pk = payer.pubkey();
    let tip_account = tip_accounts[0];

    let mut limits = Vec::new();

    if intent.single_tx_bundle {
        let trade_ixs = intent
            .trade_ix_groups
            .iter()
            .find(|g| !g.is_empty())
            .cloned()
            .unwrap_or_default();
        let mut body = trade_ixs;
        body.push(system_transfer(&payer_pk, &tip_account, tip_lamports));
        let consumed = simulate_body(rpc_url, payer, &body, bh).await?;
        limits.push(limit_from_consumed(consumed));
        return Ok(limits);
    }

    let cap = max_trades.min(MAX_TRADE_TXS).min(MAX_BUNDLE_TXS - 1);
    for group in intent.trade_ix_groups.iter().take(cap) {
        if group.is_empty() {
            continue;
        }
        let consumed = simulate_body(rpc_url, payer, group, bh).await?;
        limits.push(limit_from_consumed(consumed));
    }

    let tip_ix = system_transfer(&payer_pk, &tip_account, tip_lamports);
    let consumed = simulate_body(rpc_url, payer, std::slice::from_ref(&tip_ix), bh).await?;
    limits.push(limit_from_consumed(consumed));

    Ok(limits)
}

async fn simulate_body(
    rpc_url: &str,
    payer: &Keypair,
    body_ixs: &[Instruction],
    blockhash: Hash,
) -> Result<u64> {
    let mut ixs: Vec<Instruction> = body_ixs.to_vec();
    ixs.insert(0, set_compute_unit_limit(SIMULATION_CU_CEILING));
    let msg = v0::Message::try_compile(&payer.pubkey(), &ixs, &[], blockhash)
        .map_err(|e| TurbineError::Execute(format!("simulation message compile: {e}")))?;
    let tx = VersionedTransaction::try_new(VersionedMessage::V0(msg), &[payer])
        .map_err(|e| TurbineError::Execute(format!("simulation sign: {e}")))?;
    simulate_transaction(rpc_url, &tx).await
}

async fn simulate_transaction(rpc_url: &str, tx: &VersionedTransaction) -> Result<u64> {
    let wire = bincode::serialize(tx)
        .map_err(|e| TurbineError::Execute(format!("simulation serialize: {e}")))?;
    let encoded = base64::engine::general_purpose::STANDARD.encode(wire);
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "simulateTransaction",
        "params": [
            encoded,
            {
                "encoding": "base64",
                "sigVerify": false,
                "replaceRecentBlockhash": true,
                "commitment": "confirmed"
            }
        ]
    });

    let resp: serde_json::Value = reqwest::Client::new()
        .post(rpc_url)
        .json(&body)
        .send()
        .await
        .map_err(|e| TurbineError::Execute(format!("simulateTransaction request: {e}")))?
        .json()
        .await
        .map_err(|e| TurbineError::Execute(format!("simulateTransaction decode: {e}")))?;

    if let Some(err) = resp.get("error") {
        return Err(TurbineError::Execute(format!("simulateTransaction rpc error: {err}")));
    }

    let value = &resp["result"]["value"];
    if let Some(err) = value.get("err").filter(|v| !v.is_null()) {
        return Err(TurbineError::Execute(format!("simulation failed: {err}")));
    }

    let consumed = value["unitsConsumed"]
        .as_u64()
        .ok_or_else(|| TurbineError::Execute("simulateTransaction: missing unitsConsumed".into()))?;
    debug!(units_consumed = consumed, "simulateTransaction ok");
    Ok(consumed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_units_consumed_from_fixture() {
        let value = serde_json::json!({
            "err": null,
            "unitsConsumed": 1234
        });
        let consumed = value["unitsConsumed"].as_u64().unwrap();
        assert_eq!(limit_from_consumed(consumed), 1388);
    }
}

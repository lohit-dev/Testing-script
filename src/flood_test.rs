//RUST_LOG=info cargo run --release --bin flood_test
//cargo run --release
use breez_sdk_spark::{
    BreezSdk, Config, ConnectRequest, GetInfoRequest, Network, PrepareSendPaymentRequest, ReceivePaymentMethod, ReceivePaymentRequest, Seed, SendPaymentRequest, connect, default_config
};
use color_eyre::eyre::{eyre, Result};
use ethers::{
    middleware::SignerMiddleware,
    prelude::*,
    providers::{Http, Provider},
    signers::{LocalWallet, Signer},
    types::{Address, Bytes, Eip1559TransactionRequest, U256},
};
use log::{error, info};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

fn env_var(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| eyre!("Missing required env var: {}", key))
}

// Flood test config
const FLOOD_TOTAL_ORDERS: u32 = 50;
const FLOOD_CREATE_BATCH_SIZE: u32 = 5; // create 10 at a time
const FLOOD_CREATE_DELAY_MS: u64 = 2000; // 600ms between creates (avoids 429)
const FLOOD_CREATE_BATCH_PAUSE_MS: u64 = 10000; // 3s pause between create batches
const FLOOD_FILL_CONCURRENCY: usize = 20; // fill 20 orders at once in the blast
const FLOOD_POLL_CONCURRENCY: usize = 50; // poll 50 orders at once
const FLOOD_ORDER_TIMEOUT_SECS: u64 = 600; // 10 min timeout per order

// ============================================================================
// API TYPES (same as your main.rs)
// ============================================================================

#[derive(Serialize, Deserialize, Debug)]
struct OrderDetails {
    asset: String,
    owner: String,
    amount: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct OrderRequest {
    source: OrderDetails,
    destination: OrderDetails,
    solver_id: Option<String>,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct CreateOrderResponse {
    status: String,
    result: CreateOrderResult,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct ErrorResponse {
    message: Option<String>,
    request_id: Option<String>,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug, Clone)]
struct CreateOrderResult {
    amount: String,
    order_id: String,
    to: String,
}

#[derive(Deserialize, Debug, Clone)]
struct EvmToSparkOrderResponse {
    status: String,
    result: EvmToSparkOrderResult,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug, Clone)]
struct EvmToSparkOrderResult {
    approval_transaction: Option<EvmTransaction>,
    initiate_transaction: EvmTransaction,
    order_id: String,
    typed_data: TypedDataInfo,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug, Clone)]
struct EvmTransaction {
    chain_id: u64,
    data: String,
    gas_limit: String,
    to: String,
    value: String,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug, Clone)]
struct TypedDataInfo {
    domain: TypedDataDomain,
    message: TypedDataMessage,
    #[serde(rename = "primaryType")]
    primary_type: String,
    types: Value,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug, Clone)]
struct TypedDataDomain {
    #[serde(rename = "chainId")]
    chain_id: String,
    name: String,
    #[serde(rename = "verifyingContract")]
    verifying_contract: String,
    version: String,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug, Clone)]
struct TypedDataMessage {
    amount: String,
    redeemer: String,
    #[serde(rename = "secretHash")]
    secret_hash: String,
    timelock: String,
}

#[derive(Debug, Default)]
struct SwapStatus {
    source_init: String,
    dest_init: String,
    source_redeem: String,
    dest_redeem: String,
}

// ============================================================================
// FLOOD REPORT
// ============================================================================

#[derive(Debug)]
struct FloodReport {
    test_name: String,
    created: u32,
    create_failed: u32,
    filled: u32,
    fill_failed: u32,
    completed: u32,
    poll_failed: u32,
    create_failures: Vec<(u32, String)>,
    fill_failures: Vec<(String, String)>,
    poll_failures: Vec<(String, String)>,
    swap_times: Vec<f64>,
    phase1_duration: f64,
    phase2_duration: f64,
    phase3_duration: f64,
    total_duration: f64,
}

impl FloodReport {
    fn new(name: &str) -> Self {
        Self {
            test_name: name.to_string(),
            created: 0,
            create_failed: 0,
            filled: 0,
            fill_failed: 0,
            completed: 0,
            poll_failed: 0,
            create_failures: vec![],
            fill_failures: vec![],
            poll_failures: vec![],
            swap_times: vec![],
            phase1_duration: 0.0,
            phase2_duration: 0.0,
            phase3_duration: 0.0,
            total_duration: 0.0,
        }
    }

    fn print(&self) {
        let avg = if self.swap_times.is_empty() {
            0.0
        } else {
            self.swap_times.iter().sum::<f64>() / self.swap_times.len() as f64
        };
        let min = self
            .swap_times
            .iter()
            .cloned()
            .fold(f64::INFINITY, f64::min);
        let max = self.swap_times.iter().cloned().fold(0.0f64, f64::max);
        let tps = if self.phase2_duration + self.phase3_duration > 0.0 {
            self.completed as f64 / (self.phase2_duration + self.phase3_duration)
        } else {
            0.0
        };

        info!("");
        info!("╔══════════════════════════════════════════════════════════════════╗");
        info!("║  FLOOD TEST REPORT: {:44}║", self.test_name);
        info!("╠══════════════════════════════════════════════════════════════════╣");
        info!("║                                                                  ║");
        info!("║  PHASE 1 — Slow Create (rate-limit safe)                         ║");
        info!(
            "║    Orders Created:    {:>6}                                      ║",
            self.created
        );
        info!(
            "║    Create Failed:     {:>6}                                      ║",
            self.create_failed
        );
        info!(
            "║    Phase Duration:    {:>6.1}s                                     ║",
            self.phase1_duration
        );
        info!("║                                                                  ║");
        info!("║  PHASE 2 — Flood Fill (all at once)                              ║");
        info!(
            "║    Orders Filled:     {:>6}                                      ║",
            self.filled
        );
        info!(
            "║    Fill Failed:       {:>6}                                      ║",
            self.fill_failed
        );
        info!(
            "║    Phase Duration:    {:>6.1}s                                     ║",
            self.phase2_duration
        );
        info!(
            "║    Fill Concurrency:  {:>6}                                      ║",
            FLOOD_FILL_CONCURRENCY
        );
        info!("║                                                                  ║");
        info!("║  PHASE 3 — Poll Completion                                       ║");
        info!(
            "║    Completed:         {:>6}                                      ║",
            self.completed
        );
        info!(
            "║    Poll Failed:       {:>6}                                      ║",
            self.poll_failed
        );
        info!(
            "║    Phase Duration:    {:>6.1}s                                     ║",
            self.phase3_duration
        );
        info!("║                                                                  ║");
        info!("║  TIMING                                                          ║");
        info!(
            "║    Total Wall Time:   {:>6.1}s                                     ║",
            self.total_duration
        );
        info!(
            "║    Throughput:        {:>6.2} swaps/sec                             ║",
            tps
        );
        info!(
            "║    Avg Swap Time:     {:>6.1}s                                     ║",
            avg
        );
        info!(
            "║    Min Swap Time:     {:>6.1}s                                     ║",
            if min.is_infinite() { 0.0 } else { min }
        );
        info!(
            "║    Max Swap Time:     {:>6.1}s                                     ║",
            max
        );
        info!("║                                                                  ║");
        info!("╚══════════════════════════════════════════════════════════════════╝");

        if !self.create_failures.is_empty() {
            info!("");
            info!("Create Failures:");
            for (idx, reason) in &self.create_failures {
                info!("  Order #{}: {}", idx, reason);
            }
        }
        if !self.fill_failures.is_empty() {
            info!("");
            info!("Fill Failures:");
            for (oid, reason) in &self.fill_failures {
                info!("  {}: {}", oid, reason);
            }
        }
        if !self.poll_failures.is_empty() {
            info!("");
            info!("Poll Failures:");
            for (oid, reason) in &self.poll_failures {
                info!("  {}: {}", oid, reason);
            }
        }
    }
}

// ============================================================================
// MAIN
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    color_eyre::install()?;
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    info!("╔══════════════════════════════════════════════════════════╗");
    info!("║       GARDEN SWAP FLOOD TEST — STAGING                  ║");
    info!("║       {}               ║", env_var("API_BASE_URL")?);
    info!("╚══════════════════════════════════════════════════════════╝");

    // Uncomment the flood test you want:
    flood_spark_to_evm().await?;
    //flood_evm_to_spark().await?;

    Ok(())
}

// ============================================================================
// FLOOD TEST: SPARK → EVM
//
// Phase 1: Slowly create 500 orders (respecting rate limits)
// Phase 2: Fill ALL 500 orders at once (blast send_funds concurrently)
// Phase 3: Poll all 500 orders for completion concurrently
// ============================================================================

#[allow(dead_code)]
async fn flood_spark_to_evm() -> Result<()> {
    info!("═══════════════════════════════════════════════════════════════");
    info!("  FLOOD TEST: Spark → EVM");
    info!("  {} orders × 5 sats", FLOOD_TOTAL_ORDERS);
    info!(
        "  Phase 1: Slow create ({} orders/batch, {}ms between, {}ms batch pause)",
        FLOOD_CREATE_BATCH_SIZE, FLOOD_CREATE_DELAY_MS, FLOOD_CREATE_BATCH_PAUSE_MS
    );
    info!(
        "  Phase 2: Flood fill (concurrency={})",
        FLOOD_FILL_CONCURRENCY
    );
    info!(
        "  Phase 3: Poll completion (concurrency={})",
        FLOOD_POLL_CONCURRENCY
    );
    info!("═══════════════════════════════════════════════════════════════");

    let total_start = Instant::now();
    let mut report = FloodReport::new("Spark → EVM (5 sats)");

    // Init SDK
    let sdk = Arc::new(init_sdk().await?);
    let my_address = get_address(&sdk).await?;
    info!("Spark Address: {}", my_address);

    let source_asset = "spark:btc";
    let dest_asset = "arbitrum_sepolia:wbtc";
    let amount_sats: u64 = 5;

    // ── PHASE 1: Slow Create ────────────────────────────────────
    info!("");
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    info!(
        "  PHASE 1: Creating {} orders slowly...",
        FLOOD_TOTAL_ORDERS
    );
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let phase1_start = Instant::now();
    let mut pending_orders: Vec<(u32, CreateOrderResult)> = Vec::new();
    let num_batches = (FLOOD_TOTAL_ORDERS + FLOOD_CREATE_BATCH_SIZE - 1) / FLOOD_CREATE_BATCH_SIZE;

    for batch in 0..num_batches {
        let b_start = batch * FLOOD_CREATE_BATCH_SIZE;
        let b_end = std::cmp::min(b_start + FLOOD_CREATE_BATCH_SIZE, FLOOD_TOTAL_ORDERS);

        info!(
            "  Creating batch {}/{} (orders {}-{})...",
            batch + 1,
            num_batches,
            b_start + 1,
            b_end
        );

        for idx in b_start..b_end {
            let order_num = idx + 1;

            if idx > b_start {
                sleep(Duration::from_millis(FLOOD_CREATE_DELAY_MS)).await;
            }

            match create_order_spark_to_evm(
                source_asset,
                &my_address,
                dest_asset,
                &env_var("EVM_DEST_ADDRESS")?,
                amount_sats.to_string(),
            )
            .await
            {
                Ok(order) => {
                    info!(
                        "  [{}] ✓ Created: {} (send {} to {})",
                        order_num, order.order_id, order.amount, order.to
                    );
                    pending_orders.push((order_num, order));
                    report.created += 1;
                }
                Err(e) => {
                    error!("  [{}] ✗ Create failed: {}", order_num, e);
                    report.create_failed += 1;
                    report.create_failures.push((order_num, e.to_string()));
                }
            }
        }

        // Pause between batches
        if batch + 1 < num_batches {
            info!(
                "  Batch {} done. {} created so far. Pausing {}ms...",
                batch + 1,
                report.created,
                FLOOD_CREATE_BATCH_PAUSE_MS
            );
            sleep(Duration::from_millis(FLOOD_CREATE_BATCH_PAUSE_MS)).await;
        }
    }

    report.phase1_duration = phase1_start.elapsed().as_secs_f64();
    info!("");
    info!(
        "  Phase 1 complete: {} orders created in {:.1}s ({} failed)",
        report.created, report.phase1_duration, report.create_failed
    );

    if pending_orders.is_empty() {
        error!("No orders created, aborting flood test");
        return Err(eyre!("No orders were created"));
    }

    // ── PHASE 2: Flood Fill ─────────────────────────────────────
    info!("");
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    info!(
        "  PHASE 2: Flood filling {} orders (concurrency={})",
        pending_orders.len(),
        FLOOD_FILL_CONCURRENCY
    );
    info!("  Sending all funds NOW...");
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let phase2_start = Instant::now();
    let semaphore = Arc::new(tokio::sync::Semaphore::new(FLOOD_FILL_CONCURRENCY));

    let mut fill_handles = Vec::new();

    for (order_num, order) in &pending_orders {
        let sdk_clone = sdk.clone();
        let sem_clone = semaphore.clone();
        let dest = order.to.clone();
        let amount_str = order.amount.clone();
        let oid = order.order_id.clone();
        let onum = *order_num;

        let handle = tokio::spawn(async move {
            let _permit = sem_clone.acquire().await.unwrap();
            let amount: u64 = match amount_str.parse() {
                Ok(v) => v,
                Err(e) => return (onum, oid, Err(format!("Bad amount: {}", e))),
            };
            match send_funds(&sdk_clone, &dest, amount).await {
                Ok(_) => (onum, oid, Ok(())),
                Err(e) => (onum, oid, Err(format!("Send failed: {}", e))),
            }
        });
        fill_handles.push(handle);
    }

    // Collect fill results
    let mut filled_orders: Vec<(u32, String, Instant)> = Vec::new();

    for handle in fill_handles {
        match handle.await {
            Ok((onum, oid, Ok(()))) => {
                info!("  [{}] ⚡ Filled: {}", onum, oid);
                report.filled += 1;
                filled_orders.push((onum, oid, Instant::now()));
            }
            Ok((onum, oid, Err(reason))) => {
                error!("  [{}] ✗ Fill failed: {} — {}", onum, oid, reason);
                report.fill_failed += 1;
                report.fill_failures.push((oid, reason));
            }
            Err(e) => {
                error!("  Task panicked: {}", e);
                report.fill_failed += 1;
                report
                    .fill_failures
                    .push(("unknown".into(), format!("Task panic: {}", e)));
            }
        }
    }

    report.phase2_duration = phase2_start.elapsed().as_secs_f64();
    info!("");
    info!(
        "  Phase 2 complete: {} filled in {:.1}s ({} failed)",
        report.filled, report.phase2_duration, report.fill_failed
    );

    if filled_orders.is_empty() {
        error!("No orders filled, aborting");
        report.total_duration = total_start.elapsed().as_secs_f64();
        report.print();
        return Err(eyre!("No orders were filled"));
    }

    // ── PHASE 3: Poll All ───────────────────────────────────────
    info!("");
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    info!(
        "  PHASE 3: Polling {} orders for completion (concurrency={})",
        filled_orders.len(),
        FLOOD_POLL_CONCURRENCY
    );
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let phase3_start = Instant::now();
    let poll_sem = Arc::new(tokio::sync::Semaphore::new(FLOOD_POLL_CONCURRENCY));

    let mut poll_handles = Vec::new();

    for (onum, oid, fill_time) in filled_orders {
        let sem_clone = poll_sem.clone();
        let order_id = oid.clone();

        let handle = tokio::spawn(async move {
            let _permit = sem_clone.acquire().await.unwrap();
            match poll_order_silent(&order_id, fill_time).await {
                Ok(secs) => (onum, order_id, Ok(secs)),
                Err(e) => (onum, order_id, Err(e.to_string())),
            }
        });
        poll_handles.push(handle);
    }

    for handle in poll_handles {
        match handle.await {
            Ok((onum, oid, Ok(secs))) => {
                info!("  [{}] ✅ {} completed in {:.1}s", onum, oid, secs);
                report.completed += 1;
                report.swap_times.push(secs);
            }
            Ok((onum, oid, Err(reason))) => {
                error!("  [{}] ❌ {} failed: {}", onum, oid, reason);
                report.poll_failed += 1;
                report.poll_failures.push((oid, reason));
            }
            Err(e) => {
                error!("  Poll task panicked: {}", e);
                report.poll_failed += 1;
                report
                    .poll_failures
                    .push(("unknown".into(), format!("Panic: {}", e)));
            }
        }
    }

    report.phase3_duration = phase3_start.elapsed().as_secs_f64();
    report.total_duration = total_start.elapsed().as_secs_f64();
    report.print();
    Ok(())
}

// ============================================================================
// FLOOD TEST: EVM (WBTC) → SPARK (100 -> 99 units)
//
// Phase 1: Slowly create 500 orders (respecting rate limits)
// Phase 2: Fill ALL 500 orders at once (blast EVM txs concurrently)
// Phase 3: Poll all 500 orders for completion concurrently
// ============================================================================

#[allow(dead_code)]
async fn flood_evm_to_spark() -> Result<()> {
    info!("═══════════════════════════════════════════════════════════════");
    info!("  FLOOD TEST: EVM (WBTC) → Spark");
    info!("  {} orders × 100 -> 99 units", FLOOD_TOTAL_ORDERS);
    info!(
        "  Phase 1: Slow create ({} orders/batch, {}ms between, {}ms batch pause)",
        FLOOD_CREATE_BATCH_SIZE, FLOOD_CREATE_DELAY_MS, FLOOD_CREATE_BATCH_PAUSE_MS
    );
    info!(
        "  Phase 2: Flood fill (concurrency={})",
        FLOOD_FILL_CONCURRENCY
    );
    info!(
        "  Phase 3: Poll completion (concurrency={})",
        FLOOD_POLL_CONCURRENCY
    );
    info!("═══════════════════════════════════════════════════════════════");

    let total_start = Instant::now();
    let mut report = FloodReport::new("EVM (WBTC) → Spark (100 -> 99 units)");

    // Init SDK
    let sdk = init_sdk().await?;
    let my_spark_address = get_address(&sdk).await?;
    info!("Spark Address: {}", my_spark_address);

    // Setup EVM wallet
    let provider = Provider::<Http>::try_from(env_var("ARBITRUM_SEPOLIA_RPC")?)?;
    let wallet: LocalWallet = env_var("EVM_PRIVATE_KEY")?
        .parse::<LocalWallet>()?
        .with_chain_id(421614u64);
    let evm_address = format!("{:?}", wallet.address());
    info!("EVM Address: {}", evm_address);
    let evm_client = Arc::new(SignerMiddleware::new(provider, wallet));

    let source_asset = "arbitrum_sepolia:wbtc";
    let dest_asset = "spark:btc";
    let source_amount = "100";
    let dest_amount = "99";

    // ── PHASE 1: Slow Create ────────────────────────────────────
    info!("");
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    info!(
        "  PHASE 1: Creating {} orders slowly...",
        FLOOD_TOTAL_ORDERS
    );
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let phase1_start = Instant::now();
    let mut pending_orders: Vec<(u32, EvmToSparkOrderResult)> = Vec::new();
    let num_batches = (FLOOD_TOTAL_ORDERS + FLOOD_CREATE_BATCH_SIZE - 1) / FLOOD_CREATE_BATCH_SIZE;

    for batch in 0..num_batches {
        let b_start = batch * FLOOD_CREATE_BATCH_SIZE;
        let b_end = std::cmp::min(b_start + FLOOD_CREATE_BATCH_SIZE, FLOOD_TOTAL_ORDERS);

        info!(
            "  Creating batch {}/{} (orders {}-{})...",
            batch + 1,
            num_batches,
            b_start + 1,
            b_end
        );

        for idx in b_start..b_end {
            let order_num = idx + 1;

            if idx > b_start {
                sleep(Duration::from_millis(FLOOD_CREATE_DELAY_MS)).await;
            }

            match create_order_evm_to_spark(
                source_asset,
                &evm_address,
                source_amount,
                dest_asset,
                &my_spark_address,
                dest_amount,
            )
            .await
            {
                Ok(order) => {
                    info!("  [{}] ✓ Created: {}", order_num, order.order_id);
                    pending_orders.push((order_num, order));
                    report.created += 1;
                }
                Err(e) => {
                    error!("  [{}] ✗ Create failed: {}", order_num, e);
                    report.create_failed += 1;
                    report.create_failures.push((order_num, e.to_string()));
                }
            }
        }

        if batch + 1 < num_batches {
            info!(
                "  Batch {} done. {} created so far. Pausing {}ms...",
                batch + 1,
                report.created,
                FLOOD_CREATE_BATCH_PAUSE_MS
            );
            sleep(Duration::from_millis(FLOOD_CREATE_BATCH_PAUSE_MS)).await;
        }
    }

    report.phase1_duration = phase1_start.elapsed().as_secs_f64();
    info!("");
    info!(
        "  Phase 1 complete: {} orders created in {:.1}s ({} failed)",
        report.created, report.phase1_duration, report.create_failed
    );

    if pending_orders.is_empty() {
        error!("No orders created, aborting flood test");
        return Err(eyre!("No orders were created"));
    }

    // ── PHASE 2: Flood Fill (approval + initiate blast) ─────────
    // NOTE: EVM txs are sequential per wallet (nonce ordering) so
    //       we process approvals first, then blast all initiates.
    info!("");
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    info!(
        "  PHASE 2: Processing approvals then flooding {} initiate txs",
        pending_orders.len()
    );
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let phase2_start = Instant::now();

    // Step 2a: Do approvals sequentially (they share nonce space)
    let mut approved_orders: Vec<(u32, EvmToSparkOrderResult)> = Vec::new();

    for (order_num, order) in &pending_orders {
        if let Some(approval_tx) = &order.approval_transaction {
            info!("  [{}] Approving {}...", order_num, order.order_id);
            match execute_evm_transaction(&evm_client, approval_tx).await {
                Ok(hash) => {
                    info!("  [{}] Approval tx: {:?}", order_num, hash);
                    sleep(Duration::from_secs(2)).await;
                    approved_orders.push((*order_num, order.clone()));
                }
                Err(e) => {
                    error!("  [{}] Approval failed: {}", order_num, e);
                    report.fill_failed += 1;
                    report
                        .fill_failures
                        .push((order.order_id.clone(), format!("Approval: {}", e)));
                }
            }
        } else {
            approved_orders.push((*order_num, order.clone()));
        }
    }

    info!("");
    info!(
        "  Approvals done. {} orders ready. Now blasting initiate txs...",
        approved_orders.len()
    );

    // Step 2b: Blast all initiate txs sequentially (nonce ordering)
    // but as fast as possible — no delays between them
    let mut filled_orders: Vec<(u32, String, Instant)> = Vec::new();

    for (order_num, order) in &approved_orders {
        info!("  [{}] Initiating {}...", order_num, order.order_id);
        let start = Instant::now();
        match execute_evm_transaction(&evm_client, &order.initiate_transaction).await {
            Ok(hash) => {
                info!("  [{}] ⚡ Initiate tx: {:?}", order_num, hash);
                report.filled += 1;
                filled_orders.push((*order_num, order.order_id.clone(), start));
            }
            Err(e) => {
                error!("  [{}] Initiate failed: {}", order_num, e);
                report.fill_failed += 1;
                report
                    .fill_failures
                    .push((order.order_id.clone(), format!("Initiate: {}", e)));
            }
        }
    }

    report.phase2_duration = phase2_start.elapsed().as_secs_f64();
    info!("");
    info!(
        "  Phase 2 complete: {} filled in {:.1}s ({} failed)",
        report.filled, report.phase2_duration, report.fill_failed
    );

    if filled_orders.is_empty() {
        error!("No orders filled, aborting");
        report.total_duration = total_start.elapsed().as_secs_f64();
        report.print();
        return Err(eyre!("No orders were filled"));
    }

    // ── PHASE 3: Poll All ───────────────────────────────────────
    info!("");
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    info!(
        "  PHASE 3: Polling {} orders (concurrency={})",
        filled_orders.len(),
        FLOOD_POLL_CONCURRENCY
    );
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let phase3_start = Instant::now();
    let poll_sem = Arc::new(tokio::sync::Semaphore::new(FLOOD_POLL_CONCURRENCY));

    let mut poll_handles = Vec::new();

    for (onum, oid, fill_time) in filled_orders {
        let sem_clone = poll_sem.clone();
        let order_id = oid.clone();

        let handle = tokio::spawn(async move {
            let _permit = sem_clone.acquire().await.unwrap();
            match poll_order_silent(&order_id, fill_time).await {
                Ok(secs) => (onum, order_id, Ok(secs)),
                Err(e) => (onum, order_id, Err(e.to_string())),
            }
        });
        poll_handles.push(handle);
    }

    for handle in poll_handles {
        match handle.await {
            Ok((onum, oid, Ok(secs))) => {
                info!("  [{}] ✅ {} completed in {:.1}s", onum, oid, secs);
                report.completed += 1;
                report.swap_times.push(secs);
            }
            Ok((onum, oid, Err(reason))) => {
                error!("  [{}] ❌ {} failed: {}", onum, oid, reason);
                report.poll_failed += 1;
                report.poll_failures.push((oid, reason));
            }
            Err(e) => {
                error!("  Poll task panicked: {}", e);
                report.poll_failed += 1;
                report
                    .poll_failures
                    .push(("unknown".into(), format!("Panic: {}", e)));
            }
        }
    }

    report.phase3_duration = phase3_start.elapsed().as_secs_f64();
    report.total_duration = total_start.elapsed().as_secs_f64();
    report.print();
    Ok(())
}

// ============================================================================
// SDK HELPERS
// ============================================================================

async fn init_sdk() -> Result<BreezSdk> {
    info!("Initializing SDK...");
    let storage_dir = env_var("STORAGE_DIR")?;
    let breez_api_key = env_var("BREEZ_API_KEY")?;
    std::fs::create_dir_all(&storage_dir)?;
    let mut config: Config = default_config(Network::Mainnet);
    config.api_key = Some(breez_api_key);
    let seed = Seed::Mnemonic {
        mnemonic: env_var("TEST_MNEMONIC")?,
        passphrase: None,
    };
    let sdk = connect(ConnectRequest {
        config,
        seed,
        storage_dir,
    })
    .await?;
    info!("SDK initialized");
    Ok(sdk)
}

async fn get_address(sdk: &BreezSdk) -> Result<String> {
    let info = sdk
        .get_info(GetInfoRequest {
            ensure_synced: Some(false),
        })
        .await?;
    info!("Balance: {} sats", info.balance_sats);
    let resp = sdk
        .receive_payment(ReceivePaymentRequest {
            payment_method: ReceivePaymentMethod::SparkAddress,
        })
        .await?;
    Ok(resp.payment_request)
}

async fn send_funds(sdk: &BreezSdk, dest: &str, amount: u64) -> Result<()> {
    let prep = sdk
        .prepare_send_payment(PrepareSendPaymentRequest {
            payment_request: dest.to_string(),
            amount: Some(amount as u128),
            token_identifier: None,
            conversion_options: None,
            fee_policy: None,
        })
        .await?;
    sdk.send_payment(SendPaymentRequest {
        prepare_response: prep,
        options: None,
        idempotency_key: None,
    })
    .await?;
    Ok(())
}

// ============================================================================
// API HELPERS
// ============================================================================

async fn make_order_request_with_retry(
    client: &Client,
    url: &str,
    req: &OrderRequest,
    max_retries: u32,
) -> Result<(reqwest::StatusCode, String)> {
    let mut retry_count = 0;
    let base_delay = Duration::from_secs(2);
    let garden_app_id = env_var("GARDEN_APP_ID")?;
    loop {
        let res = client
            .post(url)
            .header("garden-app-id", &garden_app_id)
            .header("Content-Type", "application/json")
            .json(req)
            .send()
            .await?;
        let status = res.status();
        let res_text = res.text().await?;

        // Retry on 429 (rate limit) OR 5xx (server error)
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
            if retry_count < max_retries {
                retry_count += 1;
                let delay = base_delay * (1 << (retry_count - 1)); // 2s, 4s, 8s, 16s, 32s
                info!(
                    "  Server returned {} — retry {}/{} in {:.0}s...",
                    status,
                    retry_count,
                    max_retries,
                    delay.as_secs_f64()
                );
                sleep(delay).await;
                continue;
            } else {
                return Err(eyre!(
                    "API {} after {} retries: {}",
                    status,
                    max_retries,
                    res_text
                ));
            }
        }
        return Ok((status, res_text));
    }
}

async fn create_order_spark_to_evm(
    source_asset: &str,
    source_owner: &str,
    dest_asset: &str,
    dest_owner: &str,
    amount: String,
) -> Result<CreateOrderResult> {
    let url = format!("{}/v2/orders", env_var("API_BASE_URL")?);
    let client = Client::new();
    let req = OrderRequest {
        source: OrderDetails {
            asset: source_asset.to_string(),
            owner: source_owner.to_string(),
            amount: amount.clone(),
        },
        destination: OrderDetails {
            asset: dest_asset.to_string(),
            owner: dest_owner.to_string(),
            amount: "499".to_string(),
        },
        solver_id: Some("staging-2".to_string()),
    };
    let (status, res_text) = make_order_request_with_retry(&client, &url, &req, 5).await?;
    if !status.is_success() {
        if let Ok(e) = serde_json::from_str::<ErrorResponse>(&res_text) {
            return Err(eyre!("API {}: {}", status, e.message.unwrap_or_default()));
        }
        return Err(eyre!("API {}: {}", status, res_text));
    }
    if let Ok(e) = serde_json::from_str::<ErrorResponse>(&res_text) {
        if e.message.is_some() {
            return Err(eyre!("API error: {}", e.message.unwrap()));
        }
    }
    let res_json: CreateOrderResponse = serde_json::from_str(&res_text)
        .map_err(|e| eyre!("Parse error: {}. Body: {}", e, res_text))?;
    if res_json.status != "Ok" {
        return Err(eyre!("Order status: {}", res_json.status));
    }
    Ok(res_json.result)
}

async fn create_order_evm_to_spark(
    source_asset: &str,
    source_owner: &str,
    source_amount: &str,
    dest_asset: &str,
    dest_owner: &str,
    dest_amount: &str,
) -> Result<EvmToSparkOrderResult> {
    let url = format!("{}/v2/orders", env_var("API_BASE_URL")?);
    let client = Client::new();
    let req = OrderRequest {
        source: OrderDetails {
            asset: source_asset.to_string(),
            owner: source_owner.to_string(),
            amount: source_amount.to_string(),
        },
        destination: OrderDetails {
            asset: dest_asset.to_string(),
            owner: dest_owner.to_string(),
            amount: dest_amount.to_string(),
        },
        solver_id: Some("staging-2".to_string()),
    };
    let (status, res_text) = make_order_request_with_retry(&client, &url, &req, 5).await?;
    if !status.is_success() {
        if let Ok(e) = serde_json::from_str::<ErrorResponse>(&res_text) {
            return Err(eyre!("API {}: {}", status, e.message.unwrap_or_default()));
        }
        return Err(eyre!("API {}: {}", status, res_text));
    }
    if let Ok(e) = serde_json::from_str::<ErrorResponse>(&res_text) {
        if e.message.is_some() {
            return Err(eyre!("API error: {}", e.message.unwrap()));
        }
    }
    let res_json: EvmToSparkOrderResponse = serde_json::from_str(&res_text)
        .map_err(|e| eyre!("Parse error: {}. Body: {}", e, res_text))?;
    if res_json.status != "Ok" {
        return Err(eyre!("Order status: {}", res_json.status));
    }
    Ok(res_json.result)
}

// ============================================================================
// EVM TRANSACTION
// ============================================================================

async fn execute_evm_transaction(
    client: &Arc<SignerMiddleware<Provider<Http>, LocalWallet>>,
    tx_data: &EvmTransaction,
) -> Result<TxHash> {
    let to_address: Address = tx_data.to.parse()?;
    let value =
        U256::from_str_radix(tx_data.value.trim_start_matches("0x"), 16).unwrap_or(U256::zero());
    let gas_limit = U256::from_str_radix(tx_data.gas_limit.trim_start_matches("0x"), 16)?;
    let data: Bytes = tx_data.data.parse()?;

    let (max_fee, priority_fee) = match client.inner().estimate_eip1559_fees(None).await {
        Ok(fees) => fees,
        Err(_) => (U256::from(100_000_000_000u64), U256::from(1_000_000_000u64)),
    };

    let tx = Eip1559TransactionRequest::new()
        .to(to_address)
        .value(value)
        .data(data)
        .gas(gas_limit)
        .max_fee_per_gas(max_fee)
        .max_priority_fee_per_gas(priority_fee);

    let pending = client.send_transaction(tx, None).await?;
    let hash = pending.tx_hash();
    let receipt = pending
        .await?
        .ok_or_else(|| eyre!("No receipt for {:?}", hash))?;

    if receipt.status == Some(1.into()) {
        Ok(hash)
    } else {
        Err(eyre!(
            "Tx reverted: {:?} block={:?} gas={:?}",
            hash,
            receipt.block_number,
            receipt.gas_used
        ))
    }
}

// ============================================================================
// ORDER POLLING
// ============================================================================

async fn check_order_status(order_id: &str) -> Result<Option<SwapStatus>> {
    let url = format!("{}/v2/orders/{}", env_var("API_BASE_URL")?, order_id);
    let client = Client::new();
    let mut retry_count = 0u32;
    let max_retries = 3u32;
    let base_delay = Duration::from_secs(1);

    loop {
        let response = client
            .get(&url)
            .header("garden-app-id", env_var("GARDEN_APP_ID")?)
            .send()
            .await;

        let res = match response {
            Ok(r) => r,
            Err(e) => {
                if retry_count < max_retries {
                    retry_count += 1;
                    sleep(base_delay * (1 << (retry_count - 1))).await;
                    continue;
                }
                return Err(eyre!("Network error after retries: {}", e));
            }
        };

        let status = res.status();
        let text = res.text().await?;

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            if retry_count < max_retries {
                retry_count += 1;
                sleep(base_delay * (1 << (retry_count - 1))).await;
                continue;
            }
            return Err(eyre!("Rate limited on status check"));
        }

        if !status.is_success() {
            if retry_count < max_retries && status.is_server_error() {
                retry_count += 1;
                sleep(base_delay * (1 << (retry_count - 1))).await;
                continue;
            }
            return Err(eyre!("API {}: {}", status, text));
        }

        let json: Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(e) => {
                if retry_count < max_retries {
                    retry_count += 1;
                    sleep(base_delay * (1 << (retry_count - 1))).await;
                    continue;
                }
                return Err(eyre!("JSON parse: {}", e));
            }
        };

        if let Some(result) = json.get("result") {
            return Ok(Some(SwapStatus {
                source_init: result["source_swap"]["initiate_tx_hash"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
                dest_init: result["destination_swap"]["initiate_tx_hash"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
                source_redeem: result["source_swap"]["redeem_tx_hash"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
                dest_redeem: result["destination_swap"]["redeem_tx_hash"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
            }));
        }

        return Ok(None);
    }
}

async fn poll_order_silent(order_id: &str, start: Instant) -> Result<f64> {
    let mut consecutive_errors = 0u32;

    loop {
        if start.elapsed() > Duration::from_secs(FLOOD_ORDER_TIMEOUT_SECS) {
            return Err(eyre!("Timeout after {}s", FLOOD_ORDER_TIMEOUT_SECS));
        }

        match check_order_status(order_id).await {
            Ok(Some(s)) => {
                consecutive_errors = 0;
                if !s.source_init.is_empty() && !s.dest_redeem.is_empty() {
                    return Ok(start.elapsed().as_secs_f64());
                }
            }
            Ok(None) => {
                consecutive_errors = 0;
            }
            Err(_) => {
                consecutive_errors += 1;
                if consecutive_errors >= 5 {
                    return Err(eyre!("Too many consecutive poll errors"));
                }
            }
        }

        sleep(Duration::from_secs(5)).await;
    }
}

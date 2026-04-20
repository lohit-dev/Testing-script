//cargo run --release --bin testing_script
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
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::task::JoinSet;
use tokio::time::sleep;

fn env_var(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| eyre!("Missing required env var: {}", key))
}

// ============================================================================
// DIRECT EVM INITIATE TEST (MANUAL DATA)
// ============================================================================

#[cfg(test)]
mod manual_evm_initiate_tests {
    use super::*;

    #[tokio::test]
    async fn test_manual_evm_initiate_on_arbitrum() -> Result<()> {
        dotenvy::dotenv().ok();
        // Init EVM wallet on Arbitrum Sepolia
        let provider = Provider::<Http>::try_from(env_var("ARBITRUM_SEPOLIA_RPC")?)?;
        let wallet: LocalWallet = env_var("EVM_PRIVATE_KEY")?
            .parse::<LocalWallet>()?
            .with_chain_id(421614u64);
        let client = Arc::new(SignerMiddleware::new(provider, wallet));

        // Manual initiate tx data returned by the Garden API
        let tx = EvmTransaction {
            chain_id: 421_614,
            data: "0x97ffc7ae000000000000000000000000155072bdfb5735b91920255974ffa4af4882d9ed0000000000000000000000000000000000000000000000000000000000069780000000000000000000000000000000000000000000000000000000000000038e88189359b6ec3d25f3c4629194890c12ff0a0a26529d4ba349f620d37e166319".to_string(),
            gas_limit: "0x493e0".to_string(),
            to: "0x9648b9d01242f537301b98ec0bf8b6854cdb97e6".to_string(),
            value: "0x0".to_string(),
        };

        // Execute the initiate transaction on Arbitrum
        let _tx_hash = execute_evm_transaction(&client, &tx).await?;
        Ok(())
    }
}

#[cfg(test)]
mod spark_address_verification_tests {
    use super::*;

    #[tokio::test]
    #[ignore = "Requires network + valid Breez credentials"]
    async fn verify_expected_spark_address_from_mnemonic() -> Result<()> {
        dotenvy::dotenv().ok();
        let expected = "spark1pgssxklgsn4sytzymxah4jf3r8rzqgsxmsv63nrs4jj0w0ed634kjq32p6hxju";

        let sdk = init_sdk().await?;
        let actual = get_address(&sdk).await?;
        assert_eq!(
            actual, expected,
            "Spark address mismatch. expected={}, actual={}",
            expected, actual
        );
        Ok(())
    }
}

// ============================================================================
// API TYPES
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

#[derive(Debug, Clone)]
struct OrderTimelineReport {
    order_id: String,
    create_attempt_at_secs: f64,
    create_success: bool,
    source_init_after_secs: Option<f64>,
    destination_init_after_secs: Option<f64>,
    destination_redeem_after_secs: Option<f64>,
    source_redeem_after_secs: Option<f64>,
    final_status: String,
    note: String,
    error: String,
}

// ============================================================================
// BATCH REPORT
// ============================================================================

#[derive(Debug, Default)]
struct BatchTestReport {
    total_orders: u32,
    successful_orders: u32,
    failed_orders: u32,
    failed_order_ids: Vec<String>,
    failed_reasons: Vec<String>,
    swap_times: Vec<f64>,
    start_time: Option<Instant>,
    end_time: Option<Instant>,
}

impl BatchTestReport {
    fn new() -> Self {
        Self::default()
    }

    fn start(&mut self) {
        self.start_time = Some(Instant::now());
    }

    fn finish(&mut self) {
        self.end_time = Some(Instant::now());
    }

    fn record_success(&mut self, swap_duration_secs: f64) {
        self.total_orders += 1;
        self.successful_orders += 1;
        self.swap_times.push(swap_duration_secs);
    }

    fn record_failure(&mut self, order_id: String, reason: String) {
        self.total_orders += 1;
        self.failed_orders += 1;
        self.failed_order_ids.push(order_id);
        self.failed_reasons.push(reason);
    }

    fn print_report(&self, test_name: &str) {
        let avg_time = if self.swap_times.is_empty() {
            0.0
        } else {
            self.swap_times.iter().sum::<f64>() / self.swap_times.len() as f64
        };
        let min_time = self
            .swap_times
            .iter()
            .cloned()
            .fold(f64::INFINITY, f64::min);
        let max_time = self.swap_times.iter().cloned().fold(0.0f64, f64::max);

        info!("╔════════════════════════════════════════════════════════════════╗");
        info!("║  BATCH TEST REPORT: {:42}║", test_name);
        info!("╠════════════════════════════════════════════════════════════════╣");
        info!(
            "║  Total Orders:    {:>6}                                      ║",
            self.total_orders
        );
        info!(
            "║  Successful:      {:>6} ({:>5.1}%)                            ║",
            self.successful_orders,
            if self.total_orders > 0 {
                self.successful_orders as f64 / self.total_orders as f64 * 100.0
            } else {
                0.0
            }
        );
        info!(
            "║  Failed:          {:>6} ({:>5.1}%)                            ║",
            self.failed_orders,
            if self.total_orders > 0 {
                self.failed_orders as f64 / self.total_orders as f64 * 100.0
            } else {
                0.0
            }
        );
        if let (Some(start), Some(end)) = (self.start_time, self.end_time) {
            let wall = end.duration_since(start).as_secs_f64();
            let tps = if wall > 0.0 {
                self.successful_orders as f64 / wall
            } else {
                0.0
            };
            info!(
                "║  Wall Time:       {:>6.1} seconds                             ║",
                wall
            );
            info!(
                "║  Throughput:      {:>6.2} orders/sec                           ║",
                tps
            );
        }
        info!(
            "║  Avg Swap Time:   {:>6.1} seconds                             ║",
            avg_time
        );
        info!(
            "║  Min Swap Time:   {:>6.1} seconds                             ║",
            if min_time.is_infinite() {
                0.0
            } else {
                min_time
            }
        );
        info!(
            "║  Max Swap Time:   {:>6.1} seconds                             ║",
            max_time
        );
        info!("╚════════════════════════════════════════════════════════════════╝");

        if !self.failed_order_ids.is_empty() {
            info!("");
            info!("Failed Orders Details:");
            for (i, (order_id, reason)) in self
                .failed_order_ids
                .iter()
                .zip(self.failed_reasons.iter())
                .enumerate()
            {
                info!("  {}. Order ID: {} - Reason: {}", i + 1, order_id, reason);
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
    .filter_level(log::LevelFilter::Off)              // default: nothing logs
    .filter_module("testing_script", log::LevelFilter::Info) // only your crate logs
    .init();

    info!("╔══════════════════════════════════════════════════════════╗");
    info!("║       GARDEN SWAP TEST SUITE — STAGING                  ║");
    info!("║       {}               ║", env_var("API_BASE_URL")?);
    info!("╚══════════════════════════════════════════════════════════╝");

    // Select scenario via env var TEST_MODE to avoid accidental runs.
    // Supported values:
    // - baseline
    // - single_spark_to_evm
    // - single_evm_to_spark
    // - batch_spark_to_evm
    // - batch_evm_to_spark
    let test_mode = std::env::var("TEST_MODE").unwrap_or_else(|_| "baseline".to_string());
    match test_mode.as_str() {
        "baseline" => run_spark_to_evm_minute_baseline_for_a_day().await?,
        "single_spark_to_evm" => test_single_spark_to_evm().await?,
        "single_evm_to_spark" => test_single_evm_to_spark().await?,
        "batch_spark_to_evm" => {
            let _ = batch_test_spark_to_evm(100, 3).await?;
        }
        "batch_evm_to_spark" => {
            let _ = batch_test_evm_to_spark(100, 3).await?;
        }
        other => {
            return Err(eyre!(
                "Unknown TEST_MODE='{}'. Use one of: baseline, single_spark_to_evm, single_evm_to_spark, batch_spark_to_evm, batch_evm_to_spark",
                other
            ));
        }
    }

    Ok(())
}

// ============================================================================
// BASELINE SOAK TEST: CREATE 2 ORDERS / MIN FOR 24H + PER-ORDER TIMELINE REPORT
// ============================================================================

async fn run_spark_to_evm_minute_baseline_for_a_day() -> Result<()> {
    const CREATION_INTERVAL: Duration = Duration::from_secs(60);
    const RUN_DURATION: Duration = Duration::from_secs(24 * 60 * 60);
    const ORDER_POLL_TIMEOUT: Duration = Duration::from_secs(60 * 60);
    const POLL_EVERY: Duration = Duration::from_secs(5);

    info!("=== Baseline Soak: 1 Spark->EVM + 1 EVM->Spark per minute for 24h ===");
    info!(
        "Per-order timeout: {:.0} min | Poll interval: {}s",
        ORDER_POLL_TIMEOUT.as_secs_f64() / 60.0,
        POLL_EVERY.as_secs()
    );

    let sdk = init_sdk().await?;
    let my_address = get_address(&sdk).await?;
    let source_asset = "spark:btc";
    let dest_asset = "arbitrum_sepolia:wbtc";
    let source_amount_sats: u64 = 5;
    let destination_amount = "4".to_string();
    let evm_dest_address = env_var("EVM_DEST_ADDRESS")?;

    let provider = Provider::<Http>::try_from(env_var("ARBITRUM_SEPOLIA_RPC")?)?;
    let wallet: LocalWallet = env_var("EVM_PRIVATE_KEY")?
        .parse::<LocalWallet>()?
        .with_chain_id(421614u64);
    let evm_source_address = format!("{:?}", wallet.address());
    let evm_client = Arc::new(SignerMiddleware::new(provider, wallet));

    let evm_to_spark_source_asset = "arbitrum_sepolia:wbtc";
    let evm_to_spark_dest_asset = "spark:btc";
    let evm_to_spark_source_amount = "10";
    let evm_to_spark_dest_amount = "9";

    let (live_csv_path, mut live_csv_file) = init_live_report_csv()?;
    let run_start = Instant::now();
    let mut minute_idx: u64 = 0;
    let mut monitor_set: JoinSet<OrderTimelineReport> = JoinSet::new();
    let mut report_rows: Vec<OrderTimelineReport> = Vec::new();

    while run_start.elapsed() < RUN_DURATION {
        minute_idx += 1;
        let minute_start = Instant::now();
        info!("");
        info!(
            "----- Minute {} (elapsed {:.1} min) -----",
            minute_idx,
            run_start.elapsed().as_secs_f64() / 60.0
        );

        let spark_result = create_spark_to_evm_minute_order(
            minute_idx,
            run_start.elapsed().as_secs_f64(),
            source_asset,
            &my_address,
            dest_asset,
            &evm_dest_address,
            source_amount_sats,
            &destination_amount,
            &sdk,
        )
        .await;

        // Avoid burst traffic across providers/services.
        sleep(Duration::from_secs(3)).await;

        let evm_result = create_evm_to_spark_minute_order(
            minute_idx,
            run_start.elapsed().as_secs_f64(),
            evm_to_spark_source_asset,
            &evm_source_address,
            evm_to_spark_source_amount,
            evm_to_spark_dest_asset,
            &my_address,
            evm_to_spark_dest_amount,
            &evm_client,
        )
        .await;

        handle_minute_order_result(
            spark_result,
            &mut report_rows,
            &mut live_csv_file,
            &mut monitor_set,
            ORDER_POLL_TIMEOUT,
            POLL_EVERY,
            &live_csv_path,
        )?;
        handle_minute_order_result(
            evm_result,
            &mut report_rows,
            &mut live_csv_file,
            &mut monitor_set,
            ORDER_POLL_TIMEOUT,
            POLL_EVERY,
            &live_csv_path,
        )?;
        drain_ready_monitor_rows(
            &mut monitor_set,
            &mut report_rows,
            &mut live_csv_file,
            &live_csv_path,
        )?;

        // Baseline requirement: always attempt 2 new orders every minute.
        let elapsed_this_minute = minute_start.elapsed();
        if elapsed_this_minute < CREATION_INTERVAL {
            sleep(CREATION_INTERVAL - elapsed_this_minute).await;
        } else {
            info!(
                "Minute {} creation work took {:.1}s (> 60s), continuing immediately",
                minute_idx,
                elapsed_this_minute.as_secs_f64()
            );
        }
    }

    info!("");
    info!(
        "24h creation window finished. Waiting for {} order pollers...",
        monitor_set.len()
    );

    while let Some(result) = monitor_set.join_next().await {
        match result {
            Ok(row) => {
                append_and_persist_row(&mut report_rows, &mut live_csv_file, row)?;
            }
            Err(e) => append_and_persist_row(
                &mut report_rows,
                &mut live_csv_file,
                build_pending_row(
                    "poll_task_join_error".to_string(),
                    run_start.elapsed().as_secs_f64(),
                    true,
                    format!("poll task failed to join: {}", e),
                    format!("{:#}", e),
                ),
            )?,
        }
    }

    print_timeline_report(&report_rows, run_start.elapsed().as_secs_f64());
    info!("Live CSV report: {}", live_csv_path);
    write_timeline_report_txt(&report_rows)?;
    info!("=== Baseline Soak Complete ===");
    Ok(())
}

fn build_pending_row(
    order_id: String,
    create_attempt_at_secs: f64,
    create_success: bool,
    note: String,
    error: String,
) -> OrderTimelineReport {
    OrderTimelineReport {
        order_id,
        create_attempt_at_secs,
        create_success,
        source_init_after_secs: None,
        destination_init_after_secs: None,
        destination_redeem_after_secs: None,
        source_redeem_after_secs: None,
        final_status: "pending".to_string(),
        note,
        error,
    }
}

fn spawn_order_monitor(
    set: &mut JoinSet<OrderTimelineReport>,
    order_id: String,
    create_attempt_at_secs: f64,
    order_poll_timeout: Duration,
    poll_every: Duration,
    live_csv_path: String,
) {
    set.spawn(async move {
        monitor_order_timeline(
            order_id,
            create_attempt_at_secs,
            Instant::now(),
            order_poll_timeout,
            poll_every,
            Some(live_csv_path),
        )
        .await
    });
}

enum MinuteOrderResult {
    Pending(OrderTimelineReport),
    Monitoring {
        order_id: String,
        create_attempt_at_secs: f64,
    },
}

async fn create_spark_to_evm_minute_order(
    minute_idx: u64,
    create_attempt_at_secs: f64,
    source_asset: &str,
    source_owner: &str,
    dest_asset: &str,
    dest_owner: &str,
    source_amount_sats: u64,
    destination_amount: &str,
    sdk: &BreezSdk,
) -> MinuteOrderResult {
    info!("[m{} spark->evm] Creating order...", minute_idx);
    match create_order_spark_to_evm(
        source_asset,
        source_owner,
        dest_asset,
        dest_owner,
        source_amount_sats.to_string(),
        destination_amount.to_string(),
    )
    .await
    {
        Ok(order) => {
            let order_id = order.order_id.clone();
            info!("[m{} spark->evm] Created order_id={}", minute_idx, order_id);
            match order.amount.parse::<u64>() {
                Ok(amount_to_send) => {
                    if let Err(e) = send_funds(sdk, &order.to, amount_to_send).await {
                        MinuteOrderResult::Pending(build_pending_row(
                            order_id,
                            create_attempt_at_secs,
                            true,
                            format!("funding failed: {}", e),
                            format!("{:#}", e),
                        ))
                    } else {
                        MinuteOrderResult::Monitoring {
                            order_id: order.order_id,
                            create_attempt_at_secs,
                        }
                    }
                }
                Err(e) => MinuteOrderResult::Pending(build_pending_row(
                    order_id,
                    create_attempt_at_secs,
                    true,
                    format!("failed to parse amount for send: {}", e),
                    format!("{:#}", e),
                )),
            }
        }
        Err(e) => MinuteOrderResult::Pending(build_pending_row(
            format!("create_failed_m{}_spark_to_evm", minute_idx),
            create_attempt_at_secs,
            false,
            "spark->evm order create failed".to_string(),
            format!("{:#}", e),
        )),
    }
}

async fn create_evm_to_spark_minute_order(
    minute_idx: u64,
    create_attempt_at_secs: f64,
    source_asset: &str,
    source_owner: &str,
    source_amount: &str,
    dest_asset: &str,
    dest_owner: &str,
    dest_amount: &str,
    evm_client: &Arc<SignerMiddleware<Provider<Http>, LocalWallet>>,
) -> MinuteOrderResult {
    info!("[m{} evm->spark] Creating order...", minute_idx);
    match create_order_evm_to_spark(
        source_asset,
        source_owner,
        source_amount,
        dest_asset,
        dest_owner,
        dest_amount,
    )
    .await
    {
        Ok(order) => {
            let order_id = order.order_id.clone();
            info!("[m{} evm->spark] Created order_id={}", minute_idx, order_id);

            if let Some(approval_tx) = &order.approval_transaction {
                if let Err(e) = execute_evm_transaction(evm_client, approval_tx).await {
                    return MinuteOrderResult::Pending(build_pending_row(
                        order_id,
                        create_attempt_at_secs,
                        true,
                        "evm->spark approval tx failed".to_string(),
                        format!("{:#}", e),
                    ));
                }
                sleep(Duration::from_secs(2)).await;
            }

            if let Err(e) = execute_evm_transaction(evm_client, &order.initiate_transaction).await {
                MinuteOrderResult::Pending(build_pending_row(
                    order_id,
                    create_attempt_at_secs,
                    true,
                    "evm->spark initiate tx failed".to_string(),
                    format!("{:#}", e),
                ))
            } else {
                MinuteOrderResult::Monitoring {
                    order_id: order.order_id,
                    create_attempt_at_secs,
                }
            }
        }
        Err(e) => MinuteOrderResult::Pending(build_pending_row(
            format!("create_failed_m{}_evm_to_spark", minute_idx),
            create_attempt_at_secs,
            false,
            "evm->spark order create failed".to_string(),
            format!("{:#}", e),
        )),
    }
}

fn handle_minute_order_result(
    result: MinuteOrderResult,
    rows: &mut Vec<OrderTimelineReport>,
    live_csv_file: &mut File,
    monitor_set: &mut JoinSet<OrderTimelineReport>,
    order_poll_timeout: Duration,
    poll_every: Duration,
    live_csv_path: &str,
) -> Result<()> {
    match result {
        MinuteOrderResult::Pending(row) => append_and_persist_row(rows, live_csv_file, row),
        MinuteOrderResult::Monitoring {
            order_id,
            create_attempt_at_secs,
        } => {
            spawn_order_monitor(
                monitor_set,
                order_id,
                create_attempt_at_secs,
                order_poll_timeout,
                poll_every,
                live_csv_path.to_string(),
            );
            Ok(())
        }
    }
}

fn init_live_report_csv() -> Result<(String, File)> {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let path = format!("order_timeline_report_{}.csv", ts);
    let mut file = File::create(&path)?;
    writeln!(
        file,
        "order_id,create_attempt_at_secs,create_success,source_init_after_secs,destination_init_after_secs,destination_redeem_after_secs,source_redeem_after_secs,status,note,error"
    )?;
    file.flush()?;
    Ok((path, file))
}

fn append_row_to_csv(file: &mut File, row: &OrderTimelineReport) -> Result<()> {
    writeln!(
        file,
        "{},{:.2},{},{},{},{},{},{},{},{}",
        row.order_id,
        row.create_attempt_at_secs,
        row.create_success,
        row.source_init_after_secs
            .map(|v| format!("{:.2}", v))
            .unwrap_or_default(),
        row.destination_init_after_secs
            .map(|v| format!("{:.2}", v))
            .unwrap_or_default(),
        row.destination_redeem_after_secs
            .map(|v| format!("{:.2}", v))
            .unwrap_or_default(),
        row.source_redeem_after_secs
            .map(|v| format!("{:.2}", v))
            .unwrap_or_default(),
        row.final_status,
        sanitize_cell(&row.note),
        sanitize_cell(&row.error)
    )?;
    file.flush()?;
    Ok(())
}

fn append_snapshot_row_to_csv_path(path: &str, row: &OrderTimelineReport) -> Result<()> {
    let mut file = OpenOptions::new().append(true).open(path)?;
    append_row_to_csv(&mut file, row)
}

fn append_and_persist_row(
    rows: &mut Vec<OrderTimelineReport>,
    file: &mut File,
    row: OrderTimelineReport,
) -> Result<()> {
    append_row_to_csv(file, &row)?;
    rows.push(row);
    Ok(())
}

fn drain_ready_monitor_rows(
    monitor_set: &mut JoinSet<OrderTimelineReport>,
    rows: &mut Vec<OrderTimelineReport>,
    live_csv_file: &mut File,
    live_csv_path: &str,
) -> Result<()> {
    while let Some(result) = monitor_set.try_join_next() {
        match result {
            Ok(row) => append_and_persist_row(rows, live_csv_file, row)?,
            Err(e) => append_and_persist_row(
                rows,
                live_csv_file,
                build_pending_row(
                    "poll_task_join_error".to_string(),
                    0.0,
                    true,
                    format!("poll task failed to join: {}", e),
                    format!("{:#}", e),
                ),
            )?,
        }
    }
    let _ = live_csv_path;
    Ok(())
}

async fn monitor_order_timeline(
    order_id: String,
    create_attempt_at_secs: f64,
    start_time: Instant,
    timeout: Duration,
    poll_every: Duration,
    live_csv_path: Option<String>,
) -> OrderTimelineReport {
    let mut source_init_after_secs: Option<f64> = None;
    let mut destination_init_after_secs: Option<f64> = None;
    let mut destination_redeem_after_secs: Option<f64> = None;
    let mut source_redeem_after_secs: Option<f64> = None;
    let mut note = "ok".to_string();
    let mut captured_error = String::new();
    let mut consecutive_errors = 0_u32;
    const MAX_CONSECUTIVE_ERRORS: u32 = 5;

    if let Some(path) = &live_csv_path {
        let _ = append_snapshot_row_to_csv_path(
            path,
            &OrderTimelineReport {
                order_id: order_id.clone(),
                create_attempt_at_secs,
                create_success: true,
                source_init_after_secs: None,
                destination_init_after_secs: None,
                destination_redeem_after_secs: None,
                source_redeem_after_secs: None,
                final_status: "pending".to_string(),
                note: "monitoring".to_string(),
                error: String::new(),
            },
        );
    }

    loop {
        if start_time.elapsed() > timeout {
            note = "poll timeout after 1 hour".to_string();
            break;
        }

        match check_order_status(&order_id).await {
            Ok(Some(s)) => {
                consecutive_errors = 0;
                let elapsed = start_time.elapsed().as_secs_f64();

                if source_init_after_secs.is_none() && !s.source_init.is_empty() {
                    source_init_after_secs = Some(elapsed);
                    if let Some(path) = &live_csv_path {
                        let _ = append_snapshot_row_to_csv_path(
                            path,
                            &OrderTimelineReport {
                                order_id: order_id.clone(),
                                create_attempt_at_secs,
                                create_success: true,
                                source_init_after_secs,
                                destination_init_after_secs,
                                destination_redeem_after_secs,
                                source_redeem_after_secs,
                                final_status: "pending".to_string(),
                                note: "source init detected".to_string(),
                                error: String::new(),
                            },
                        );
                    }
                }
                if destination_init_after_secs.is_none() && !s.dest_init.is_empty() {
                    destination_init_after_secs = Some(elapsed);
                    if let Some(path) = &live_csv_path {
                        let _ = append_snapshot_row_to_csv_path(
                            path,
                            &OrderTimelineReport {
                                order_id: order_id.clone(),
                                create_attempt_at_secs,
                                create_success: true,
                                source_init_after_secs,
                                destination_init_after_secs,
                                destination_redeem_after_secs,
                                source_redeem_after_secs,
                                final_status: "pending".to_string(),
                                note: "destination init detected".to_string(),
                                error: String::new(),
                            },
                        );
                    }
                }
                if destination_redeem_after_secs.is_none() && !s.dest_redeem.is_empty() {
                    destination_redeem_after_secs = Some(elapsed);
                    if let Some(path) = &live_csv_path {
                        let _ = append_snapshot_row_to_csv_path(
                            path,
                            &OrderTimelineReport {
                                order_id: order_id.clone(),
                                create_attempt_at_secs,
                                create_success: true,
                                source_init_after_secs,
                                destination_init_after_secs,
                                destination_redeem_after_secs,
                                source_redeem_after_secs,
                                final_status: "pending".to_string(),
                                note: "destination redeem detected".to_string(),
                                error: String::new(),
                            },
                        );
                    }
                }
                if source_redeem_after_secs.is_none() && !s.source_redeem.is_empty() {
                    source_redeem_after_secs = Some(elapsed);
                    if let Some(path) = &live_csv_path {
                        let _ = append_snapshot_row_to_csv_path(
                            path,
                            &OrderTimelineReport {
                                order_id: order_id.clone(),
                                create_attempt_at_secs,
                                create_success: true,
                                source_init_after_secs,
                                destination_init_after_secs,
                                destination_redeem_after_secs,
                                source_redeem_after_secs,
                                final_status: "pending".to_string(),
                                note: "source redeem detected".to_string(),
                                error: String::new(),
                            },
                        );
                    }
                }

                if source_init_after_secs.is_some()
                    && destination_init_after_secs.is_some()
                    && destination_redeem_after_secs.is_some()
                    && source_redeem_after_secs.is_some()
                {
                    break;
                }
            }
            Ok(None) => {
                consecutive_errors = 0;
            }
            Err(e) => {
                consecutive_errors += 1;
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                    note = format!("status check errors reached {}: {}", MAX_CONSECUTIVE_ERRORS, e);
                    captured_error = format!("{:#}", e);
                    break;
                }
            }
        }

        sleep(poll_every).await;
    }

    let completed = source_init_after_secs.is_some()
        && destination_init_after_secs.is_some()
        && destination_redeem_after_secs.is_some()
        && source_redeem_after_secs.is_some();

    OrderTimelineReport {
        order_id,
        create_attempt_at_secs,
        create_success: true,
        source_init_after_secs,
        destination_init_after_secs,
        destination_redeem_after_secs,
        source_redeem_after_secs,
        final_status: if completed {
            "completed".to_string()
        } else {
            "pending".to_string()
        },
        note,
        error: captured_error,
    }
}

fn fmt_optional_secs(value: Option<f64>) -> String {
    match value {
        Some(v) => format!("{:.2}", v),
        None => "-".to_string(),
    }
}

fn print_timeline_report(rows: &[OrderTimelineReport], total_runtime_secs: f64) {
    let total = rows.len();
    let completed = rows.iter().filter(|r| r.final_status == "completed").count();
    let pending = total.saturating_sub(completed);

    info!("");
    info!("================== BASELINE TIMELINE REPORT ==================");
    info!("Runtime: {:.1} minutes", total_runtime_secs / 60.0);
    info!(
        "Orders tracked: {} | completed: {} | pending: {}",
        total, completed, pending
    );
    info!("Columns:");
    info!(
        "order_id | s-init-after | destination-init-after | destination-redeem-after | source-redeem-after | status | note | error"
    );

    for row in rows {
        info!(
            "{} | {} | {} | {} | {} | {} | {} | {}",
            row.order_id,
            fmt_optional_secs(row.source_init_after_secs),
            fmt_optional_secs(row.destination_init_after_secs),
            fmt_optional_secs(row.destination_redeem_after_secs),
            fmt_optional_secs(row.source_redeem_after_secs),
            row.final_status,
            row.note,
            if row.error.is_empty() { "-" } else { &row.error }
        );
    }

    info!("===============================================================");
}

fn write_timeline_report_txt(rows: &[OrderTimelineReport]) -> Result<()> {
    let mut file = File::create("report.txt")?;
    writeln!(
        file,
        "order_id | s-init-after | destination-init-after | destination-redeem-after | source-redeem-after | status | note | error"
    )?;
    for row in rows {
        writeln!(
            file,
            "{} | {} | {} | {} | {} | {} | {} | {}",
            row.order_id,
            fmt_optional_secs(row.source_init_after_secs),
            fmt_optional_secs(row.destination_init_after_secs),
            fmt_optional_secs(row.destination_redeem_after_secs),
            fmt_optional_secs(row.source_redeem_after_secs),
            row.final_status,
            sanitize_cell(&row.note),
            sanitize_cell(&row.error),
        )?;
    }
    info!("Report TXT written: report.txt");
    Ok(())
}

fn sanitize_cell(s: &str) -> String {
    s.replace('\n', " | ").replace('\r', " ").replace(',', ";")
}

// ============================================================================
// SINGLE ORDER: SPARK → EVM  (5 sats -> 4 sats)
// ============================================================================

#[allow(dead_code)]
async fn test_single_spark_to_evm() -> Result<()> {
    info!("=== Single Order: Spark → EVM (5 sats -> 4 sats) ===");

    // 1. Init SDK
    let sdk = init_sdk().await?;
    let my_address = get_address(&sdk).await?;
    info!("My Spark Address: {}", my_address);

    // 2. Quote
    let source_asset = "spark:btc";
    let dest_asset = "arbitrum_sepolia:wbtc";
    let amount_sats: u64 = 5;
    let dest_amount = "4";

    info!(
        "Fetching quote {} → {} for {} sats",
        source_asset, dest_asset, amount_sats
    );
    let quote = get_quote(source_asset, dest_asset, amount_sats).await?;
    info!("Quote: {:?}", quote);

    // 3. Create Order
    info!("Creating order...");
    let order = create_order_spark_to_evm(
        source_asset,
        &my_address,
        dest_asset,
        &env_var("EVM_DEST_ADDRESS")?,
        amount_sats.to_string(),
        dest_amount.to_string(),
    )
    .await?;
    info!(
        "Order Created: ID={}, To={}, Amount={}",
        order.order_id, order.to, order.amount
    );

    // 4. Send Funds via Spark
    let send_amount: u64 = order.amount.parse()?;
    info!("Sending {} sats to {}", send_amount, order.to);
    let start_time = Instant::now();
    send_funds(&sdk, &order.to, send_amount).await?;
    info!("Funds sent!");

    // 5. Poll until complete
    poll_order_completion(&order.order_id, start_time).await?;
    info!("=== Single Spark → EVM Complete ===");
    Ok(())
}

// ============================================================================
// SINGLE ORDER: EVM (WBTC) → SPARK  (100 -> 99 units)
// ============================================================================

#[allow(dead_code)]
async fn test_single_evm_to_spark() -> Result<()> {
    info!("=== Single Order: EVM → Spark (100 -> 99 units) ===");

    // 1. Init SDK for Spark address
    let sdk = init_sdk().await?;
    let my_spark_address = get_address(&sdk).await?;
    info!("My Spark Address: {}", my_spark_address);

    // 2. Setup EVM wallet
    let provider = Provider::<Http>::try_from(env_var("ARBITRUM_SEPOLIA_RPC")?)?;
    let wallet: LocalWallet = env_var("EVM_PRIVATE_KEY")?
        .parse::<LocalWallet>()?
        .with_chain_id(421614u64);
    let evm_address = format!("{:?}", wallet.address());
    info!("EVM Wallet Address: {}", evm_address);
    let client = Arc::new(SignerMiddleware::new(provider, wallet));

    // 3. Quote
    let source_asset = "arbitrum_sepolia:wbtc";
    let dest_asset = "spark:btc";
    let source_amount = "100";
    let dest_amount = "99";

    info!("Fetching quote {} → {}", source_asset, dest_asset);
    let quote = get_quote(source_asset, dest_asset, 100).await?;
    info!("Quote: {:?}", quote);

    // 4. Create Order
    info!("Creating EVM → Spark order...");
    let order = create_order_evm_to_spark(
        source_asset,
        &evm_address,
        source_amount,
        dest_asset,
        &my_spark_address,
        dest_amount,
    )
    .await?;
    info!("Order Created: ID={}", order.order_id);

    // 5. Approval tx if needed
    if let Some(approval_tx) = &order.approval_transaction {
        info!("Executing approval transaction...");
        let approval_hash = execute_evm_transaction(&client, approval_tx).await?;
        info!("Approval tx: {:?}", approval_hash);
        info!("Waiting 15 seconds for approval to be processed...");
        sleep(Duration::from_secs(15)).await;
    } else {
        info!("No approval transaction required");
    }

    // 6. Initiate tx
    info!("Executing initiate transaction...");
    let start_time = Instant::now();
    let init_tx_hash = execute_evm_transaction(&client, &order.initiate_transaction).await?;
    info!("Initiate tx: {:?}", init_tx_hash);

    // 7. Poll until complete
    poll_order_completion(&order.order_id, start_time).await?;
    info!("=== Single EVM → Spark Complete ===");
    Ok(())
}

// ============================================================================
// BATCH TEST: SPARK → EVM  (N orders × 5 sats -> 4 sats)
// ============================================================================

#[allow(dead_code)]
async fn batch_test_spark_to_evm(total_orders: u32, batch_size: u32) -> Result<BatchTestReport> {
    info!("=== Starting Batch Test: Spark → EVM ===");
    info!(
        "Total orders: {}, Batch size: {}, Amount: 5 sats -> 4 sats each",
        total_orders, batch_size
    );

    let mut report = BatchTestReport::new();
    report.start();

    let sdk = init_sdk().await?;
    let my_address = get_address(&sdk).await?;
    info!("My Spark Address: {}", my_address);

    let source_asset = "spark:btc";
    let dest_asset = "arbitrum_sepolia:wbtc";
    let amount_sats: u64 = 5;
    let dest_amount_sats: u64 = 4;

    let num_batches = (total_orders + batch_size - 1) / batch_size;

    for batch_num in 0..num_batches {
        let batch_start = batch_num * batch_size;
        let batch_end = std::cmp::min(batch_start + batch_size, total_orders);

        info!("");
        info!(
            "========== Batch {}/{} (Orders {}-{}) ==========",
            batch_num + 1,
            num_batches,
            batch_start + 1,
            batch_end
        );

        let mut order_handles = Vec::new();

        for order_num in batch_start..batch_end {
            let order_idx = order_num + 1;

            if order_num > batch_start {
                sleep(Duration::from_millis(500)).await;
            }

            info!("[Order {}] Creating order...", order_idx);

            match create_order_spark_to_evm(
                source_asset,
                &my_address,
                dest_asset,
                &env_var("EVM_DEST_ADDRESS")?,
                amount_sats.to_string(),
                dest_amount_sats.to_string(),
            )
            .await
            {
                Ok(order_res) => {
                    info!(
                        "[Order {}] Order created: ID={}",
                        order_idx, order_res.order_id
                    );

                    let amount_val: u64 = match order_res.amount.parse() {
                        Ok(v) => v,
                        Err(e) => {
                            report.record_failure(
                                order_res.order_id.clone(),
                                format!("Failed to parse amount: {}", e),
                            );
                            continue;
                        }
                    };

                    let start_time = Instant::now();
                    match send_funds(&sdk, &order_res.to, amount_val).await {
                        Ok(_) => {
                            info!("[Order {}] Funds sent to {}", order_idx, order_res.to);
                            order_handles.push((order_idx, order_res.order_id, start_time));
                        }
                        Err(e) => {
                            report.record_failure(
                                order_res.order_id,
                                format!("Failed to send funds: {}", e),
                            );
                        }
                    }
                }
                Err(e) => {
                    report.record_failure(
                        format!("order_{}", order_idx),
                        format!("Failed to create order: {}", e),
                    );
                }
            }
        }

        // Poll all orders in this batch
        for (order_idx, order_id, start_time) in order_handles {
            info!("[Order {}] Waiting for completion...", order_idx);
            match poll_order_completion_silent(&order_id, start_time).await {
                Ok(duration) => {
                    info!("[Order {}] ✅ Completed in {:.2}s", order_idx, duration);
                    report.record_success(duration);
                }
                Err(e) => {
                    info!("[Order {}] ❌ Failed: {}", order_idx, e);
                    report.record_failure(order_id, e.to_string());
                }
            }
        }

        info!(
            "Batch {} complete. Progress: {}/{} orders processed",
            batch_num + 1,
            report.total_orders,
            total_orders
        );

        // Pause between batches to avoid rate limits
        if batch_num + 1 < num_batches {
            sleep(Duration::from_secs(2)).await;
        }
    }

    report.finish();
    report.print_report("Spark → EVM (5->4 sats per order)");
    info!("=== Batch Test Complete: Spark → EVM ===");
    Ok(report)
}

// ============================================================================
// BATCH TEST: EVM (WBTC) → SPARK  (N orders × 100 -> 99 units)
// ============================================================================

#[allow(dead_code)]
async fn batch_test_evm_to_spark(total_orders: u32, batch_size: u32) -> Result<BatchTestReport> {
    info!("=== Starting Batch Test: EVM → Spark ===");
    info!(
        "Total orders: {}, Batch size: {}, Amount: 100 -> 99 units each",
        total_orders, batch_size
    );

    let mut report = BatchTestReport::new();
    report.start();

    let sdk = init_sdk().await?;
    let my_spark_address = get_address(&sdk).await?;
    info!("My Spark Address: {}", my_spark_address);

    let provider = Provider::<Http>::try_from(env_var("ARBITRUM_SEPOLIA_RPC")?)?;
    let wallet: LocalWallet = env_var("EVM_PRIVATE_KEY")?
        .parse::<LocalWallet>()?
        .with_chain_id(421614u64);
    let evm_address = format!("{:?}", wallet.address());
    info!("EVM Wallet Address: {}", evm_address);
    let client = Arc::new(SignerMiddleware::new(provider, wallet));

    let source_asset = "arbitrum_sepolia:wbtc";
    let dest_asset = "spark:btc";
    let source_amount = "100";
    let dest_amount = "99";

    let num_batches = (total_orders + batch_size - 1) / batch_size;

    for batch_num in 0..num_batches {
        let batch_start = batch_num * batch_size;
        let batch_end = std::cmp::min(batch_start + batch_size, total_orders);

        info!("");
        info!(
            "========== Batch {}/{} (Orders {}-{}) ==========",
            batch_num + 1,
            num_batches,
            batch_start + 1,
            batch_end
        );

        let mut order_ids = Vec::new();

        for order_num in batch_start..batch_end {
            let order_idx = order_num + 1;

            if order_num > batch_start {
                sleep(Duration::from_millis(500)).await;
            }

            info!("[Order {}] Creating EVM → Spark order...", order_idx);

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
                Ok(order_res) => {
                    info!(
                        "[Order {}] Order created: ID={}",
                        order_idx, order_res.order_id
                    );

                    // Approval if needed
                    if let Some(approval_tx) = &order_res.approval_transaction {
                        info!("[Order {}] Executing approval transaction...", order_idx);
                        if let Err(e) = execute_evm_transaction(&client, approval_tx).await {
                            report.record_failure(
                                order_res.order_id,
                                format!("Approval tx failed: {}", e),
                            );
                            continue;
                        }
                        sleep(Duration::from_secs(2)).await;
                    }

                    // Initiate
                    info!("[Order {}] Executing initiate transaction...", order_idx);
                    let start_time = Instant::now();
                    match execute_evm_transaction(&client, &order_res.initiate_transaction).await {
                        Ok(tx_hash) => {
                            info!("[Order {}] Initiate tx: {:?}", order_idx, tx_hash);
                            order_ids.push((order_idx, order_res.order_id, start_time));
                        }
                        Err(e) => {
                            report.record_failure(
                                order_res.order_id,
                                format!("Initiate tx failed: {}", e),
                            );
                        }
                    }
                }
                Err(e) => {
                    report.record_failure(
                        format!("order_{}", order_idx),
                        format!("Failed to create order: {}", e),
                    );
                }
            }
        }

        // Poll all orders in this batch
        for (order_idx, order_id, start_time) in order_ids {
            info!("[Order {}] Waiting for completion...", order_idx);
            match poll_order_completion_silent(&order_id, start_time).await {
                Ok(duration) => {
                    info!("[Order {}] ✅ Completed in {:.2}s", order_idx, duration);
                    report.record_success(duration);
                }
                Err(e) => {
                    info!("[Order {}] ❌ Failed: {}", order_idx, e);
                    report.record_failure(order_id, e.to_string());
                }
            }
        }

        info!(
            "Batch {} complete. Progress: {}/{} orders processed",
            batch_num + 1,
            report.total_orders,
            total_orders
        );

        if batch_num + 1 < num_batches {
            sleep(Duration::from_secs(2)).await;
        }
    }

    report.finish();
    report.print_report("EVM → Spark (100->99 units per order)");
    info!("=== Batch Test Complete: EVM → Spark ===");
    Ok(report)
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
    let connect_request = ConnectRequest {
        config,
        seed,
        storage_dir,
    };
    let sdk = connect(connect_request).await?;
    info!("SDK initialized successfully");
    Ok(sdk)
}

async fn get_address(sdk: &BreezSdk) -> Result<String> {
    let info = sdk
        .get_info(GetInfoRequest {
            ensure_synced: Some(false),
        })
        .await?;
    info!("Balance: {} sats", info.balance_sats);
    let receive_response = sdk
        .receive_payment(ReceivePaymentRequest {
            payment_method: ReceivePaymentMethod::SparkAddress,
        })
        .await?;
    Ok(receive_response.payment_request)
}

#[allow(dead_code)]
async fn send_funds(sdk: &BreezSdk, dest: &str, amount: u64) -> Result<()> {
    info!("Preparing payment to {} for {} sats", dest, amount);
    let prepare_request = PrepareSendPaymentRequest {
        payment_request: dest.to_string(),
        amount: Some(amount as u128),
        token_identifier: None,
        conversion_options: None,
        fee_policy: None,
    };
    let prepare_response = sdk.prepare_send_payment(prepare_request).await?;
    info!("Payment prepared successfully");
    let send_request = SendPaymentRequest {
        prepare_response,
        options: None,
        idempotency_key: None,
    };
    let _payment_result = sdk.send_payment(send_request).await?;
    info!("Payment sent successfully");
    Ok(())
}

// ============================================================================
// API HELPERS
// ============================================================================

#[allow(dead_code)]
async fn get_quote(from: &str, to: &str, amount: u64) -> Result<Value> {
    let url = format!("{}/v2/quote", env_var("QUOTE_BASE_URL")?);
    let client = Client::new();
    let garden_app_id = env_var("GARDEN_APP_ID")?;
    let response = client
        .get(&url)
        .header("garden-app-id", garden_app_id)
        .query(&[
            ("from", from),
            ("to", to),
            ("from_amount", &amount.to_string()),
        ])
        .send()
        .await?;
    let status = response.status();
    let response_text = response.text().await?;
    if !status.is_success() {
        return Err(eyre!(
            "Quote API failed with status {}: {}",
            status,
            response_text
        ));
    }
    if response_text.is_empty() {
        return Err(eyre!("Quote API returned empty response"));
    }
    let json: Value = serde_json::from_str(&response_text)
        .map_err(|e| eyre!("Failed to parse quote JSON: {}. Body: {}", e, response_text))?;
    Ok(json)
}

async fn make_order_request_with_retry(
    client: &Client,
    url: &str,
    req: &OrderRequest,
    max_retries: u32,
) -> Result<(reqwest::StatusCode, String)> {
    let mut retry_count = 0;
    let base_delay = Duration::from_secs(1);
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
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            if retry_count < max_retries {
                retry_count += 1;
                let delay = base_delay * (1 << (retry_count - 1));
                info!(
                    "Rate limit hit (429), retrying in {:.1}s (attempt {}/{})...",
                    delay.as_secs_f64(),
                    retry_count,
                    max_retries
                );
                sleep(delay).await;
                continue;
            } else {
                if let Ok(error_res) = serde_json::from_str::<ErrorResponse>(&res_text) {
                    let msg = error_res
                        .message
                        .unwrap_or_else(|| "Rate limit exceeded".to_string());
                    return Err(eyre!("API error ({}): {} (max retries)", status, msg));
                }
                return Err(eyre!("Rate limit exceeded after {} retries", max_retries));
            }
        }
        return Ok((status, res_text));
    }
}

#[allow(dead_code)]
async fn create_order_spark_to_evm(
    source_asset: &str,
    source_owner: &str,
    dest_asset: &str,
    dest_owner: &str,
    amount: String,
    dest_amount: String,
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
            amount: dest_amount,
        },
        solver_id: Some("staging-2".to_string()),
    };
    let (status, res_text) = make_order_request_with_retry(&client, &url, &req, 5).await?;
    if !status.is_success() {
        if let Ok(error_res) = serde_json::from_str::<ErrorResponse>(&res_text) {
            let msg = error_res.message.unwrap_or_else(|| "Unknown".to_string());
            return Err(eyre!("API error ({}): {}", status, msg));
        }
        return Err(eyre!("API failed {}: {}", status, res_text));
    }
    if let Ok(error_res) = serde_json::from_str::<ErrorResponse>(&res_text) {
        if error_res.message.is_some() {
            return Err(eyre!("API error: {}", error_res.message.unwrap()));
        }
    }
    let res_json: CreateOrderResponse = serde_json::from_str(&res_text)
        .map_err(|e| eyre!("Parse error: {}. Body: {}", e, res_text))?;
    if res_json.status != "Ok" {
        return Err(eyre!("Order creation failed: {}", res_json.status));
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
    info!("Creating order with request: {:?}", req);
    let (status, res_text) = make_order_request_with_retry(&client, &url, &req, 5).await?;
    info!("Order response: {}", res_text);
    if !status.is_success() {
        if let Ok(error_res) = serde_json::from_str::<ErrorResponse>(&res_text) {
            let msg = error_res.message.unwrap_or_else(|| "Unknown".to_string());
            return Err(eyre!("API error ({}): {}", status, msg));
        }
        return Err(eyre!("API failed {}: {}", status, res_text));
    }
    if let Ok(error_res) = serde_json::from_str::<ErrorResponse>(&res_text) {
        if error_res.message.is_some() {
            return Err(eyre!("API error: {}", error_res.message.unwrap()));
        }
    }
    let res_json: EvmToSparkOrderResponse = serde_json::from_str(&res_text)
        .map_err(|e| eyre!("Parse error: {}. Body: {}", e, res_text))?;
    if res_json.status != "Ok" {
        return Err(eyre!("Order creation failed: {}", res_json.status));
    }
    Ok(res_json.result)
}

// ============================================================================
// EVM TRANSACTION EXECUTION
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
    let data_len = data.len();

    let provider = client.inner();
    let fee_data = provider.estimate_eip1559_fees(None).await;
    let (max_fee_per_gas, max_priority_fee_per_gas) = match fee_data {
        Ok(fees) => {
            info!("Gas fees - Max: {:?}, Priority: {:?}", fees.0, fees.1);
            (fees.0, fees.1)
        }
        Err(e) => {
            info!("Could not estimate gas fees, using defaults: {}", e);
            (U256::from(100_000_000_000u64), U256::from(1_000_000_000u64))
        }
    };

    info!("Sending tx to: {}", tx_data.to);
    info!(
        "Gas limit: {:?}, Value: {:?}, Data: {} bytes",
        gas_limit, value, data_len
    );

    let tx = Eip1559TransactionRequest::new()
        .to(to_address)
        .value(value)
        .data(data)
        .gas(gas_limit)
        .max_fee_per_gas(max_fee_per_gas)
        .max_priority_fee_per_gas(max_priority_fee_per_gas);

    let pending_tx = client.send_transaction(tx, None).await?;
    let tx_hash = pending_tx.tx_hash();
    info!("Transaction hash: {:?}", tx_hash);

    let receipt = pending_tx
        .await?
        .ok_or_else(|| eyre!("Transaction failed - no receipt"))?;

    info!(
        "Receipt - Status: {:?}, Block: {:?}, Gas Used: {:?}",
        receipt.status, receipt.block_number, receipt.gas_used
    );

    if receipt.status == Some(1.into()) {
        info!("Transaction confirmed in block: {:?}", receipt.block_number);
    } else {
        let error_msg = if let Some(gas_used) = receipt.gas_used {
            let pct = (gas_used.as_u64() as f64 / gas_limit.as_u64() as f64) * 100.0;
            format!(
                "Reverted. Gas used: {:?} ({:.1}%), Block: {:?}",
                gas_used, pct, receipt.block_number
            )
        } else {
            format!("Reverted. Block: {:?}", receipt.block_number)
        };
        error!("{}", error_msg);
        error!(
            "To: {}, Value: {:?}, Data: {} bytes",
            tx_data.to, value, data_len
        );
        error!("Tx hash: {:?}", tx_hash);
        error!("Debug: GET {}/v2/orders/<order_id>", env_var("API_BASE_URL")?);
        error!("Explorer: https://sepolia.arbiscan.io/tx/{:?}", tx_hash);
        return Err(eyre!("Transaction reverted: {}", error_msg));
    }

    Ok(tx_hash)
}

// ============================================================================
// ORDER STATUS POLLING
// ============================================================================

async fn check_order_status_with_retry(
    order_id: &str,
    max_retries: u32,
) -> Result<Option<SwapStatus>> {
    let api_base_url = env_var("API_BASE_URL")?;
    let url = format!("{}/v2/orders/{}", api_base_url, order_id);
    let client = Client::new();
    let mut retry_count = 0;
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
                    let delay = base_delay * (1 << (retry_count - 1));
                    info!(
                        "[{}] Network error, retry {}/{} in {:.1}s...",
                        order_id,
                        retry_count,
                        max_retries,
                        delay.as_secs_f64()
                    );
                    sleep(delay).await;
                    continue;
                } else {
                    return Err(eyre!(
                        "Status check failed after {} retries: {}",
                        max_retries,
                        e
                    ));
                }
            }
        };

        let status = res.status();
        let res_text = res.text().await?;

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            if retry_count < max_retries {
                retry_count += 1;
                let delay = base_delay * (1 << (retry_count - 1));
                info!(
                    "[{}] Rate limited, retry {}/{} in {:.1}s...",
                    order_id,
                    retry_count,
                    max_retries,
                    delay.as_secs_f64()
                );
                sleep(delay).await;
                continue;
            } else {
                return Err(eyre!(
                    "Rate limit after {} retries for {}",
                    max_retries,
                    order_id
                ));
            }
        }

        if !status.is_success() {
            if retry_count < max_retries && status.is_server_error() {
                retry_count += 1;
                let delay = base_delay * (1 << (retry_count - 1));
                info!(
                    "[{}] Server error ({}), retry {}/{} in {:.1}s...",
                    order_id,
                    status,
                    retry_count,
                    max_retries,
                    delay.as_secs_f64()
                );
                sleep(delay).await;
                continue;
            } else {
                return Err(eyre!("API {} for order {}: {}", status, order_id, res_text));
            }
        }

        let res: Value = match serde_json::from_str(&res_text) {
            Ok(v) => v,
            Err(e) => {
                if retry_count < max_retries {
                    retry_count += 1;
                    let delay = base_delay * (1 << (retry_count - 1));
                    sleep(delay).await;
                    continue;
                } else {
                    return Err(eyre!("JSON parse error for {}: {}", order_id, e));
                }
            }
        };

        if let Some(result) = res.get("result") {
            let source_init = result["source_swap"]["initiate_tx_hash"]
                .as_str()
                .unwrap_or("")
                .to_string();
            let source_redeem = result["source_swap"]["redeem_tx_hash"]
                .as_str()
                .unwrap_or("")
                .to_string();
            let dest_init = result["destination_swap"]["initiate_tx_hash"]
                .as_str()
                .unwrap_or("")
                .to_string();
            let dest_redeem = result["destination_swap"]["redeem_tx_hash"]
                .as_str()
                .unwrap_or("")
                .to_string();
            return Ok(Some(SwapStatus {
                source_init,
                dest_init,
                source_redeem,
                dest_redeem,
            }));
        }

        return Ok(None);
    }
}

async fn check_order_status(order_id: &str) -> Result<Option<SwapStatus>> {
    check_order_status_with_retry(order_id, 3).await
}

async fn poll_order_completion(order_id: &str, start_time: Instant) -> Result<()> {
    info!("Polling for order completion...");
    let mut s_init_detected = false;
    let mut d_init_detected = false;
    let mut s_redeem_detected = false;
    let mut consecutive_errors = 0;
    const MAX_CONSECUTIVE_ERRORS: u32 = 5;

    loop {
        if start_time.elapsed() > Duration::from_secs(300) {
            error!("TIMEOUT: Order did not complete in 5 minutes!");
            return Err(eyre!("Order completion timeout"));
        }

        let status_result = check_order_status(order_id).await;
        match status_result {
            Ok(Some(s)) => {
                consecutive_errors = 0;

                if !s.source_init.is_empty() && !s_init_detected {
                    s_init_detected = true;
                    info!(
                        "⚡ Source Initiate after {:.2}s: {}",
                        start_time.elapsed().as_secs_f64(),
                        s.source_init
                    );
                }

                if !s.dest_init.is_empty() && !d_init_detected {
                    d_init_detected = true;
                    info!(
                        "⚡ Dest Initiate after {:.2}s: {}",
                        start_time.elapsed().as_secs_f64(),
                        s.dest_init
                    );
                }

                if !s.source_redeem.is_empty() && !s_redeem_detected {
                    s_redeem_detected = true;
                    info!(
                        "⚡ Source Redeem after {:.2}s: {}",
                        start_time.elapsed().as_secs_f64(),
                        s.source_redeem
                    );
                }

                info!(
                    "Progress: [S-Init: {}, D-Init: {}, S-Redeem: {}, D-Redeem: {}]",
                    if s.source_init.is_empty() {
                        "pend"
                    } else {
                        "ok"
                    },
                    if s.dest_init.is_empty() { "pend" } else { "ok" },
                    if s.source_redeem.is_empty() {
                        "pend"
                    } else {
                        "ok"
                    },
                    if s.dest_redeem.is_empty() {
                        "pend"
                    } else {
                        "ok"
                    },
                );

                if !s.source_init.is_empty() && !s.dest_redeem.is_empty() {
                    let duration = start_time.elapsed();
                    info!(
                        "✅ Order Complete! S-Init: {}, D-Redeem: {}",
                        s.source_init, s.dest_redeem
                    );
                    info!("⏱️  Total Swap took: {:.2} seconds", duration.as_secs_f64());
                    return Ok(());
                }
            }
            Ok(None) => {
                consecutive_errors = 0;
            }
            Err(e) => {
                consecutive_errors += 1;
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                    return Err(eyre!(
                        "Too many consecutive errors ({}): {}",
                        consecutive_errors,
                        e
                    ));
                }
                error!(
                    "Status check error (attempt {}/{}): {}",
                    consecutive_errors, MAX_CONSECUTIVE_ERRORS, e
                );
            }
        }
        sleep(Duration::from_secs(5)).await;
    }
}

/// Silent polling — returns swap duration in seconds
async fn poll_order_completion_silent(order_id: &str, start_time: Instant) -> Result<f64> {
    let mut s_init_detected = false;
    let mut d_init_detected = false;
    let mut s_redeem_detected = false;
    let mut consecutive_errors = 0;
    const MAX_CONSECUTIVE_ERRORS: u32 = 5;

    loop {
        if start_time.elapsed() > Duration::from_secs(300) {
            return Err(eyre!("Order completion timeout after 5 minutes"));
        }

        let status_result = check_order_status(order_id).await;
        match status_result {
            Ok(Some(s)) => {
                consecutive_errors = 0;

                if !s.source_init.is_empty() && !s_init_detected {
                    s_init_detected = true;
                    info!(
                        "[{}] ⚡ S-Init after {:.2}s",
                        order_id,
                        start_time.elapsed().as_secs_f64()
                    );
                }

                if !s.dest_init.is_empty() && !d_init_detected {
                    d_init_detected = true;
                    info!(
                        "[{}] ⚡ D-Init after {:.2}s",
                        order_id,
                        start_time.elapsed().as_secs_f64()
                    );
                }

                if !s.source_redeem.is_empty() && !s_redeem_detected {
                    s_redeem_detected = true;
                    info!(
                        "[{}] ⚡ S-Redeem after {:.2}s",
                        order_id,
                        start_time.elapsed().as_secs_f64()
                    );
                }

                if !s.source_init.is_empty() && !s.dest_redeem.is_empty() {
                    let duration = start_time.elapsed().as_secs_f64();
                    info!("[{}] ✅ Done in {:.2}s", order_id, duration);
                    return Ok(duration);
                }
            }
            Ok(None) => {
                consecutive_errors = 0;
            }
            Err(e) => {
                consecutive_errors += 1;
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                    return Err(eyre!("Too many errors ({}): {}", consecutive_errors, e));
                }
                error!(
                    "[{}] Status error ({}/{}): {}",
                    order_id, consecutive_errors, MAX_CONSECUTIVE_ERRORS, e
                );
            }
        }
        sleep(Duration::from_secs(5)).await;
    }
}


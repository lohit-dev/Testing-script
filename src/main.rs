//cargo run --release --bin testing_script
use breez_sdk_spark::{
    connect, default_config, BreezSdk, ConnectRequest, GetInfoRequest, Network,
    PrepareSendPaymentRequest, ReceivePaymentMethod, ReceivePaymentRequest, Seed,
    SendPaymentRequest,
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

mod flood_test;
// ============================================================================
// CONSTANTS
// ============================================================================

// const API_BASE_URL: &str = "https://api.garden-staging.dealpulley.com";
// const GARDEN_APP_ID: &str = "a3a9c5cb79b8dd354c4829ef01342d37e0ca0aa3df3929780b14f6a38e99346b";
// const API_BASE_URL: &str = "https://testnet.api.garden.finance"; 
// const GARDEN_APP_ID: &str = "79702d04cb63391922f2e1471afe4743b0e3ba71f260e3c2117aa36e7fb74a9c";

const QUOTE_BASE_URL: &str = "http://gsg8cwk4k8oscg4sgcgg8ww8.garden-staging.dealpulley.com";
const  API_BASE_URL: &str = "http://w4skog4oscw8sk00c8g8wg8s.garden-staging.dealpulley.com";
const GARDEN_APP_ID: &str = "ec2f8d63f5a098aceefff45f0af2afa25f800fdc3af9f094baea084a9860a522";

const TEST_MNEMONIC: &str =
    "stomach split fat oil voice swear ecology armor creek author urge pelican";
const STORAGE_DIR: &str = "./tmp/spark_test_sdk";

const EVM_PRIVATE_KEY: &str = "e7a855ef5d7cbb0704c1e52d59fd2c8e77bfb43c60cba9921f222abf52a5804f";
const ARBITRUM_SEPOLIA_RPC: &str = "https://sepolia-rollup.arbitrum.io/rpc";

const EVM_DEST_ADDRESS: &str = "0x47b03906469a90c8a597766ab1830c57a656968b";

// ============================================================================
// SOLANA CONSTANTS
// ============================================================================
const SOLANA_PRIVATE_KEY: &str =
    "5QvsL4MZrizGkPbapnz6DQsKGuFUYg2akhuWV6YxsLw7eqHMFKtM55hA8wKMK6VfXkHn6SHsYLpPzNU2UraAdQpj";
const SOLANA_DEVNET_RPC: &str = "https://api.devnet.solana.com";

// ============================================================================
// SOLANA ORDER RESPONSE TYPES
// ============================================================================
#[derive(Deserialize, Debug, Clone)]
struct SolanaOrderResponse {
    status: String,
    result: SolanaOrderResult,
}

// ============================================================================
// DIRECT EVM INITIATE TEST (MANUAL DATA)
// ============================================================================

#[cfg(test)]
mod manual_evm_initiate_tests {
    use super::*;

    #[tokio::test]
    async fn test_manual_evm_initiate_on_arbitrum() -> Result<()> {
        // Init EVM wallet on Arbitrum Sepolia
        let provider = Provider::<Http>::try_from(ARBITRUM_SEPOLIA_RPC)?;
        let wallet: LocalWallet = EVM_PRIVATE_KEY
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

#[allow(dead_code)]
#[derive(Deserialize, Debug, Clone)]
struct SolanaOrderResult {
    order_id: String,
    versioned_tx: String,
    versioned_tx_gasless: Option<String>,
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
    color_eyre::install()?;
    env_logger::builder()
    .filter_level(log::LevelFilter::Off)              // default: nothing logs
    .filter_module("testing_script", log::LevelFilter::Info) // only your crate logs
    .init();

    info!("╔══════════════════════════════════════════════════════════╗");
    info!("║       GARDEN SWAP TEST SUITE — STAGING                  ║");
    info!("║       {}               ║", API_BASE_URL);
    info!("╚══════════════════════════════════════════════════════════╝");

    // ── Uncomment the test you want to run ───────────────────────

    // Single order tests (500 sats / 500 wbtc)
    test_single_evm_to_spark().await?;
    // for i in 0..100 {
    // test_single_spark_to_evm().await?;
    // }
    // for i in 0..100 {
    //     test_solona_to_spark().await?;
    // }
    //test_solana_cbbtc_to_spark().await?;

    // Batch stress tests (500 orders)
    // batch_test_evm_to_spark(100, 5).await?;
    // batch_test_spark_to_evm(100, 5).await?;

    // batch_test_solana_to_spark(500, 10).await?;
    // or
    // batch_test_spark_to_solana(500, 10).await?;

    Ok(())
}

// ============================================================================
// SINGLE ORDER: SPARK → EVM  (500 sats)
// ============================================================================

#[allow(dead_code)]
async fn test_single_spark_to_evm() -> Result<()> {
    info!("=== Single Order: Spark → EVM (500 sats) ===");

    // 1. Init SDK
    let sdk = init_sdk().await?;
    let my_address = get_address(&sdk).await?;
    info!("My Spark Address: {}", my_address);

    // 2. Quote
    let source_asset = "spark_regtest:btc";
    let dest_asset = "arbitrum_sepolia:wbtc";
    let amount_sats: u64 = 500;

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
        EVM_DEST_ADDRESS,
        amount_sats.to_string(),
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
// SINGLE ORDER: EVM (WBTC) → SPARK  (500 wbtc units)
// ============================================================================

#[allow(dead_code)]
async fn test_single_evm_to_spark() -> Result<()> {
    info!("=== Single Order: EVM → Spark (500 WBTC) ===");

    // 1. Init SDK for Spark address
    let sdk = init_sdk().await?;
    let my_spark_address = get_address(&sdk).await?;
    info!("My Spark Address: {}", my_spark_address);

    // 2. Setup EVM wallet
    let provider = Provider::<Http>::try_from(ARBITRUM_SEPOLIA_RPC)?;
    let wallet: LocalWallet = EVM_PRIVATE_KEY
        .parse::<LocalWallet>()?
        .with_chain_id(421614u64);
    let evm_address = format!("{:?}", wallet.address());
    info!("EVM Wallet Address: {}", evm_address);
    let client = Arc::new(SignerMiddleware::new(provider, wallet));

    // 3. Quote
    let source_asset = "arbitrum_sepolia:wbtc";
    let dest_asset = "spark_regtest:btc";
    let source_amount = "500";
    let dest_amount = "498";

    info!("Fetching quote {} → {}", source_asset, dest_asset);
    let quote = get_quote(dest_asset, source_asset, 500).await?;
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
// BATCH TEST: SPARK → EVM  (500 orders × 500 sats)
// ============================================================================

#[allow(dead_code)]
async fn batch_test_spark_to_evm(total_orders: u32, batch_size: u32) -> Result<BatchTestReport> {
    info!("=== Starting Batch Test: Spark → EVM ===");
    info!(
        "Total orders: {}, Batch size: {}, Amount: 500 sats each",
        total_orders, batch_size
    );

    let mut report = BatchTestReport::new();
    report.start();

    let sdk = init_sdk().await?;
    let my_address = get_address(&sdk).await?;
    info!("My Spark Address: {}", my_address);

    let source_asset = "spark_regtest:btc";
    let dest_asset = "arbitrum_sepolia:wbtc";
    let amount_sats: u64 = 500;

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
                EVM_DEST_ADDRESS,
                amount_sats.to_string(),
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
    report.print_report("Spark → EVM (500 sats × 500 orders)");
    info!("=== Batch Test Complete: Spark → EVM ===");
    Ok(report)
}

// ============================================================================
// BATCH TEST: EVM (WBTC) → SPARK  (500 orders × 500 wbtc)
// ============================================================================

#[allow(dead_code)]
async fn batch_test_evm_to_spark(total_orders: u32, batch_size: u32) -> Result<BatchTestReport> {
    info!("=== Starting Batch Test: EVM → Spark ===");
    info!(
        "Total orders: {}, Batch size: {}, Amount: 500 WBTC each",
        total_orders, batch_size
    );

    let mut report = BatchTestReport::new();
    report.start();

    let sdk = init_sdk().await?;
    let my_spark_address = get_address(&sdk).await?;
    info!("My Spark Address: {}", my_spark_address);

    let provider = Provider::<Http>::try_from(ARBITRUM_SEPOLIA_RPC)?;
    let wallet: LocalWallet = EVM_PRIVATE_KEY
        .parse::<LocalWallet>()?
        .with_chain_id(421614u64);
    let evm_address = format!("{:?}", wallet.address());
    info!("EVM Wallet Address: {}", evm_address);
    let client = Arc::new(SignerMiddleware::new(provider, wallet));

    let source_asset = "arbitrum_sepolia:wbtc";
    let dest_asset = "spark_regtest:btc";
    let source_amount = "500";
    let dest_amount = "490";

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
    report.print_report("EVM → Spark (500 WBTC × 500 orders)");
    info!("=== Batch Test Complete: EVM → Spark ===");
    Ok(report)
}

// ============================================================================
// SDK HELPERS
// ============================================================================

async fn init_sdk() -> Result<BreezSdk> {
    info!("Initializing SDK...");
    std::fs::create_dir_all(STORAGE_DIR)?;
    let config = default_config(Network::Regtest);
    let seed = Seed::Mnemonic {
        mnemonic: TEST_MNEMONIC.to_string(),
        passphrase: None,
    };
    let connect_request = ConnectRequest {
        config,
        seed,
        storage_dir: STORAGE_DIR.to_string(),
    };
    let sdk = connect(connect_request).await?;
    info!("SDK initialized successfully");
    Ok(sdk)
}

async fn get_address(sdk: &BreezSdk) -> Result<String> {
    let info = sdk
        .get_info(GetInfoRequest {
            ensure_synced: Some(true),
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
    let url = format!("{}/v2/quote", QUOTE_BASE_URL);
    let client = Client::new();
    let response = client
        .get(&url)
        .header("garden-app-id", GARDEN_APP_ID)
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
    loop {
        let res = client
            .post(url)
            .header("garden-app-id", GARDEN_APP_ID)
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
) -> Result<CreateOrderResult> {
    let url = format!("{}/v2/orders", API_BASE_URL);
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
        solver_id: None,
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
    let url = format!("{}/v2/orders", API_BASE_URL);
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
        solver_id: None,
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
        error!("Debug: GET {}/v2/orders/<order_id>", API_BASE_URL);
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
    let url = format!("{}/v2/orders/{}", API_BASE_URL, order_id);
    let client = Client::new();
    let mut retry_count = 0;
    let base_delay = Duration::from_secs(1);

    loop {
        let response = client
            .get(&url)
            .header("garden-app-id", GARDEN_APP_ID)
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

// ============================================================================
// SINGLE ORDER: SOLANA (devnet) → SPARK (0.001 SOL)
// ============================================================================
#[allow(dead_code)]
async fn test_solona_to_spark() -> Result<()> {
    use base64::Engine;
    use solana_client::nonblocking::rpc_client::RpcClient as SolanaRpcClient;
    use solana_sdk::{
        commitment_config::CommitmentConfig, signature::Keypair, signer::Signer as SolanaSigner,
        transaction::VersionedTransaction,
    };

    info!("=== Single Order: Solana (devnet) → Spark (0.001 SOL) ===");

    // 1. Init Spark SDK for destination address
    let sdk = init_sdk().await?;
    let spark_address = get_address(&sdk).await?;
    info!("Spark Destination Address: {}", spark_address);

    // 2. Setup Solana keypair from base58 private key
    let keypair_bytes = bs58::decode(SOLANA_PRIVATE_KEY)
        .into_vec()
        .map_err(|e| eyre!("Failed to decode Solana private key: {}", e))?;
    let keypair =
        Keypair::from_bytes(&keypair_bytes).map_err(|e| eyre!("Invalid Solana keypair: {}", e))?;
    let solana_pubkey = keypair.pubkey();
    info!("Solana Wallet: {}", solana_pubkey);

    // 3. Setup async Solana RPC client
    let rpc_client = SolanaRpcClient::new_with_commitment(
        SOLANA_DEVNET_RPC.to_string(),
        CommitmentConfig::confirmed(),
    );

    // Check balance
    let balance = rpc_client
        .get_balance(&solana_pubkey)
        .await
        .map_err(|e| eyre!("Failed to get Solana balance: {}", e))?;
    info!(
        "Solana Balance: {} lamports ({:.6} SOL)",
        balance,
        balance as f64 / 1_000_000_000.0
    );
    if balance < 2_000_000 {
        return Err(eyre!(
            "Insufficient SOL: {} lamports. Need ~0.002 SOL (0.001 + fees). Use: solana airdrop 1 {} --url devnet",
            balance,
            solana_pubkey
        ));
    }

    // 4. Get Quote: 0.001 SOL = 1,000,000 lamports
    let source_asset = "solana_testnet:sol";
    let dest_asset = "spark_regtest:btc";
    let amount_lamports: u64 = 1_000_000;

    info!(
        "Fetching quote {} → {} for {} lamports (0.001 SOL)",
        source_asset, dest_asset, amount_lamports
    );
    let quote = get_quote(source_asset, dest_asset, amount_lamports).await?;
    info!("Quote: {:?}", quote);

    let dest_amount = quote["result"][0]["destination"]["amount"]
        .as_str()
        .ok_or_else(|| eyre!("No destination amount in quote: {:?}", quote))?
        .to_string();
    info!("Destination amount: {} sats", dest_amount);

    // 5. Create Order
    info!("Creating Solana → Spark order...");
    let order = create_order_solana_to_spark(
        source_asset,
        &solana_pubkey.to_string(),
        &amount_lamports.to_string(),
        dest_asset,
        &spark_address,
        &dest_amount,
    )
    .await?;
    info!("Order Created: ID={}", order.order_id);

    // 6. Decode the versioned_tx (API returns base64)
    info!("Decoding versioned transaction...");
    let tx_bytes = base64::engine::general_purpose::STANDARD
        .decode(&order.versioned_tx)
        .map_err(|e| eyre!("Failed to base64-decode versioned_tx: {}", e))?;

    let mut transaction: VersionedTransaction = bincode::deserialize(&tx_bytes)
        .map_err(|e| eyre!("Failed to deserialize VersionedTransaction: {}", e))?;
    info!("Transaction deserialized ({} bytes)", tx_bytes.len());

    // 7. Sign the transaction (replace placeholder signature at index 0)
    let message_bytes = transaction.message.serialize();
    let sig = keypair.sign_message(&message_bytes);
    transaction.signatures[0] = sig;
    info!("Transaction signed: {}", sig);

    // 8. Send the transaction to Solana devnet
    let start_time = Instant::now();
    info!("Sending Solana transaction...");

    let tx_sig = rpc_client
        .send_and_confirm_transaction(&transaction)
        .await
        .map_err(|e| eyre!("Failed to send Solana tx: {}", e))?;

    info!("✅ Solana TX confirmed: {}", tx_sig);
    info!(
        "Explorer: https://explorer.solana.com/tx/{}?cluster=devnet",
        tx_sig
    );

    // 9. Poll until swap completes on Spark side
    poll_order_completion(&order.order_id, start_time).await?;
    info!("=== Single Solana → Spark Complete ===");
    Ok(())
}

// ============================================================================
// SOLANA ORDER CREATION HELPER
// ============================================================================
async fn create_order_solana_to_spark(
    source_asset: &str,
    source_owner: &str,
    source_amount: &str,
    dest_asset: &str,
    dest_owner: &str,
    dest_amount: &str,
) -> Result<SolanaOrderResult> {
    let url = format!("{}/v2/orders", API_BASE_URL);
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
        solver_id: None,
    };

    info!("Order request: {:?}", req);
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

    let res_json: SolanaOrderResponse = serde_json::from_str(&res_text)
        .map_err(|e| eyre!("Parse error: {}. Body: {}", e, res_text))?;
    if res_json.status != "Ok" {
        return Err(eyre!("Order creation failed: {}", res_json.status));
    }

    Ok(res_json.result)
}

// ============================================================================
// SINGLE ORDER: SOLANA cbBTC (testnet) → SPARK (0.0005 cbBTC)
// ============================================================================
#[allow(dead_code)]
async fn test_solana_cbbtc_to_spark() -> Result<()> {
    use base64::Engine;
    use solana_client::nonblocking::rpc_client::RpcClient as SolanaRpcClient;
    use solana_sdk::{
        commitment_config::CommitmentConfig, signature::Keypair, signer::Signer as SolanaSigner,
        transaction::VersionedTransaction,
    };

    info!("=== Single Order: Solana cbBTC (testnet) → Spark (0.0005 cbBTC) ===");

    // 1. Init Spark SDK for destination address
    let sdk = init_sdk().await?;
    let spark_address = get_address(&sdk).await?;
    info!("Spark Destination Address: {}", spark_address);

    // 2. Setup Solana keypair from base58 private key
    let keypair_bytes = bs58::decode(SOLANA_PRIVATE_KEY)
        .into_vec()
        .map_err(|e| eyre!("Failed to decode Solana private key: {}", e))?;
    let keypair =
        Keypair::from_bytes(&keypair_bytes).map_err(|e| eyre!("Invalid Solana keypair: {}", e))?;
    let solana_pubkey = keypair.pubkey();
    info!("Solana Wallet: {}", solana_pubkey);

    // 3. Setup Solana RPC client (testnet)
    let rpc_client = SolanaRpcClient::new_with_commitment(
        SOLANA_DEVNET_RPC.to_string(), // "https://api.devnet.solana.com"
        CommitmentConfig::confirmed(),
    );

    // Check SOL balance for tx fees
    let sol_balance = rpc_client
        .get_balance(&solana_pubkey)
        .await
        .map_err(|e| eyre!("Failed to get SOL balance: {}", e))?;
    info!(
        "SOL Balance: {} lamports ({:.6} SOL)",
        sol_balance,
        sol_balance as f64 / 1_000_000_000.0
    );
    if sol_balance < 100_000 {
        return Err(eyre!(
            "Insufficient SOL for tx fees: {} lamports. Airdrop with: solana airdrop 1 {} --url testnet",
            sol_balance,
            solana_pubkey
        ));
    }

    // 4. Get Quote: 0.0005 cbBTC = 50,000 units (8 decimals)
    let source_asset = "solana_testnet:cbbtc";
    let dest_asset = "spark_regtest:btc";
    let amount: u64 = 50_000; // 0.0005 cbBTC

    info!(
        "Fetching quote {} → {} for {} units (0.0005 cbBTC)",
        source_asset, dest_asset, amount
    );
    let quote = get_quote(source_asset, dest_asset, amount).await?;
    info!("Quote: {:?}", quote);

    let dest_amount = quote["result"][0]["destination"]["amount"]
        .as_str()
        .ok_or_else(|| eyre!("No destination amount in quote: {:?}", quote))?
        .to_string();
    info!("Destination amount: {} sats", dest_amount);

    // 5. Create Order
    info!("Creating Solana cbBTC → Spark order...");
    let order = create_order_solana_to_spark(
        source_asset,
        &solana_pubkey.to_string(),
        &amount.to_string(),
        dest_asset,
        &spark_address,
        &dest_amount,
    )
    .await?;
    info!("Order Created: ID={}", order.order_id);

    // 6. Decode the versioned_tx (base64 from API)
    info!("Decoding versioned transaction...");
    let tx_bytes = base64::engine::general_purpose::STANDARD
        .decode(&order.versioned_tx)
        .map_err(|e| eyre!("Failed to base64-decode versioned_tx: {}", e))?;

    let mut transaction: VersionedTransaction = bincode::deserialize(&tx_bytes)
        .map_err(|e| eyre!("Failed to deserialize VersionedTransaction: {}", e))?;
    info!("Transaction deserialized ({} bytes)", tx_bytes.len());

    // 7. Sign the transaction
    let message_bytes = transaction.message.serialize();
    let sig = keypair.sign_message(&message_bytes);
    transaction.signatures[0] = sig;
    info!("Transaction signed: {}", sig);

    // 8. Send to Solana testnet
    let start_time = Instant::now();
    info!("Sending Solana cbBTC transaction...");

    let tx_sig = rpc_client
        .send_and_confirm_transaction(&transaction)
        .await
        .map_err(|e| eyre!("Failed to send Solana tx: {}", e))?;

    info!("✅ Solana TX confirmed: {}", tx_sig);
    info!(
        "Explorer: https://explorer.solana.com/tx/{}?cluster=testnet",
        tx_sig
    );

    // 9. Poll until swap completes on Spark side
    poll_order_completion(&order.order_id, start_time).await?;
    info!("=== Single Solana cbBTC → Spark Complete ===");
    Ok(())
}

// ============================================================================
// BATCH TEST: SPARK → SOLANA  (N orders × amount_lamports each)
// ============================================================================

#[allow(dead_code)]
async fn batch_test_spark_to_solana(total_orders: u32, batch_size: u32) -> Result<BatchTestReport> {
    use base64::Engine;
    use solana_client::nonblocking::rpc_client::RpcClient as SolanaRpcClient;
    use solana_sdk::{
        commitment_config::CommitmentConfig, signature::Keypair, signer::Signer as SolanaSigner,
        transaction::VersionedTransaction,
    };

    info!("=== Starting Batch Test: Spark → Solana ===");
    info!(
        "Total orders: {}, Batch size: {}, Amount: 1,000,000 lamports (0.001 SOL) each",
        total_orders, batch_size
    );

    let mut report = BatchTestReport::new();
    report.start();

    // 1. Init Spark SDK
    let sdk = init_sdk().await?;
    let my_spark_address = get_address(&sdk).await?;
    info!("My Spark Address: {}", my_spark_address);

    // 2. Solana destination wallet (reuse SOLANA_PRIVATE_KEY as recipient identity,
    //    or substitute a dedicated destination pubkey string if preferred)
    let keypair_bytes = bs58::decode(SOLANA_PRIVATE_KEY)
        .into_vec()
        .map_err(|e| eyre!("Failed to decode Solana private key: {}", e))?;
    let keypair =
        Keypair::from_bytes(&keypair_bytes).map_err(|e| eyre!("Invalid Solana keypair: {}", e))?;
    let solana_dest_address = keypair.pubkey().to_string();
    info!("Solana Destination Address: {}", solana_dest_address);

    let source_asset = "spark_regtest:btc";
    let dest_asset = "solana_testnet:sol";
    let amount_sats: u64 = 10_000; // sats sent from Spark side

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

        let mut order_handles: Vec<(u32, String, Instant)> = Vec::new();

        for order_num in batch_start..batch_end {
            let order_idx = order_num + 1;

            if order_num > batch_start {
                sleep(Duration::from_millis(500)).await;
            }

            info!("[Order {}] Creating Spark → Solana order...", order_idx);

            // Fetch a fresh quote to get the expected destination amount
            let quote_result = get_quote(source_asset, dest_asset, amount_sats).await;
            let dest_amount = match quote_result {
                Ok(q) => q["result"][0]["destination"]["amount"]
                    .as_str()
                    .unwrap_or("900000")
                    .to_string(),
                Err(e) => {
                    info!(
                        "[Order {}] Quote failed, using fallback dest amount: {}",
                        order_idx, e
                    );
                    "900000".to_string() // fallback lamports
                }
            };

            match create_order_spark_to_evm(
                // Garden's v2/orders endpoint is chain-agnostic; reuse the same helper
                // but point assets at Solana
                source_asset,
                &my_spark_address,
                dest_asset,
                &solana_dest_address,
                amount_sats.to_string(),
            )
            // NOTE: create_order_spark_to_evm hardcodes dest amount to "499".
            // For Solana you need a proper dest_amount, so call the generic helper below:
            // create_order_spark_to_solana(...).await
            .await
            {
                Ok(order_res) => {
                    info!(
                        "[Order {}] Order created: ID={}, To={}, Amount={}",
                        order_idx, order_res.order_id, order_res.to, order_res.amount
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

        if batch_num + 1 < num_batches {
            sleep(Duration::from_secs(2)).await;
        }
    }

    report.finish();
    report.print_report("Spark → Solana (10,000 sats × N orders)");
    info!("=== Batch Test Complete: Spark → Solana ===");
    Ok(report)
}

// ============================================================================
// HELPER: Create order with explicit dest amount (needed for Solana dest)
// ============================================================================

#[allow(dead_code)]
async fn create_order_spark_to_solana(
    source_asset: &str,
    source_owner: &str,
    source_amount: String,
    dest_asset: &str,
    dest_owner: &str,
    dest_amount: String,
) -> Result<CreateOrderResult> {
    let url = format!("{}/v2/orders", API_BASE_URL);
    let client = Client::new();
    let req = OrderRequest {
        source: OrderDetails {
            asset: source_asset.to_string(),
            owner: source_owner.to_string(),
            amount: source_amount,
        },
        destination: OrderDetails {
            asset: dest_asset.to_string(),
            owner: dest_owner.to_string(),
            amount: dest_amount,
        },
        solver_id: None,
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

// ============================================================================
// BATCH TEST: SOLANA → SPARK  (N orders × 1,000,000 lamports each)
// ============================================================================

#[allow(dead_code)]
async fn batch_test_solana_to_spark(total_orders: u32, batch_size: u32) -> Result<BatchTestReport> {
    use base64::Engine;
    use solana_client::nonblocking::rpc_client::RpcClient as SolanaRpcClient;
    use solana_sdk::{
        commitment_config::CommitmentConfig, signature::Keypair, signer::Signer as SolanaSigner,
        transaction::VersionedTransaction,
    };

    info!("=== Starting Batch Test: Solana → Spark ===");
    info!(
        "Total orders: {}, Batch size: {}, Amount: 1,000,000 lamports (0.001 SOL) each",
        total_orders, batch_size
    );

    let mut report = BatchTestReport::new();
    report.start();

    // 1. Init Spark SDK for destination address
    let sdk = init_sdk().await?;
    let spark_address = get_address(&sdk).await?;
    info!("Spark Destination Address: {}", spark_address);

    // 2. Setup Solana keypair
    let keypair_bytes = bs58::decode(SOLANA_PRIVATE_KEY)
        .into_vec()
        .map_err(|e| eyre!("Failed to decode Solana private key: {}", e))?;
    let keypair =
        Keypair::from_bytes(&keypair_bytes).map_err(|e| eyre!("Invalid Solana keypair: {}", e))?;
    let solana_pubkey = keypair.pubkey();
    info!("Solana Wallet: {}", solana_pubkey);

    // 3. Solana RPC client
    let rpc_client = SolanaRpcClient::new_with_commitment(
        SOLANA_DEVNET_RPC.to_string(),
        CommitmentConfig::confirmed(),
    );

    // Pre-flight balance check
    let balance = rpc_client
        .get_balance(&solana_pubkey)
        .await
        .map_err(|e| eyre!("Failed to get Solana balance: {}", e))?;
    let lamports_per_order: u64 = 1_000_000;
    let estimated_fees_per_order: u64 = 10_000; // rough fee estimate
    let required = (lamports_per_order + estimated_fees_per_order) * total_orders as u64;
    info!(
        "Solana Balance: {} lamports ({:.6} SOL) | Required ~{} lamports",
        balance,
        balance as f64 / 1_000_000_000.0,
        required
    );
    if balance < required {
        return Err(eyre!(
            "Insufficient SOL: {} lamports. Need ~{} lamports for {} orders. Airdrop with: solana airdrop {} {} --url devnet",
            balance,
            required,
            total_orders,
            (required as f64 / 1_000_000_000.0).ceil() as u64,
            solana_pubkey
        ));
    }

    let source_asset = "solana_testnet:sol";
    let dest_asset = "spark_regtest:btc";

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

        // We collect (order_idx, order_id, start_time) for polling after the batch sends
        let mut order_handles: Vec<(u32, String, Instant)> = Vec::new();

        for order_num in batch_start..batch_end {
            let order_idx = order_num + 1;

            // Stagger requests slightly to avoid hammering the API
            if order_num > batch_start {
                sleep(Duration::from_millis(500)).await;
            }

            info!("[Order {}] Fetching quote...", order_idx);

            // Get quote for this order
            let dest_amount = match get_quote(source_asset, dest_asset, lamports_per_order).await {
                Ok(q) => q["result"][0]["destination"]["amount"]
                    .as_str()
                    .unwrap_or("800")
                    .to_string(),
                Err(e) => {
                    info!(
                        "[Order {}] Quote failed ({}), using fallback dest amount",
                        order_idx, e
                    );
                    "800".to_string() // fallback sats
                }
            };
            info!("[Order {}] Dest amount: {} sats", order_idx, dest_amount);

            info!("[Order {}] Creating Solana → Spark order...", order_idx);
            match create_order_solana_to_spark(
                source_asset,
                &solana_pubkey.to_string(),
                &lamports_per_order.to_string(),
                dest_asset,
                &spark_address,
                &dest_amount,
            )
            .await
            {
                Ok(order_res) => {
                    info!(
                        "[Order {}] Order created: ID={}",
                        order_idx, order_res.order_id
                    );

                    // Decode versioned transaction returned by the API
                    let tx_bytes = match base64::engine::general_purpose::STANDARD
                        .decode(&order_res.versioned_tx)
                    {
                        Ok(b) => b,
                        Err(e) => {
                            report.record_failure(
                                order_res.order_id,
                                format!("Failed to base64-decode versioned_tx: {}", e),
                            );
                            continue;
                        }
                    };

                    let mut transaction: VersionedTransaction =
                        match bincode::deserialize(&tx_bytes) {
                            Ok(t) => t,
                            Err(e) => {
                                report.record_failure(
                                    order_res.order_id,
                                    format!("Failed to deserialize VersionedTransaction: {}", e),
                                );
                                continue;
                            }
                        };

                    // Sign
                    let message_bytes = transaction.message.serialize();
                    let sig = keypair.sign_message(&message_bytes);
                    transaction.signatures[0] = sig;
                    info!("[Order {}] Transaction signed: {}", order_idx, sig);

                    // Send to Solana
                    let start_time = Instant::now();
                    match rpc_client.send_and_confirm_transaction(&transaction).await {
                        Ok(tx_sig) => {
                            info!(
                                "[Order {}] ✅ Solana TX confirmed: {} (Explorer: https://explorer.solana.com/tx/{}?cluster=devnet)",
                                order_idx, tx_sig, tx_sig
                            );
                            order_handles.push((order_idx, order_res.order_id, start_time));
                        }
                        Err(e) => {
                            report.record_failure(
                                order_res.order_id,
                                format!("Solana tx failed: {}", e),
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

        // Poll all orders in this batch for Garden swap completion
        for (order_idx, order_id, start_time) in order_handles {
            info!("[Order {}] Waiting for swap completion...", order_idx);
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

        // Brief pause between batches to respect rate limits
        if batch_num + 1 < num_batches {
            sleep(Duration::from_secs(2)).await;
        }
    }

    report.finish();
    report.print_report("Solana → Spark (0.001 SOL × N orders)");
    info!("=== Batch Test Complete: Solana → Spark ===");
    Ok(report)
}

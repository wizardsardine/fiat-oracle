const SAMPLE_INTERVAL_SECS: u64 = 64;
const HEALTHZ_TIMEOUT_SECS: u64 = 30;
const SYNC_TIMEOUT_SECS: u64 = 2 * 3600;
const SYNC_POLL_INTERVAL_SECS: u64 = 10;
// Only currencies with ≥ MIN_PRICE_SOURCES=3 historical sources can produce
// a valid median; the production binary refuses to start otherwise. Today
// that means USD, EUR, GBP. Excluded:
// - CAD: zero working historical sources (Coinbase Exchange has no BTC-CAD).
// - JPY: only 2 sources (Binance + Bitbank) after Bitfinex/BitMEX/Coinbase
//        JPY were dropped as dead/delisted.
// - KRW: only 1 source (Upbit) after Bithumb was dropped (its 1h-synth path
//        couldn't be reproduced by the live 1m path).
const ALL_CURRENCIES: &[&str] = &["USD", "EUR", "GBP"];
// All exchange sources (historical + spot), sorted for deadlock-free acquisition.
const ALL_LOCK_NAMES: &[&str] = &[
    "binance", "bitbank", "bitfinex", "bitmex", "bitstamp", "coinbase", "kucoin", "okx", "upbit",
];

/// Acquire an exclusive `flock(2)` per source. Mirrors the helper in
/// `src/fetcher/historical/mod.rs` — same lock files, same semantics.
fn exchange_locks(names: &[&str]) -> Vec<std::fs::File> {
    use std::os::unix::io::AsRawFd;
    let guards: Vec<std::fs::File> = names
        .iter()
        .map(|name| {
            let path = format!("/tmp/fiat_oracle_test_{name}.lock");
            let file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&path)
                .unwrap();
            let ret = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
            assert_eq!(
                ret,
                0,
                "flock({path}) failed: {}",
                std::io::Error::last_os_error()
            );
            file
        })
        .collect();
    // Allow the previous lock-holder's in-flight exchange requests to clear
    // any rate-limit windows before we start making requests.
    std::thread::sleep(std::time::Duration::from_secs(3));
    guards
}

struct OracleProcess {
    child: std::process::Child,
    _dir: tempfile::TempDir,
}

impl Drop for OracleProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn write_config(dir: &std::path::Path, port: u16, currencies: &[&str]) -> std::path::PathBuf {
    let data_dir = dir.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let c_list = currencies
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let toml = format!(
        "data_dir = {:?}\nbind_addr = \"127.0.0.1:{}\"\ncurrencies = [{}]\n",
        data_dir.to_str().unwrap(),
        port,
        c_list
    );
    let p = dir.join("config.toml");
    std::fs::write(&p, toml).unwrap();
    p
}

fn spawn_oracle(config_path: &std::path::Path, rescan_from: u64) -> std::process::Child {
    let candles_per_sec = std::env::var("FIAT_ORACLE_TEST_CANDLES_PER_SEC")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(2880);
    std::process::Command::new(env!("CARGO_BIN_EXE_fiat-oracle"))
        .arg("--config")
        .arg(config_path)
        .arg("--rescan-from")
        .arg(rescan_from.to_string())
        .arg("--target-candles-per-sec")
        .arg(candles_per_sec.to_string())
        .env("RUST_LOG", "fiat_oracle=info")
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .expect("failed to spawn fiat-oracle")
}

async fn wait_for_healthz(client: &reqwest::Client, base_url: &str, timeout_secs: u64) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "timeout waiting for /healthz"
        );
        match client.get(format!("{base_url}/healthz")).send().await {
            Ok(r) if r.status().is_success() => return,
            _ => tokio::time::sleep(std::time::Duration::from_millis(200)).await,
        }
    }
}

async fn poll_until_serving(
    client: &reqwest::Client,
    base_url: &str,
    timeout_secs: u64,
) -> serde_json::Value {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "timeout waiting for state=serving"
        );
        let resp: serde_json::Value = client
            .get(format!("{base_url}/info"))
            .send()
            .await
            .expect("GET /info failed")
            .json()
            .await
            .expect("/info json parse failed");
        if resp["state"] == "serving" {
            return resp;
        }
        let eta = resp["eta_secs"]
            .as_u64()
            .map(|s| format!("ETA {s}s"))
            .unwrap_or_default();
        eprintln!("[e2e] syncing... {eta}");
        tokio::time::sleep(std::time::Duration::from_secs(SYNC_POLL_INTERVAL_SECS)).await;
    }
}

async fn assert_no_data_gaps(
    client: &reqwest::Client,
    base_url: &str,
    currencies: &[&str],
    from_ts: u64,
    to_ts: u64,
) {
    // Query at 64s resolution — 48h = ~2700 timestamps, well under the 10k cap.
    // "no_data"    → gap > ~1h (57 * 64s walk window): hard failure.
    // "fetch_error"→ exchange outage marker stored; walk covers adjacent slots: allowed.
    let timestamps: Vec<u64> = (from_ts..=to_ts)
        .step_by(SAMPLE_INTERVAL_SECS as usize)
        .collect();

    for currency in currencies {
        let body = serde_json::json!({
            "currency": currency,
            "timestamps": timestamps,
        });
        let resp: serde_json::Value = client
            .post(format!("{base_url}/price"))
            .json(&body)
            .send()
            .await
            .expect("POST /price failed")
            .json()
            .await
            .expect("/price json parse failed");

        let prices = resp["prices"].as_array().expect("prices must be an array");
        let mut no_data_count = 0usize;
        let mut no_data_first: Option<u64> = None;

        for (i, p) in prices.iter().enumerate() {
            assert_ne!(
                p, "unsupported",
                "currency {currency} reported unsupported — config bug"
            );
            if p == "no_data" {
                no_data_count += 1;
                no_data_first.get_or_insert(timestamps[i]);
            }
        }

        assert_eq!(
            no_data_count, 0,
            "{currency}: {no_data_count} no_data gaps (first at ts={no_data_first:?}). \
             no_data means a gap >~1h with zero exchange data in the DB."
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn backfill_48h_no_gaps() {
    let _eg = exchange_locks(ALL_LOCK_NAMES);

    let now_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    // Align to 64s grid so the first queried timestamp maps to a real sample slot.
    let from_ts = (now_ts - 48 * 3600) / SAMPLE_INTERVAL_SECS * SAMPLE_INTERVAL_SECS;

    let tmp = tempfile::tempdir().unwrap();
    let port = free_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let cfg_path = write_config(tmp.path(), port, ALL_CURRENCIES);

    let child = spawn_oracle(&cfg_path, from_ts);
    let _guard = OracleProcess { child, _dir: tmp };

    let client = reqwest::Client::new();

    // Step 1: wait for the HTTP server to be up.
    wait_for_healthz(&client, &base_url, HEALTHZ_TIMEOUT_SECS).await;

    // Step 2: confirm the service is syncing (Status::Sync is set before the server
    // starts listening, so this is guaranteed to be true immediately after healthz).
    let info: serde_json::Value = client
        .get(format!("{base_url}/info"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        info["state"], "fetching_historical",
        "/info right after startup with --rescan-from must be fetching_historical"
    );

    // Step 3: wait for all backfill to complete.
    let sync_timeout = std::env::var("FIAT_ORACLE_TEST_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(SYNC_TIMEOUT_SECS);
    let final_info = poll_until_serving(&client, &base_url, sync_timeout).await;

    // Step 4: history_starts must cover the requested range.
    let history_starts = final_info["history_starts"]
        .as_u64()
        .expect("history_starts must be set after serving");
    assert!(
        history_starts <= from_ts,
        "history_starts={history_starts} must be <= from_ts={from_ts}"
    );

    // Step 5: verify no gaps across the full 48h window for every currency.
    assert_no_data_gaps(&client, &base_url, ALL_CURRENCIES, from_ts, now_ts).await;
}

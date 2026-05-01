use std::path::{Path, PathBuf};

use clap::Parser;
use serde::Deserialize;

use crate::{currency::Currency, error::Result};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub data_dir: PathBuf,
    pub bind_addr: String,
    pub currencies: Vec<Currency>,
}

impl Config {
    pub fn from_file(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)?;
        let cfg: Config = toml::from_str(&text)?;
        Ok(cfg)
    }
}

pub fn load_config(path: &Path) -> Result<Config> {
    Config::from_file(path)
}

#[derive(Parser, Debug)]
#[command(version, about = "BTC historical fiat-price oracle")]
pub struct Args {
    /// Path to TOML config file
    #[arg(long, short)]
    pub config: PathBuf,

    /// Backfill historical prices from this unix timestamp before serving
    #[arg(long)]
    pub rescan_from: Option<u64>,

    /// Optional override of the operating throughput, in candles per second.
    /// When omitted, every backfill derives its TARGET per-currency from the
    /// `min` of `source.max_candles_per_sec(currency)` across the sources
    /// supporting that currency. Pass an explicit value to throttle (lower)
    /// or push past (higher) that automatic ceiling.
    #[arg(long)]
    pub target_candles_per_sec: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_toml() {
        let toml = r#"
data_dir = "/var/lib/fiat-oracle"
bind_addr = "127.0.0.1:8080"
currencies = ["EUR", "USD"]
"#;
        let cfg: Config = toml::from_str(toml).unwrap();
        assert_eq!(cfg.bind_addr, "127.0.0.1:8080");
        assert_eq!(cfg.data_dir, PathBuf::from("/var/lib/fiat-oracle"));
        assert_eq!(cfg.currencies[0], Currency::Eur);
        assert_eq!(cfg.currencies[1], Currency::Usd);
    }

    #[test]
    fn cli_rescan_from_optional() {
        let cli = Args::try_parse_from(["fiat-oracle", "--config", "/tmp/c.toml"]).unwrap();
        assert!(cli.rescan_from.is_none());

        let cli = Args::try_parse_from([
            "fiat-oracle",
            "--config",
            "/tmp/c.toml",
            "--rescan-from",
            "1700000000",
        ])
        .unwrap();
        assert_eq!(cli.rescan_from, Some(1700000000));
    }
}

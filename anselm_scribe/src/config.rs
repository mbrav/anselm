use clap::{arg, ArgAction, Parser};

/// Anselm Scribe - Stock trading system with a proof for existence of Truth
#[derive(Parser, Clone, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Config {
    /// Specify date starting from which market data will be downloaded
    #[arg(short = 's', long, env = "MD_DATE_START", default_value = "2024-02-01")]
    pub md_date_start: String,

    /// Specify number of days from Start Date
    #[arg(short = 'd', long, env = "MD_DAYS", default_value_t = 30)]
    pub md_days: i64,

    /// Specify empty market data threshold in days after which market data gathering for a given
    /// security will be skipped
    #[arg(long, env = "MD_DAY_THRESHOLD", default_value_t = 14)]
    pub md_day_threshold: i16,

    /// Specify whether to gather md going backwards in time
    #[arg(short = 'r', long, env = "MD_REVERSE", action=ArgAction::SetTrue)]
    pub md_reverse: bool,

    /// Specify specific security to gather market data from
    #[arg(short = 'c', long, env = "MD_SECURITIES", action=ArgAction::Append)]
    pub md_securities: Vec<String>,

    /// Specify whether to save market data to disk as json files instead of db
    #[arg(long, env = "MD_DISK", action=ArgAction::SetTrue)]
    pub md_disk: bool,

    /// Specify path to which market data file will be written
    #[arg(short = 'p', long, env = "MD_PATH", default_value = "./")]
    pub md_path: String,

    /// Specify market data interval to fetch in minutes
    #[arg(short = 'i', long, env = "MD_INTERVAL", default_value_t = 1)]
    pub md_interval: i16,

    /// Specify Clickhouse URL
    #[arg(long, env = "CH_URL", default_value = "http://localhost:8123")]
    pub ch_url: String,

    /// Specify Clickouse user
    #[arg(long, env = "CH_USER", default_value = "default")]
    pub ch_user: String,

    /// Specify Clickouse password
    #[arg(long, env = "CH_PASS", default_value = "")]
    pub ch_password: String,

    /// Specify Clickouse database
    #[arg(long, env = "CH_DB", default_value = "md_moex")]
    pub ch_db: String,

    /// Specify number of threads to uses, 0 will use all available cores
    #[arg(short, long, env = "MD_THREADS", default_value_t = 1)]
    pub threads: usize,
}

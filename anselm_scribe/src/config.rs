//use clap::{arg, ArgAction, Parser};
use clap::{arg, Parser};

/// Anselm Scribe - Stock trading system with a proof for existence of Truth
#[derive(Parser, Clone, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Config {
    /// Specify date starting from which market data will be downloaded
    #[arg(
        short = 's',
        long,
        value_name = "DATE",
        env = "MD_DATE_START",
        default_value = "2024-01-01"
    )]
    pub date_start: String,

    /// Specify number of days from Start Date
    #[arg(short, long, value_name = "INT", env = "MD_DAYS", default_value_t = 30)]
    pub days: i64,

    /// Specify market data interval to fetch in minutes
    #[arg(
        short,
        long,
        value_name = "MINUTES",
        env = "MD_INTERVAL",
        default_value_t = 1
    )]
    pub interval: i16,

    /// Specify market data interval in minutes
    #[arg(
        short,
        long,
        value_name = "INT",
        env = "MD_THREADS",
        default_value_t = 1
    )]
    pub threads: usize,

    /// Specify path to which market data file will be written
    #[arg(
        short,
        long,
        value_name = "PATH",
        env = "MD_PATH",
        default_value = "./"
    )]
    pub md_path: String,

    /// Specify Clickhouse URL
    #[arg(
        short = 'u',
        long,
        env = "CH_URL",
        default_value = "http://localhost:8123"
    )]
    pub ch_url: String,

    /// Specify Clickouse user
    #[arg(short = 'r', long, env = "CH_USER", default_value = "default")]
    pub ch_user: String,

    /// Specify Clickouse password
    #[arg(short = 'p', long, env = "CH_PASS", default_value = "")]
    pub ch_password: String,

    /// Specify Clickouse database
    #[arg(short = 'b', long, env = "CH_DB", default_value = "md_moex")]
    pub ch_db: String,
}

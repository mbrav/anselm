use clap::{arg, ArgAction, Parser};

/// Anselm Scribe - Stock trading system with a proof for existence of Truth
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Config {
    /// Specify date starting from which market data will be downloaded
    #[arg(
        short,
        long,
        value_name = "DATE",
        env = "MD_DATE_START",
        default_value = "2024-01-01"
    )]
    pub date_start: String,

    /// Specify number of days from Start Date
    #[arg(
        short = 'y',
        long,
        value_name = "INT",
        env = "MD_DAYS",
        default_value_t = 30
    )]
    pub days: i64,

    /// Specify market data interval in minutes
    #[arg(
        short,
        long,
        value_name = "MINUTES",
        env = "MD_INTERVAL",
        default_value_t = 1
    )]
    pub interval: i16,

    /// Specify path to which market data file will be written
    #[arg(
        short,
        long,
        value_name = "PATH",
        env = "MD_PATH",
        default_value = "./"
    )]
    pub md_path: String,
}

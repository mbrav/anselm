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
        env = "DATE_START",
        default_value = "2020-01-01"
    )]
    pub date_start: String,

    /// Specify number of days
    #[arg(
        short = 'y',
        long,
        value_name = "INT",
        env = "DAYS",
        default_value_t = 30
    )]
    pub days: i64,
}

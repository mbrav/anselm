use clap::{arg, Parser, Subcommand};

use crate::{agent, history, live};

/// Anselm Scribe - Stock trading system with a proof for existence of Truth
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct GlobalConfig {
    #[command(subcommand)]
    pub command: Commands,
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

/// A collection of crazy CLI tools in Rust
#[derive(Subcommand)]
pub enum Commands {
    Agent(agent::config::Config),
    History(history::config::Config),
    Live(live::config::Config),
}

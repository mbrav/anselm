use clap::{arg, Args};

/// Run anselm_scribe in history market data gathering mode
#[derive(Args)]
pub struct Config {
    /// Specify and option
    #[arg(short = 'p', long = "option", env = "md_option", default_value = "800")]
    pub prefix: String,
}

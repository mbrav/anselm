use anselm_scribe::config::Config;
use anselm_scribe::runners;

use clap::Parser;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conf = Config::parse();

    if conf.threads > 1 {
        runners::base_runner(&conf).await?;
    } else {
        runners::base_runner(&conf).await?;
    }

    Ok(())
}

use super::config::Config;
use crate::config::GlobalConfig;
use crate::db;
use crate::runners;

use clap::Parser;

#[tokio::main]
async fn main(global_conf: GlobalConfig) -> Result<(), Box<dyn std::error::Error>> {
    // Load config from CLI arguments and env variables
    let conf = Config::parse();

    // Initialize database connection and schema market data to disk is false
    let db = if conf.md_disk {
        None
    } else {
        Some(db::ClickhouseDatabase::new(&conf))
    };

    // Init db if defined
    if let Some(ref db) = db {
        db.init().await?;
    }

    // Execute runners
    if global_conf.threads > 1 {
        //runners::parallel_runner(&conf).await?;
        runners::base_runner(&conf, &db).await?;
    } else {
        runners::base_runner(&conf, &db).await?;
    }

    Ok(())
}

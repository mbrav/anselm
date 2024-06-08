use anselm_scribe::config::Config;
use anselm_scribe::db;
use anselm_scribe::metrics;
use anselm_scribe::runners;

use clap::Parser;
use tokio::try_join;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load config from CLI arguments and env variables
    let conf = Config::parse();

    // Create a metrics server handler if not disabled
    let metrics_server = if conf.metrics_disable {
        None
    } else {
        Some(metrics::new_task(conf.metrics_port).await)
    };

    // Initialize database connection if defined
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
    let runners_task = tokio::spawn(async move {
        if conf.threads > 1 {
            // TODO: Implement parallel runner
            runners::base_runner(&conf, &db).await.unwrap();
        } else {
            runners::base_runner(&conf, &db).await.unwrap();
        }
    });

    // Await both the metrics server and the runners task
    if let Some(metrics_server) = metrics_server {
        try_join!(metrics_server, runners_task)?;
    } else {
        // Run only the runner when metrics are disabled
        runners_task.await?;
    }

    Ok(())
}

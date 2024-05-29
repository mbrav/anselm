use anselm_scribe::config::Config;
use anselm_scribe::models::get_all_securities;
use chrono::{Duration, NaiveDate};
use clap::Parser;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conf = Config::parse();
    let mut securities = get_all_securities().await?;

    let date = NaiveDate::parse_from_str(conf.date_start.as_str(), "%Y-%m-%d")
        .expect("Error parsing date");

    for sec in &mut securities {
        for n in 0..conf.days {
            let new_date = (date + Duration::days(n)).to_string();
            sec.fetch_candles(1, new_date).await?;
        }
    }

    Ok(())
}

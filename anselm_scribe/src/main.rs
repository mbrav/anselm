use anselm_scribe::config::Config;
use anselm_scribe::models::get_all_securities;
use chrono::{Duration, NaiveDate};
use clap::Parser;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conf = Config::parse();
    let mut securities = get_all_securities().await?;

    let date = NaiveDate::parse_from_str(conf.date_start.as_str(), "%Y-%m-%d")?;

    for sec in &mut securities {
        for n in 0..conf.days {
            let date_start = (date + Duration::days(n)).to_string();
            let date_end = (date + Duration::days(n + 1)).to_string();
            let file_path = format!("{}/{}-{}.json", &conf.md_path, &sec.secid, &date_start);
            sec.fetch_candles(conf.interval, &date_start, &date_end)
                .await?;
            sec.save_candles_to_file(&file_path).await?;
        }
    }

    Ok(())
}

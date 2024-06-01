use crate::config::Config;
use crate::models::{CandleRecord, Security};
use chrono::{Duration, NaiveDate};
use std::collections::HashMap;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::task;

/// Base runner for running on a single thread
pub async fn base_runner(conf: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let securities = get_all_securities().await?;

    let date = NaiveDate::parse_from_str(conf.date_start.as_str(), "%Y-%m-%d")?;

    for sec in &securities {
        for n in 0..conf.days {
            let date_start = (date + Duration::days(n)).to_string();
            let date_end = (date + Duration::days(n + 1)).to_string();
            let file_path = format!("{}/{}-{}.json", &conf.md_path, &sec.secid, &date_start);
            let candles = sec
                .fetch_candles(conf.interval, &date_start, &date_end)
                .await?;
            save_candles_to_file(candles, &file_path).await?;
        }
    }

    Ok(())
}

/// Parallel runner for running
///
/// ### Thread Options
///
/// If number of `threads` equals 0 then runner will use all available cores on system.
/// Otherwise it will will use the number of threads specified.
pub async fn parallel_runner(conf: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let securities = get_all_securities().await?;
    //
    //let date = NaiveDate::parse_from_str(conf.date_start.as_str(), "%Y-%m-%d")?;
    //
    //let mut tasks = vec![];
    //for sec in securities {
    //    let sec_clone = sec.clone(); // Clone the security to move into the async block
    //    let conf_clone = conf.clone(); // Clone the config to move into the async block
    //    let date_clone = date.clone(); // Clone the date to move into the async block
    //
    //    let task = task::spawn(async move {
    //        for n in 0..conf_clone.days {
    //            let date_start = (date_clone + Duration::days(n)).to_string();
    //            let date_end = (date_clone + Duration::days(n + 1)).to_string();
    //            let file_path = format!(
    //                "{}/{}-{}.json",
    //                &conf.md_path, &sec_clone.secid, &date_start
    //            );
    //
    //            let candles = sec_clone
    //                .fetch_candles(conf_clone.interval, &date_start, &date_end)
    //                .await
    //                .expect("Error");
    //
    //            save_candles_to_file(candles, &file_path).await;
    //        }
    //    });
    //
    //    tasks.push(task);
    //}
    //
    //// Await all tasks to ensure they complete before the program exits
    //for task in tasks {
    //    task.await?;
    //}
    //
    Ok(())
}

async fn get_all_securities() -> Result<Vec<Security>, Box<dyn std::error::Error>> {
    let url = "https://iss.moex.com/iss/engines/stock/markets/shares/securities.json";
    let resp = reqwest::get(url)
        .await?
        .json::<HashMap<String, serde_json::Value>>()
        .await?;

    let resp_iter = resp["securities"]["data"]
        .as_array()
        .expect("Error parsing securities data")
        .iter();
    let records: Vec<Security> = resp_iter
        .map(|x| Security {
            secid: x[0].as_str().unwrap().into(),
            boardid: x[1].as_str().unwrap().into(),
            shortname: x[2].as_str().unwrap().into(),
            status: x[6].as_str().unwrap().into(),
            marketcode: x[11].as_str().unwrap().into(),
        })
        .collect();
    println!("Got {} Securities", records.len());

    Ok(records)
}

/// Save candle records to a JSON file
async fn save_candles_to_file(
    candles: Vec<CandleRecord>,
    file_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = File::create(file_path).await?;
    let candles_json = serde_json::to_string(&candles)?;
    file.write_all(candles_json.as_bytes()).await?;
    println!("Candles saved to {}", file_path);

    Ok(())
}

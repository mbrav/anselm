use crate::config::Config;
use crate::db::ClickhouseDatabase;
use crate::models::{CandleRecord, Security};
use chrono::{Duration, NaiveDate};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::task;
//use tokio::task::JoinHandle;

/// Base runner for running on a single thread
pub async fn base_runner(
    conf: &Config,
    db: &ClickhouseDatabase,
) -> Result<(), Box<dyn std::error::Error>> {
    // Set date
    let date = NaiveDate::parse_from_str(conf.md_date_start.as_str(), "%Y-%m-%d")?;

    let securities = get_all_securities(db).await?;

    for sec in securities {
        for n in 0..conf.md_days {
            // Caclulate date start based on reverse parameter
            let date_start = if conf.md_reverse {
                // If reverse, subtract current day making date start less in reverse direction
                (date - Duration::days(n + 1)).to_string()
            } else {
                // Otherwise, make date start less than date end
                (date + Duration::days(n)).to_string()
            };
            // Caclulate date end based on reverse parameter
            let date_end = if conf.md_reverse {
                (date - Duration::days(n)).to_string()
            } else {
                (date + Duration::days(n + 1)).to_string()
            };
            //let date_start = (date + Duration::days(n)).to_string();
            //let date_end = (date + Duration::days(n + increment)).to_string();
            let file_path = format!("{}/{}-{}.json", &conf.md_path, &sec.secid, &date_start);
            let candles = sec
                .fetch_candles(conf.md_interval, &date_start, &date_end)
                .await?;
            save_candles_to_file(candles, &file_path).await?;
        }
    }

    Ok(())
}

/// Parallel runner for running
///
/// # Thread Options
///
/// If number of `threads` equals 0 then runner will use all available cores on system.
/// Otherwise it will will use the number of threads specified.
pub async fn parallel_runner(
    conf: &Config,
    db: &ClickhouseDatabase,
) -> Result<(), Box<dyn std::error::Error>> {
    // Set date
    let date = NaiveDate::parse_from_str(conf.md_date_start.as_str(), "%Y-%m-%d")?;

    // Set whether to go backwards in time to gather data
    let increment = if conf.md_reverse { -1 } else { 1 };

    let securities = get_all_securities(db).await?;

    // Clone the config and wrap it in an Arc to share among tasks
    let conf_arc = Arc::new(conf.clone());
    let mut tasks = vec![];

    for sec in securities {
        let sec_clone = sec.clone(); // Clone the security to move into the async block
        let conf_clone = Arc::clone(&conf_arc); // Clone the Arc to move into the async block
        let date_clone = date; // Copy the date to move into the async block

        let task = task::spawn(async move {
            for n in 0..conf_clone.md_days {
                let date_start = (date_clone + Duration::days(n)).to_string();
                let date_end = (date_clone + Duration::days(n + increment)).to_string();
                let file_path = format!(
                    "{}/{}-{}.json",
                    &conf_clone.md_path, &sec_clone.secid, &date_start
                );

                println!(
                    "Running Parallel runner for {} {}-{}",
                    sec_clone.secid, date_start, date_end
                );

                let candles = sec_clone
                    .fetch_candles(conf_clone.md_interval, &date_start, &date_end)
                    .await
                    .expect("Error fetching candles");

                save_candles_to_file(candles, &file_path)
                    .await
                    .expect("Error saving candles to file");
            }
        });

        tasks.push(task);
    }

    // Await all tasks to ensure they complete before the program exits
    for task in tasks {
        task.await?;
    }

    Ok(())
}

///// Parallel runner for running
/////
///// ### Thread Options
/////
///// If number of `threads` equals 0 then runner will use all available cores on system.
///// Otherwise it will will use the number of threads specified.
//pub async fn parallel_runner(conf: Config) -> Result<(), Box<dyn std::error::Error>> {
//    let date = NaiveDate::parse_from_str(&conf.date_start, "%Y-%m-%d")?;
//    let securities = get_all_securities().await?;
//
//    // Clone the config and wrap it in an Arc to share among tasks
//    let conf_arc = Arc::new(conf);
//    let mut tasks: Vec<JoinHandle<Result<(), Box<dyn std::error::Error>>>> = vec![];
//
//    for sec in securities {
//        let sec_clone = sec.clone(); // Clone the security to move into the async block
//        let conf_clone = Arc::clone(&conf_arc); // Clone the Arc to move into the async block
//        let date_clone = date; // Copy the date to move into the async block
//
//        let task = task::spawn(async move {
//            for n in 0..conf_clone.days {
//                let date_start = (date_clone + Duration::days(n)).to_string();
//                let date_end = (date_clone + Duration::days(n + 1)).to_string();
//                let file_path = format!(
//                    "{}/{}-{}.json",
//                    &conf_clone.md_path, &sec_clone.secid, &date_start
//                );
//
//                println!(
//                    "Running Parallel runner for {} {}-{}",
//                    sec_clone.secid, date_start, date_end
//                );
//
//                let candles = sec_clone
//                    .fetch_candles(conf_clone.interval, &date_start, &date_end)
//                    .await
//                    .expect("Error fetching candles");
//
//                save_candles_to_file(candles, &file_path)
//                    .await
//                    .expect("Error saving candles to file");
//            }
//            Ok(())
//        });
//
//        tasks.push(task);
//
//        // Process tasks in batches of 8
//        if tasks.len() == conf.threads {
//            for task in tasks.drain(..) {
//                task.await.expect("Error gathering tasks");
//            }
//        }
//    }
//
//    // Await remaining tasks
//    for task in tasks {
//        task.await?;
//    }
//
//    Ok(())
//}
//
async fn get_all_securities(
    db: &ClickhouseDatabase,
) -> Result<Vec<Security>, Box<dyn std::error::Error>> {
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

    for sec in &records {
        db.create_security(&sec).await?;
    }

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

use crate::config::Config;
use crate::db::ClickhouseDatabase;
use crate::models::{Board, CandleRecord, Security, Trade};
use chrono::{Duration, NaiveDate};
use std::collections::HashMap;
//use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
//use tokio::task;
//use tokio::task::JoinHandle;

/// Base runner for running on a single thread
pub async fn base_runner(
    conf: &Config,
    db: &Option<ClickhouseDatabase>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Set date
    let date = NaiveDate::parse_from_str(conf.md_date_start.as_str(), "%Y-%m-%d")?;

    let boards = get_all_boards(db).await?;

    // Save market data
    if let Some(db) = db {
        // Save candles array to db
        //for board in &boards {
        //    db.insert_board(board).await?;
        //    //db.insert_candle_batch(&candles).await?;
        //    println!("Inserted board {} ", board.boardid);
        //}
    } else {
        println!("No inserting into db");
    }

    //let securities = get_all_securities(db).await?;
    //
    //'outer: for sec in securities {
    //    let mut empty_day = 0;
    //    'inner: for n in 0..conf.md_days {
    //        // Calculate date start based on reverse parameter
    //        let date_start = if conf.md_reverse {
    //            // If reverse, subtract current day making date start less in reverse direction
    //            (date - Duration::days(n + 1)).to_string()
    //        } else {
    //            // Otherwise, make date start less than date end
    //            (date + Duration::days(n)).to_string()
    //        };
    //        // Calculate date end based on reverse parameter
    //        let date_end = if conf.md_reverse {
    //            (date - Duration::days(n)).to_string()
    //        } else {
    //            (date + Duration::days(n + 1)).to_string()
    //        };
    //
    //        // Fetch candles
    //        let candles = sec
    //            .fetch_candles(conf.md_interval, &date_start, &date_end)
    //            .await?;
    //
    //        // Check if candles where empty for curent day
    //        if candles.is_empty() {
    //            empty_day += 1;
    //            // If empty day count surpassed threshold then continue onto next security
    //            if empty_day > conf.md_day_threshold {
    //                continue 'outer;
    //            }
    //            // Otherwise don't save candles and continue onto next day
    //            continue 'inner;
    //        }
    //
    //        // Save market data
    //        if let Some(db) = db {
    //            // Save candles array to db
    //            for candle in &candles {
    //                db.insert_candle(candle).await?;
    //            }
    //            //db.insert_candle_batch(&candles).await?;
    //            println!(
    //                "Inserted {} candles for security {} in db",
    //                candles.len(),
    //                sec.secid
    //            );
    //        } else {
    //            // Otherwise Save market data as JSON to disk
    //            let file_path = format!("{}/{}-{}.json", &conf.md_path, &sec.secid, &date_start);
    //            save_candles_to_file(&file_path, &candles).await?;
    //            println!(
    //                "saved {} candles for security {} to {}",
    //                candles.len(),
    //                sec.secid,
    //                file_path
    //            );
    //        }
    //    }
    //}

    Ok(())
}

///// Parallel runner for running
/////
///// # Thread Options
/////
///// If number of `threads` equals 0 then runner will use all available cores on system.
///// Otherwise it will will use the number of threads specified.
//pub async fn parallel_runner(
//    conf: &Config,
//    db: &ClickhouseDatabase,
//
//) -> Result<(), Box<dyn std::error::Error>> {
//    // Set date
//    let date = NaiveDate::parse_from_str(conf.md_date_start.as_str(), "%Y-%m-%d")?;
//
//    // Clone the config and wrap it in an Arc to share among tasks
//    let conf_arc = Arc::new(conf.clone());
//
//    let securities = get_all_securities(db, conf_arc.md_disk.clone()).await?;
//    let mut tasks = vec![];
//
//    for sec in securities {
//        let sec_clone = sec.clone(); // Clone the security to move into the async block
//        let conf_clone = Arc::clone(&conf_arc); // Clone the Arc to move into the async block
//
//        let task = task::spawn(async move {
//            for n in 0..conf_clone.md_days {
//                // Caclulate date start based on reverse parameter
//                let date_start = if conf_clone.md_reverse {
//                    // If reverse, subtract current day making date start less in reverse direction
//                    (date - Duration::days(n + 1)).to_string()
//                } else {
//                    // Otherwise, make date start less than date end
//                    (date + Duration::days(n)).to_string()
//                };
//                // Caclulate date end based on reverse parameter
//                let date_end = if conf_clone.md_reverse {
//                    (date - Duration::days(n)).to_string()
//                } else {
//                    (date + Duration::days(n + 1)).to_string()
//                };
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
//                    .fetch_candles(conf_clone.md_interval, &date_start, &date_end)
//                    .await
//                    .expect("Error fetching candles");
//
//                // Save market data as JSON to disk if configured
//                if conf.md_disk {
//                    save_candles_to_file(&file_path, candles)
//                        .await
//                        .expect("Error saving candles to file");
//                } else {
//                    todo!("Candle to db implementing")
//                }
//            }
//        });
//
//        tasks.push(task);
//    }
//
//    // Await all tasks to ensure they complete before the program exits
//    for task in tasks {
//        task.await?;
//    }
//
//    Ok(())
//}
//
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

async fn get_all_boards(
    db: &Option<ClickhouseDatabase>,
) -> Result<Vec<Board>, Box<dyn std::error::Error>> {
    let engine = "stock"; // TODO Actualize variance
    let market = "shares"; // TODO Actualize variance
    let url = format!("https://iss.moex.com/iss/engines/{engine}/markets/{market}/boards.json");

    let resp = reqwest::get(url)
        .await?
        .json::<HashMap<String, serde_json::Value>>()
        .await?;

    let resp_iter = resp["boards"]["data"]
        .as_array()
        .expect("Error parsing securities data")
        .iter();

    let records: Vec<Board> = resp_iter
        .map(|x| Board {
            engine: engine.to_string(),                    // TODO Actualize variance
            market: market.to_string(),                    // TODO Actualize variance
            id: x[0].as_i64().unwrap() as i16, // Convert the value to i64 first, then cast to i16
            board_group_id: x[1].as_i64().unwrap() as i16, // Convert the value to i64 first, then cast to i16
            boardid: x[2].as_str().unwrap().into(), // Convert the value to i64 first, then cast to i16
            title: x[3].as_str().unwrap().into(),
            //is_traded: x[4].as_i64().unwrap() != 0, // Convert 0 or 1 to a bool
            //is_traded: x[4].as_str().unwrap().into(), // Convert 0 or 1 to a bool
            is_traded: engine.to_string(), // FIX: Bad
        })
        .collect();
    println!("Got {} Boards", records.len());

    // Insert boards into db if Option is not empty
    if let Some(db) = db {
        for b in &records {
            db.insert_board(b).await?;
        }
        println!("Inserted {} Boards into DB", records.len());
    }

    Ok(records)
}

async fn get_all_securities(
    db: &Option<ClickhouseDatabase>,
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

    // Insert securities into db if Option is not empty
    if let Some(db) = db {
        for sec in &records {
            db.insert_security(sec).await?;
        }
    }

    Ok(records)
}

/// Save candle records to a JSON file
async fn save_candles_to_file(
    file_path: &str,
    candles: &Vec<CandleRecord>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = File::create(file_path).await?;
    let candles_json = serde_json::to_string(&candles)?;
    file.write_all(candles_json.as_bytes()).await?;
    println!("Candles saved to {}", file_path);

    Ok(())
}

use crate::config::Config;
use crate::db::ClickhouseDatabase;
use crate::models::{Board, Trade};
use std::collections::HashMap;
use std::time::Instant;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

/// Base runner for running on a single thread
pub async fn base_runner(
    conf: &Config,
    db: &Option<ClickhouseDatabase>,
) -> Result<(), Box<dyn std::error::Error>> {
    let boards = get_all_boards().await?;

    // Save board market data
    for board in &boards {
        run_board(conf, db, board).await?;
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

async fn get_all_boards() -> Result<Vec<Board>, Box<dyn std::error::Error>> {
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
            is_traded: x[4].as_i64().unwrap() != 0, // Convert 0 or 1 to a bool
        })
        .collect();
    println!("Got {} Boards", records.len());

    Ok(records)
}

/// Insert trades
async fn run_board(
    conf: &Config,
    db: &Option<ClickhouseDatabase>,
    board: &Board,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(db) = db {
        // Insert board into db
        db.insert_board(board).await?;
        println!(
            "Inserted board {}, engine: {} market: {}",
            board.boardid, board.engine, board.market
        );
        // Insert board into the database
    } else {
        println!("Not inserting board into db");
    }

    // Insert trades for each board
    let mut start: i32 = 0;
    let mut loop_num: i32 = 1;
    loop {
        let trades = board
            .fetch_trades(&board.engine, &board.market, start)
            .await?;

        if trades.is_empty() {
            println!(
                "Stopping gathering market data for board {}, engine: {} market: {}, loop {}",
                board.boardid, board.engine, board.market, loop_num
            );
            break;
        }

        // Save market data
        let time_trade: Instant = Instant::now();
        if let Some(db) = db {
            // Insert trades into the database
            for trade in &trades {
                db.insert_trade(trade).await?;
            }
            println!(
                "Trades savedto db loop {loop_num}, start {start}, {:.2?}",
                time_trade.elapsed()
            );
        } else {
            println!("Not inserting trades into db");
            // Otherwise Save market data as JSON to disk
            let file_path = format!(
                "{}/{}-{}-{}.json",
                conf.md_path, board.engine, board.market, loop_num
            );
            save_trades_to_file(&file_path, &trades).await?;
            println!(
                "Trades saved to {file_path}, loop {loop_num}, start {start}, {:.2?}",
                time_trade.elapsed()
            );
        }

        // Batch insert trades into DB
        // db.insert_trades(&trades).await?;

        start += trades.len() as i32;
        loop_num += 1;
    }
    Ok(())
}

/// Save trades to a JSON file
async fn save_trades_to_file(
    file_path: &str,
    candles: &Vec<Trade>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = File::create(file_path).await?;
    let candles_json = serde_json::to_string(&candles)?;
    file.write_all(candles_json.as_bytes()).await?;
    Ok(())
}

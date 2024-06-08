use crate::config::Config;
use crate::db::ClickhouseDatabase;
use crate::models::{get_boards, get_engines, get_markets, Board, Engine, Market, Trade};
use std::time::Instant;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

/// # Base runner for running on a single thread
pub async fn base_runner(
    conf: &Config,
    db: &Option<ClickhouseDatabase>,
) -> Result<(), Box<dyn std::error::Error>> {
    let engines = get_engines().await?;
    for chunk in engines.chunks(conf.chunks) {
        // Save Engines to db if defined
        if let Some(db) = db {
            db.insert_engines(chunk).await?;
        }

        // FIX: Implement trades format parsing for all types of Engines, Markets and Boards
        let filtered: Vec<&Engine> = engines.iter().filter(|p| p.name == "stock").collect();

        // Loop through all Engines and run them
        for engine in filtered {
            run_engine(conf, db, engine).await?;
        }
    }

    Ok(())
}

/// # Run Engine
async fn run_engine(
    conf: &Config,
    db: &Option<ClickhouseDatabase>,
    engine: &Engine,
) -> Result<(), Box<dyn std::error::Error>> {
    let markets = get_markets(&engine.name).await?;
    for chunk in markets.chunks(conf.chunks) {
        // Save Markets to db if defined
        if let Some(db) = db {
            db.insert_markets(chunk).await?;
        }

        // FIX: Implement trades format parsing for all types of Engines, Markets and Boards
        let filtered: Vec<&Market> = markets
            .iter()
            .filter(|p| p.engine == "stock" && p.name == "shares")
            .collect();

        // Loop through all Markets and run them
        for market in filtered {
            run_market(conf, db, market).await?;
        }
    }

    Ok(())
}

/// # Run Market
async fn run_market(
    conf: &Config,
    db: &Option<ClickhouseDatabase>,
    market: &Market,
) -> Result<(), Box<dyn std::error::Error>> {
    let boards = get_boards(&market.engine, &market.name).await?;
    for chunk in boards.chunks(conf.chunks) {
        // Save Board market data
        if let Some(db) = db {
            db.insert_boards(chunk).await?;
        }

        // FIX: Implement trades format parsing for all types of Engines, Markets and Boards
        let filtered: Vec<&Board> = boards
            .iter()
            // Note: is_traded is necessary
            .filter(|p| {
                p.is_traded && p.engine == "stock" && p.market == "shares" && p.boardid == "TQBR"
            })
            .collect();

        // Loop through all Boards and run them
        for board in filtered {
            run_board(conf, db, board).await?;
        }
    }
    Ok(())
}

/// # Run Board
async fn run_board(
    conf: &Config,
    db: &Option<ClickhouseDatabase>,
    board: &Board,
) -> Result<(), Box<dyn std::error::Error>> {
    // Insert trades for each board
    let mut start: i32 = 0;
    let mut loop_num: i32 = 1;
    'outer: loop {
        let trades = board
            .fetch_trades(&board.engine, &board.market, start)
            .await?;

        if trades.is_empty() {
            println!(
                "Board '{}' STOP Gathering: Market '{}' for Engine '{}' loop {}",
                board.boardid, board.engine, board.market, loop_num
            );
            break 'outer;
        }

        // Save market data
        let time_trade: Instant = Instant::now();
        if let Some(db) = db {
            // Insert trades into the database
            let mut chunk_count = 0;
            for chunk in trades.chunks(conf.chunks) {
                db.insert_trades(chunk).await?;
                chunk_count += 1;
                println!(
                    "Trades[{}] chunk saved to DB: chunk {} chunk_size {} time {:.2?}",
                    chunk.len(),
                    chunk_count,
                    conf.chunks,
                    time_trade.elapsed()
                );
            }
        } else {
            println!("Not inserting trades into db");
            // Otherwise Save market data as JSON to disk
            let file_path = format!(
                "{}/{}-{}-{}.json",
                conf.md_path, board.engine, board.market, loop_num
            );
            save_trades_to_file(&file_path, &trades).await?;
            println!(
                "Trades[{}] saved to file: {} loop {} start {} time {:.2?}",
                trades.len(),
                file_path,
                loop_num,
                start,
                time_trade.elapsed()
            );
        }

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

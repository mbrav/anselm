use clickhouse::Row;
use serde::Serialize;
use std::collections::HashMap;
use std::time::Instant;
use time::{format_description::well_known::Iso8601, OffsetDateTime, PrimitiveDateTime, UtcOffset};
/// Data Struct for holding Engine data
#[derive(Debug, Clone, Serialize, Row)]
pub struct Engine {
    // Identifiers
    pub id: i32,
    pub name: String,
    pub title: String,
}

/// Data Struct for holding Market data
#[derive(Debug, Clone, Serialize, Row)]
pub struct Market {
    // Identifiers
    pub engine: String,
    pub id: i32,
    pub name: String,
    pub title: String,
}

/// Data Struct for holding Board data
#[derive(Debug, Clone, Serialize, Row)]
pub struct Board {
    // Identifiers
    pub engine: String,
    pub market: String,
    pub id: i32,
    pub board_group_id: i32,
    pub boardid: String,
    pub title: String,
    pub is_traded: bool,
}

/// Trade Record
#[derive(Debug, Clone, Serialize, Row)]
pub struct Trade {
    // Identifiers
    pub engine: String,
    pub market: String,
    pub secid: String,
    pub boardid: String,
    // Main data
    pub tradeid: i64,
    pub buysell: String,
    pub quantity: i32,
    pub price: f64,
    pub value: f64,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub tradetime: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub systime: OffsetDateTime,
}

/// Implementation for Egnine data struct
impl Engine {
    /// Fetch market records
    pub async fn fetch_markets(&self) -> Result<Vec<Market>, Box<dyn std::error::Error>> {
        todo!("Implement")
    }
}

/// Implementation for Market data struct
impl Market {
    /// Fetch board records
    pub async fn fetch_boards(&self) -> Result<Vec<Board>, Box<dyn std::error::Error>> {
        todo!("Implement")
    }
}

/// Implementation for Board data struct
impl Board {
    /// Fetch trades records
    pub async fn fetch_trades(
        &self,
        engine: &String,
        market: &String,
        start: i32,
    ) -> Result<Vec<Trade>, Box<dyn std::error::Error>> {
        let url = format!(
            "https://iss.moex.com/iss/engines/{}/markets/{}/boards/{}/trades.json",
            engine, market, self.boardid
        );

        // Create a client
        let client = reqwest::Client::new();

        // Time req
        let time_req: Instant = Instant::now();

        // Fetch response
        let resp = client
            .get(&url)
            .query(&[("start", start.to_string())])
            .send()
            .await?
            .json::<HashMap<String, serde_json::Value>>()
            .await?;
        let time_req = time_req.elapsed();

        // Time Parsing
        let time_parse: Instant = Instant::now();

        // Convert response into an iterator
        let resp_iter = resp["trades"]["data"]
            .as_array()
            .expect("Error parsing securities")
            .iter();

        // Define Timezone for MOEX
        let moscow_offset = UtcOffset::from_hms(3, 0, 0)?;

        // Parse iterator
        let records: Vec<Trade> = resp_iter
            .map(|x| {
                // Replace space with to make it ISO8601 compliant
                let iso_date = x[9].as_str().unwrap().replace(' ', "T");
                // Parse date by converting first to primitive date
                // Then to timezone aware datetime
                let trade_time = PrimitiveDateTime::parse(&iso_date, &Iso8601::DEFAULT).unwrap();
                let price = x[4].as_f64().unwrap();
                let quantity = x[5].as_i64().unwrap();
                let value = price * quantity as f64;
                Trade {
                    engine: engine.clone(),
                    market: market.clone(),
                    tradeid: x[0].as_i64().unwrap(),
                    // TODO: Make Date + Time merge
                    tradetime: trade_time.assume_offset(moscow_offset),
                    boardid: x[2].as_str().unwrap().into(),
                    secid: x[3].as_str().unwrap().into(),
                    price,
                    quantity: quantity as i32,
                    value,
                    systime: trade_time.assume_offset(moscow_offset),
                    buysell: x[10].as_str().unwrap().into(),
                }
            })
            .collect();

        // Set time for first and last trade
        let first_trade = if !records.is_empty() {
            records.first().unwrap().tradetime.to_string()
        } else {
            "none".to_string()
        };
        let last_trade = if !records.is_empty() {
            records.last().unwrap().tradetime.to_string()
        } else {
            "none".to_string()
        };

        println!(
            "Trades[{}]: Board '{}' for Market '{}' for Engine '{}' from {} until {} start {} response {:.2?} parse {:.2?}",
            records.len(),
            self.boardid,
            market,
            engine,
            first_trade,
            last_trade,
            start,
            time_req,
            time_parse.elapsed()
        );

        Ok(records)
    }
}

/// Get engines
/// TODO: Impliment as async trait for struct
pub async fn get_engines() -> Result<Vec<Engine>, Box<dyn std::error::Error>> {
    let url = "https://iss.moex.com/iss/engines.json";

    let resp = reqwest::get(url)
        .await?
        .json::<HashMap<String, serde_json::Value>>()
        .await?;

    let resp_iter = resp["engines"]["data"]
        .as_array()
        .expect("Error parsing Engines API data")
        .iter();

    let records: Vec<Engine> = resp_iter
        .map(|x| Engine {
            id: x[0].as_i64().unwrap() as i32,
            name: x[1].as_str().unwrap().into(),
            title: x[2].as_str().unwrap().into(),
        })
        .collect();

    println!("API GET Engines[{}]", records.len());
    Ok(records)
}

/// Get markets for a given engine
/// TODO: Impliment as async trait for struct
pub async fn get_markets(engine: &String) -> Result<Vec<Market>, Box<dyn std::error::Error>> {
    let url = format!(
        "https://iss.moex.com/iss/engines/{}/markets.json",
        engine.as_str()
    );

    println!("{}", url);

    let resp = reqwest::get(url)
        .await?
        .json::<HashMap<String, serde_json::Value>>()
        .await?;

    let resp_iter = resp["markets"]["data"]
        .as_array()
        .expect("Error parsing Markets API data")
        .iter();

    let records: Vec<Market> = resp_iter
        .map(|x| Market {
            engine: engine.to_string(),
            id: x[0].as_i64().unwrap() as i32,
            name: x[1].as_str().unwrap().into(),
            title: x[2].as_str().unwrap().into(),
        })
        .collect();

    println!("API GET Markets[{}]: Engine '{}'", records.len(), engine);
    Ok(records)
}

/// Get boards for a given engine and market
/// TODO: Impliment as async trait for struct
pub async fn get_boards(
    engine: &String,
    market: &String,
) -> Result<Vec<Board>, Box<dyn std::error::Error>> {
    let url = format!("https://iss.moex.com/iss/engines/{engine}/markets/{market}/boards.json");

    let resp = reqwest::get(url)
        .await?
        .json::<HashMap<String, serde_json::Value>>()
        .await?;

    let resp_iter = resp["boards"]["data"]
        .as_array()
        .expect("Error parsing Boards API data")
        .iter();

    let records: Vec<Board> = resp_iter
        .map(|x| Board {
            engine: engine.to_string(),
            market: market.to_string(),
            // Convert the value to i64 first, then cast to i32
            id: x[0].as_i64().unwrap() as i32,
            // Convert the value to i64 first, then cast to i32
            board_group_id: x[1].as_i64().unwrap() as i32,
            boardid: x[2].as_str().unwrap().into(),
            title: x[3].as_str().unwrap().into(),
            // Convert 0 or 1 to a bool
            is_traded: x[4].as_i64().unwrap() != 0,
        })
        .collect();

    println!(
        "API GET Boards[{}]: Market '{}' for Engine '{}'",
        records.len(),
        market,
        engine
    );
    Ok(records)
}

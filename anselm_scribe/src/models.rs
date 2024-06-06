use chrono::NaiveDateTime;
use clickhouse::Row;
use serde::Serialize;
use std::collections::HashMap;

/// Candle Record
#[derive(Debug, Clone, Serialize, Row)]
pub struct CandleRecord {
    // Identifiers
    pub secid: String,
    pub boardid: String,
    pub shortname: String,
    // Main data
    pub timeframe: i16,
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub value: f64,
    pub volume: f64,
    pub begin: NaiveDateTime,
    pub end: NaiveDateTime,
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
    pub quantity: i16,
    pub price: f32,
    pub value: f32,
    pub tradetime: NaiveDateTime,
    pub systime: NaiveDateTime,
}

/// Data Struct for holding Board data
#[derive(Debug, Clone, Serialize, Row)]
pub struct Board {
    // Identifiers
    pub engine: String,
    pub market: String,
    pub id: i16,
    pub board_group_id: i16,
    pub boardid: String,
    pub title: String,
    //pub is_traded: bool,
    pub is_traded: String,
}

/// Data Struct for holding security data
#[derive(Debug, Clone, Serialize, Row)]
pub struct Security {
    pub secid: String,      // SECID: {"type": "string", "bytes": 36, "max_size": 0}
    pub boardid: String,    // BOARDID: {"type": "string", "bytes": 12, "max_size": 0}
    pub shortname: String,  // SHORTNAME: {"type": "string", "bytes": 30, "max_size": 0}
    pub status: String,     // STATUS: {"type": "string", "bytes": 3, "max_size": 0}
    pub marketcode: String, // MARKETCODE: {"type": "string", "bytes": 12, "max_size": 0}
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
            "https://iss.moex.com/iss/engines/{}/markets/{}/trades.json",
            engine, market
        );

        println!("Geting URL: {url}");

        // Create a client
        let client = reqwest::Client::new();

        // Fetch response
        let resp = client
            .get(&url)
            .query(&[("start", start.to_string())])
            .send()
            .await?
            .json::<HashMap<String, serde_json::Value>>()
            .await?;

        // Convert response into an iterator
        let resp_iter = resp["trades"]["data"]
            .as_array()
            .expect("Error parsing securities")
            .iter();

        // Parse iterator
        let records: Vec<Trade> = resp_iter
            .map(|x| Trade {
                engine: engine.clone(),
                market: market.clone(),
                tradeid: x[0].as_i64().unwrap(),
                tradetime: NaiveDateTime::parse_from_str(
                    x[1].as_str().unwrap(),
                    "%Y-%m-%d %H:%M:%S",
                )
                .unwrap(),
                boardid: x[2].as_str().unwrap().into(),
                secid: x[3].as_str().unwrap().into(),
                price: x[4].as_f64().unwrap() as f32,
                quantity: x[5].as_i64().unwrap() as i16,
                value: x[6].as_f64().unwrap() as f32,
                // TODO Make Date + Time merge
                //systime: NaiveDateTime::parse_from_str(x[9].as_str().unwrap(), "%Y-%m-%d %H:%M:%S")
                //    .unwrap(),
                systime: NaiveDateTime::parse_from_str(x[1].as_str().unwrap(), "%Y-%m-%d %H:%M:%S")
                    .unwrap(),
                buysell: x[10].as_str().unwrap().into(),
            })
            .collect();

        println!(
            "Got {} Trades for Engine {}, Market {}, start {}",
            records.len(),
            engine,
            market,
            start,
        );

        Ok(records)
    }
}

/// Implementation for Security data struct
impl Security {
    /// Fetch candle records
    pub async fn fetch_candles(
        &self,
        interval: i16,
        date_start: &String,
        date_end: &String,
    ) -> Result<Vec<CandleRecord>, Box<dyn std::error::Error>> {
        let url = format!(
            "https://iss.moex.com/iss/engines/stock/markets/shares/securities/{}/candles.json?interval={}&from={}&till={}",
            self.secid, interval, date_start, date_end
        );
        println!("Geting URL: {url}");

        let resp = reqwest::get(&url)
            .await?
            .json::<HashMap<String, serde_json::Value>>()
            .await?;

        let resp_iter = resp["candles"]["data"]
            .as_array()
            .expect("Error parsing securities")
            .iter();
        let records: Vec<CandleRecord> = resp_iter
            .map(|x| CandleRecord {
                secid: self.secid.clone(),
                boardid: self.boardid.clone(),
                shortname: self.shortname.clone(),
                timeframe: interval,
                open: x[0].as_f64().unwrap(),
                close: x[1].as_f64().unwrap(),
                high: x[2].as_f64().unwrap(),
                low: x[3].as_f64().unwrap(),
                value: x[4].as_f64().unwrap(),
                volume: x[5].as_f64().unwrap(),
                begin: NaiveDateTime::parse_from_str(x[6].as_str().unwrap(), "%Y-%m-%d %H:%M:%S")
                    .unwrap(),
                end: NaiveDateTime::parse_from_str(x[7].as_str().unwrap(), "%Y-%m-%d %H:%M:%S")
                    .unwrap(),
            })
            .collect();

        println!(
            "Got {} Candles for {} {} until {}",
            records.len(),
            self.secid,
            date_start,
            date_end,
        );

        Ok(records)
    }
}

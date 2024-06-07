use chrono::NaiveDateTime;
use clickhouse::Row;
use serde::Serialize;
use std::collections::HashMap;
use std::time::Instant;

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
    pub price: f64,
    pub value: f64,
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
    pub is_traded: bool,
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

        // Parse iterator
        let records: Vec<Trade> = resp_iter
            .map(|x| Trade {
                engine: engine.clone(),
                market: market.clone(),
                tradeid: x[0].as_i64().unwrap(),
                // TODO Make Date + Time merge
                //systime: NaiveDateTime::parse_from_str(x[1].as_str().unwrap(), "%Y-%m-%d %H:%M:%S")
                //    .unwrap(),
                tradetime: NaiveDateTime::parse_from_str(
                    x[9].as_str().unwrap(),
                    "%Y-%m-%d %H:%M:%S",
                )
                .unwrap(),
                boardid: x[2].as_str().unwrap().into(),
                secid: x[3].as_str().unwrap().into(),
                price: x[4].as_f64().unwrap(),
                quantity: x[5].as_i64().unwrap() as i16,
                value: x[6].as_f64().unwrap(),
                systime: NaiveDateTime::parse_from_str(x[9].as_str().unwrap(), "%Y-%m-%d %H:%M:%S")
                    .unwrap(),
                buysell: x[10].as_str().unwrap().into(),
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
            "Got {} Trades for Engine {}, Market {}, from {} to {}, start {}, r: {:.2?} p: {:.2?}",
            records.len(),
            engine,
            market,
            first_trade,
            last_trade,
            start,
            time_req,
            time_parse.elapsed()
        );

        Ok(records)
    }
}

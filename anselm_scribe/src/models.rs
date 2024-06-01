//use chrono::NaiveDateTime;
use serde::Serialize;
use std::collections::HashMap;

/// Candle Record
#[derive(Debug, Clone, Serialize)]
pub struct CandleRecord {
    pub timeframe: i16,
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub value: f64,
    pub volume: f64,
    pub begin: String,
    pub end: String,
    //pub begin: NaiveDateTime,
    //pub end: NaiveDateTime,
}

/// Data Struct for holding security data
#[derive(Debug, Clone)]
pub struct Security {
    pub secid: String,     // SECID: {"type": "string", "bytes": 36, "max_size": 0}
    pub boardid: String,   // BOARDID: {"type": "string", "bytes": 12, "max_size": 0}
    pub shortname: String, // SHORTNAME: {"type": "string", "bytes": 30, "max_size": 0}
    // skip prevprice
    //pub lotsize: i32, // LOTSIZE: {"type": "int32"}
    //pub facevalue: f64,    // FACEVALUE: {"type": "double"}
    pub status: String, // STATUS: {"type": "string", "bytes": 3, "max_size": 0}
    //pub boardname: String, // BOARDNAME: {"type": "string", "bytes": 381, "max_size": 0}
    //// skip decimals
    //// skip remarks
    //pub secname: String, // SECNAME: {"type": "string", "bytes": 90, "max_size": 0}
    pub marketcode: String, // MARKETCODE: {"type": "string", "bytes": 12, "max_size": 0}

                            //pub instrid: String, // INSTRID: {"type": "string", "bytes": 12, "max_size": 0}
                            //pub sectorid: String, // SECTORID: {"type": "string", "bytes": 12, "max_size": 0}
                            //pub minstep: f64,    // MINSTEP: {"type": "double"}
                            //// skip prevwaprice
                            //pub faceunit: String, // FACEUNIT: {"type": "string", "bytes": 12, "max_size": 0}
                            //pub prevdate: String, // PREVDATE: {"type": "date", "bytes": 10, "max_size": 0}
                            //pub issuesize: i64,   // ISSUESIZE: {"type": "int64"}
                            //pub isin: String,     // ISIN: {"type": "string", "bytes": 36, "max_size": 0}
                            //pub latname: String,  // LATNAME: {"type": "string", "bytes": 90, "max_size": 0}
                            //pub regnumber: String, // REGNUMBER: {"type": "string", "bytes": 90, "max_size": 0}
                            //// skip prevlegalcloseprice
                            //pub currencyid: String, // CURRENCYID: {"type": "string", "bytes": 12, "max_size": 0}
                            //pub sectype: String,    // SECTYPE: {"type": "string", "bytes": 3, "max_size": 0}
                            //pub listlevel: i32,     // LISTLEVEL: {"type": "int32"}
                            //pub settledate: String, // SETTLEDATE: {"type": "date", "bytes": 10, "max_size": 0}
                            //pub candles: Vec<CandleRecord>, // Candlestick data
}

/// Implementation for Security data struct
impl Security {
    /// Fetch candle records
    pub async fn fetch_candles(
        &mut self,
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
                timeframe: interval,
                open: x[0].as_f64().unwrap(),
                close: x[1].as_f64().unwrap(),
                high: x[2].as_f64().unwrap(),
                low: x[3].as_f64().unwrap(),
                value: x[4].as_f64().unwrap(),
                volume: x[5].as_f64().unwrap(),
                begin: x[6].as_str().unwrap().into(),
                end: x[7].as_str().unwrap().into(),
                //begin: NaiveDateTime::parse_from_str(
                //    x[6].as_str().expect("Error parsing date"),
                //    "%Y-%m-%d %H:%M:%S",
                //)
                //.unwrap(),
                //end: NaiveDateTime::parse_from_str(
                //    x[7].as_str().expect("Error parsing date"),
                //    "%Y-%m-%d %H:%M:%S",
                //.unwrap(),
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

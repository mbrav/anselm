use crate::models::Security;
use crate::{config::Config, models::CandleRecord};
use clickhouse::{error::Result, sql, Client};

/// Clickhouse Clickhouse Database struct
pub struct ClickhouseDatabase {
    client: Client,
    db: String,
}

/// Implementation for ClickhouseDatabase Struct
impl ClickhouseDatabase {
    /// ClichouseDatabase instance factory
    pub fn new(conf: &Config) -> Self {
        let client = Client::default()
            .with_url(&conf.ch_url)
            .with_user(&conf.ch_user)
            .with_password(&conf.ch_password);

        Self {
            client,
            db: conf.ch_db.clone(),
        }
    }

    /// Init database with required tables
    ///
    /// # Steps
    /// - Create defined database if it does not exist
    /// - Create table for securities
    /// - Create table for candles
    pub async fn init(&self) -> Result<()> {
        self.client
            .query("CREATE DATABASE IF NOT EXISTS ?")
            .bind(sql::Identifier(self.db.as_str()))
            .execute()
            .await?;

        self.init_security().await?;
        self.init_candles().await?;

        Ok(())
    }

    /// Init security table
    pub async fn init_security(&self) -> Result<()> {
        self.client
            .query(
                "
                CREATE TABLE IF NOT EXISTS ?.securities(
                    secid       String DEFAULT '',
                    boardid     String DEFAULT '',
                    shortname   String DEFAULT '',
                    status      String DEFAULT '',
                    marketcode  String DEFAULT '',
                )
                ENGINE = MergeTree
                PRIMARY KEY secid
                ",
            )
            .bind(sql::Identifier(self.db.as_str()))
            .execute()
            .await?;
        Ok(())
    }

    /// # Table Optimizations
    /// In order to provide an efficient way for storing and querying data, a few important
    /// decisions and optimizations were made when defining the table schema.
    ///
    /// ## Table Schema
    /// The `candles` table stores the OHLCV (Open, High, Low, Close, Volume) data for
    /// securities over different time frames. Each record in this table represents a single
    /// candle for a specific security and time frame.
    ///
    /// The columns in the table are as follows:
    /// - `secid`: The unique identifier for the security. It is stored as a
    ///   `LowCardinality(String)` for efficient storage and querying of categorical data.
    /// - `timeframe`: The time frame for the candle (e.g., 1 minute, 5 minutes, etc.). It is
    ///   stored as an `Int16` and uses the `CODEC(Delta, Default)` codec for efficient storage.
    /// - `open`: The opening price of the security for the candle's time frame.
    /// - `close`: The closing price of the security for the candle's time frame.
    /// - `high`: The highest price of the security for the candle's time frame.
    /// - `low`: The lowest price of the security for the candle's time frame.
    /// - `value`: The total value traded during the candle's time frame.
    /// - `volume`: The total volume traded during the candle's time frame.
    /// - `begin`: The start time of the candle.
    /// - `end`: The end time of the candle.
    ///
    /// ## Storing Only Deltas
    /// For columns where values typically change incrementally, the `CODEC(Delta, Default)`
    /// keyword is used. This codec helps in reducing the storage space required by storing only
    /// the differences (deltas) between consecutive values, rather than the full values. This
    /// is particularly useful for columns like `open`, `close`, `high`, `low`, `value`, and
    /// `volume`, where the changes between rows are often small.
    ///
    /// For example:
    ///
    /// ```sql
    /// open       Nullable(Float64) CODEC(Delta, Default),
    /// close      Nullable(Float64) CODEC(Delta, Default),
    /// ```
    ///
    /// By using the `CODEC(Delta, Default)`, the storage engine can store the difference
    /// between consecutive values, which can lead to significant space savings.
    ///
    /// ## ClickHouse Engine and Order
    /// The table uses the `MergeTree` engine, which is optimized for analytical queries. The
    /// `ORDER BY` clause specifies that the data should be ordered by `secid` and `begin`,
    /// allowing efficient range queries based on security ID and time.
    ///
    /// ## Resources
    ///  - [Using ClickHouse for financial market data - Christoph Wurm (ClickHouse)](https://youtu.be/Ojv6LPXKy2U?si=Je8BkFA8nOTczLZn)
    pub async fn init_candles(&self) -> Result<()> {
        self.client
            .query(
                "
                CREATE TABLE IF NOT EXISTS ?.candles(
                    secid      LowCardinality(String) DEFAULT '',
                    timeframe  Int16 DEFAULT 1 CODEC(Delta, Default),
                    open       Nullable(Float64) CODEC(Delta, Default),
                    close      Nullable(Float64) CODEC(Delta, Default),
                    high       Nullable(Float64) CODEC(Delta, Default),
                    low        Nullable(Float64) CODEC(Delta, Default),
                    value      Nullable(Float64) CODEC(Delta, Default),
                    volume     Nullable(Float64) CODEC(Delta, Default),
                    begin      DateTime CODEC(Delta, Default),
                    end        DateTime CODEC(Delta, Default),
                )
                ENGINE = MergeTree
                ORDER BY (secid, begin)
                ",
            )
            .bind(sql::Identifier(self.db.as_str()))
            .execute()
            .await?;
        Ok(())
    }

    /// Insert new security record in database
    pub async fn insert_security(&self, security: &Security) -> Result<()> {
        self.client
            .query("INSERT INTO ?.securities (*) VALUES (?,?,?,?,?)")
            .bind(sql::Identifier(self.db.as_str()))
            .bind(security.secid.as_str())
            .bind(security.boardid.as_str())
            .bind(security.shortname.as_str())
            .bind(security.status.as_str())
            .bind(security.marketcode.as_str())
            .execute()
            .await?;

        println!("Inserted security {} into db", security.secid);
        Ok(())
    }
    /// Insert new candle record in database
    pub async fn insert_candle(&self, candle: &CandleRecord) -> Result<()> {
        self.client
            .query("INSERT INTO ?.candles (*) VALUES (?,?,?,?,?,?,?,?,toDateTime('?'),'toDateTime('?'))")
            .bind(sql::Identifier(self.db.as_str()))
            .bind(candle.secid.as_str())
            .bind(candle.timeframe)
            .bind(candle.open)
            .bind(candle.close)
            .bind(candle.high)
            .bind(candle.low)
            .bind(candle.value)
            .bind(candle.volume)
            .bind(candle.begin.to_string())
            .bind(candle.end.to_string())
            .execute()
            .await?;

        println!(
            "Inserted candle from {} to {} for security {} into db",
            candle.begin, candle.end, candle.secid
        );
        Ok(())
    }
}

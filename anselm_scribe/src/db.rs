use crate::config::Config;
use crate::models::{Board, Trade};
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

        // Init tables
        self.init_security().await?;
        self.init_board().await?;
        self.init_candles().await?;
        self.init_trades().await?;

        Ok(())
    }

    /// Init security table
    pub async fn init_security(&self) -> Result<()> {
        self.client
            .query(
                "
                CREATE TABLE IF NOT EXISTS ?.securities(
                    secid       String,
                    boardid     String,
                    shortname   String,
                    status      String,
                    marketcode  String,
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

    /// Init board table
    pub async fn init_board(&self) -> Result<()> {
        self.client
            // TODO boolen
            .query(
                "
                CREATE TABLE IF NOT EXISTS ?.boards(
                    engine           LowCardinality(String) Codec(ZSTD(1)),
                    market           LowCardinality(String) Codec(ZSTD(1)),
                    id               UInt16,
                    board_group_id   UInt16,
                    boardid          LowCardinality(String) Codec(ZSTD(1)),
                    title            String,
                    is_traded        Boolean,
                )
                ENGINE = MergeTree
                PRIMARY KEY (engine, market, boardid)
                ORDER BY (engine, market, boardid);
                ",
            )
            .bind(sql::Identifier(self.db.as_str()))
            .execute()
            .await?;
        Ok(())
    }

    /// # Initialize Trade Record table
    pub async fn init_trades(&self) -> Result<()> {
        self.client
            // TODO enum
            //  buysell    Enum8('B' = 1, 'S' = 2) Codec(ZSTD(1)),
            .query(
                "
                CREATE TABLE IF NOT EXISTS ?.trades(
                    engine     LowCardinality(String) Codec(ZSTD(1)),
                    market     LowCardinality(String) Codec(ZSTD(1)),
                    secid      LowCardinality(String) Codec(ZSTD(1)),
                    boardid    LowCardinality(String) Codec(ZSTD(1)),
                    tradeid    UInt64 Codec(Delta, Default),
                    buysell    LowCardinality(String) Codec(ZSTD(1)),
                    quantity   UInt16,
                    price      Float32 Codec(Delta, Default),
                    value      Float32 Codec(Delta, Default),
                    tradetime  DateTime Codec(DoubleDelta, ZSTD(1)),
                    systime    DateTime Codec(DoubleDelta, ZSTD(1)),
                )
                ENGINE = MergeTree
                PARTITION BY toYYYYMM(tradetime)
                ORDER BY (engine, market, secid, boardid, tradetime, tradeid);
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
    ///   stored as an `Int16` and uses the `Codec(Delta, Default)` codec for efficient storage.
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
    /// For columns where values typically change incrementally, the `Codec(Delta, Default)`
    /// keyword is used. This codec helps in reducing the storage space required by storing only
    /// the differences (deltas) between consecutive values, rather than the full values. This
    /// is particularly useful for columns like `open`, `close`, `high`, `low`, `value`, and
    /// `volume`, where the changes between rows are often small.
    ///
    /// For example:
    ///
    /// ```sql
    /// open       Nullable(Float64) Codec(Delta, Default),
    /// close      Nullable(Float64) Codec(Delta, Default),
    /// ```
    ///
    /// By using the `Codec(Delta, Default)`, the storage engine can store the difference
    /// between consecutive values, which can lead to significant space savings.
    ///
    /// ## ClickHouse Engine and Order
    /// The table uses the `MergeTree` engine, which is optimized for analytical queries. The
    /// `ORDER BY` clause specifies that the data should be ordered by `secid` and `begin`,
    /// allowing efficient range queries based on security ID and time.
    ///
    /// ## Resources
    ///  - [Optimizing ClickHouse with Schemas and Codecs](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
    ///  - [Using ClickHouse for financial market data - Christoph Wurm (ClickHouse)](https://youtu.be/Ojv6LPXKy2U?si=Je8BkFA8nOTczLZn)
    ///  - [New Tips and Tricks that Every ClickHouse Developer Should Know | ClickHouse Webinar](https://youtu.be/BLt246SijGU?si=4yBgOVfvfjs-34Qc)
    pub async fn init_candles(&self) -> Result<()> {
        self.client
            .query(
                "
                CREATE TABLE IF NOT EXISTS ?.candles(
                    secid      LowCardinality(String) Codec(ZSTD(1)),
                    boardid    LowCardinality(String) Codec(ZSTD(1)),
                    shortname  LowCardinality(String) Codec(ZSTD(1)),
                    timeframe  Int16 Codec(Delta, Default),
                    open       Nullable(Float64) Codec(Delta, Default),
                    close      Nullable(Float64) Codec(Delta, Default),
                    high       Nullable(Float64) Codec(Delta, Default),
                    low        Nullable(Float64) Codec(Delta, Default),
                    value      Nullable(Float64) Codec(Delta, Default),
                    volume     Nullable(Float64) Codec(Delta, Default),
                    begin      DateTime Codec(DoubleDelta, ZSTD(1)),
                    end        DateTime Codec(DoubleDelta, ZSTD(1)),
                )
                ENGINE = MergeTree
                PARTITION BY toYYYYMM(begin)
                ORDER BY (secid, begin, end)
                ",
            )
            .bind(sql::Identifier(self.db.as_str()))
            .execute()
            .await?;
        Ok(())
    }

    /// Insert new board record in database
    pub async fn insert_board(&self, board: &Board) -> Result<()> {
        self.client
            .query("INSERT INTO ?.boards (*) VALUES (?,?,?,?,?,?,?)")
            .bind(sql::Identifier(self.db.as_str()))
            .bind(board.engine.as_str())
            .bind(board.market.as_str())
            .bind(board.id)
            .bind(board.board_group_id)
            .bind(board.boardid.as_str())
            .bind(board.title.as_str())
            .bind(board.is_traded)
            .execute()
            .await?;
        Ok(())
    }

    /// Insert new trade record in database
    pub async fn insert_trade(&self, trade: &Trade) -> Result<()> {
        self.client
            .query(
                "INSERT INTO ?.trades (*) VALUES (?,?,?,?,?,?,?,?,?,toDateTime(?),toDateTime(?))",
            )
            .bind(sql::Identifier(self.db.as_str()))
            .bind(trade.secid.as_str())
            .bind(trade.market.as_str())
            .bind(trade.secid.as_str())
            .bind(trade.boardid.as_str())
            .bind(trade.tradeid)
            .bind(trade.buysell.as_str())
            .bind(trade.quantity)
            .bind(trade.price)
            .bind(trade.value)
            .bind(trade.tradetime.to_string())
            .bind(trade.systime.to_string())
            .execute()
            .await?;

        Ok(())
    }
    /// Insert new trades in database as a batch
    pub async fn insert_trades(&self, candles: &Vec<Trade>) -> Result<()> {
        let mut insert = self.client.insert(format!("{}.trades", self.db).as_str())?;
        for candle in candles {
            insert.write(candle).await?;
        }
        insert.end().await?;
        Ok(())
    }
}

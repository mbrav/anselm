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
        self.init_board().await?;
        self.init_trades().await?;

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
                    price      Float64 Codec(Gorilla, ZSTD(1)),
                    value      Float64 Codec(Gorilla, ZSTD(1)),
                    tradetime  DateTime Codec(DoubleDelta, ZSTD(1)),
                    systime    DateTime Codec(DoubleDelta, ZSTD(1)),
                )
                ENGINE = MergeTree
                PARTITION BY toYYYYMM(tradetime)
                ORDER BY (engine, market, secid, boardid, tradeid, tradetime, systime);
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
    /// Insert a batch of trades into database
    pub async fn insert_trades(&self, trades: &[Trade]) -> Result<()> {
        let mut insert = self.client.insert(format!("{}.trades", self.db).as_str())?;
        for trade in trades {
            insert.write(trade).await?;
        }
        insert.end().await.unwrap();
        Ok(())
    }
}

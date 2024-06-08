use crate::config::Config;
use crate::models::{Board, Engine, Market, Trade};
use clickhouse::{error::Result, sql, Client};

/// # Clickhouse Clickhouse Database struct
pub struct ClickhouseDatabase {
    client: Client,
    db: String,
}

/// # Implementation for ClickhouseDatabase Struct
impl ClickhouseDatabase {
    /// # ClichouseDatabase instance factory
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

    /// # Init database with required tables
    ///
    /// ## Steps
    /// Create defined database tables if they do not exist:
    /// - `>db_name<.engines`
    /// - `>db_name<.markets`
    /// - `>db_name<.boards`
    /// - `>db_name<.trades`
    ///
    pub async fn init(&self) -> Result<()> {
        self.client
            .query("CREATE DATABASE IF NOT EXISTS ?")
            .bind(sql::Identifier(self.db.as_str()))
            .execute()
            .await?;

        // Init DB tables
        self.init_engines().await?;
        self.init_markets().await?;
        self.init_boards().await?;
        self.init_trades().await?;

        Ok(())
    }

    /// # Initialize Engine Record table
    pub async fn init_engines(&self) -> Result<()> {
        self.client
            .query(
                "
                CREATE TABLE IF NOT EXISTS ?.ebgines(
                    id               UInt32,
                    name             String,
                    title            String
                )
                ENGINE = MergeTree
                PRIMARY KEY (id)
                ORDER BY (id,name);
                ",
            )
            .bind(sql::Identifier(self.db.as_str()))
            .execute()
            .await?;
        Ok(())
    }

    /// # Initialize Market Record table
    pub async fn init_markets(&self) -> Result<()> {
        self.client
            .query(
                "
                CREATE TABLE IF NOT EXISTS ?.markets(
                    engine           LowCardinality(String) Codec(ZSTD(1)),
                    id               UInt32,
                    name             String,
                    title            String
                )
                ENGINE = MergeTree
                PRIMARY KEY (id)
                ORDER BY (id,name);
                ",
            )
            .bind(sql::Identifier(self.db.as_str()))
            .execute()
            .await?;
        Ok(())
    }

    /// # Initialize Boards Record table
    pub async fn init_boards(&self) -> Result<()> {
        self.client
            .query(
                "
                CREATE TABLE IF NOT EXISTS ?.boards(
                    engine           LowCardinality(String) Codec(ZSTD(1)),
                    market           LowCardinality(String) Codec(ZSTD(1)),
                    id               UInt32,
                    board_group_id   UInt32,
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
            // TODO: enum
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
                    quantity   UInt32,
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

    /// # Insert a batch of Engine Records into database
    pub async fn insert_engines(&self, engines: &[Engine]) -> Result<()> {
        let mut insert = self
            .client
            .insert(format!("{}.engines", self.db).as_str())?;
        for engine in engines {
            insert.write(engine).await?;
        }
        insert.end().await.unwrap();
        Ok(())
    }

    /// # Insert a batch of Market Records into database
    pub async fn insert_markets(&self, markets: &[Market]) -> Result<()> {
        let mut insert = self
            .client
            .insert(format!("{}.markets", self.db).as_str())?;
        for market in markets {
            insert.write(market).await?;
        }
        insert.end().await.unwrap();
        Ok(())
    }

    /// # Insert a batch of Board Records into database
    pub async fn insert_boards(&self, boards: &[Board]) -> Result<()> {
        let mut insert = self.client.insert(format!("{}.boards", self.db).as_str())?;
        for board in boards {
            insert.write(board).await?;
        }
        insert.end().await.unwrap();
        Ok(())
    }

    /// # Insert a batch of Trade Records into database
    ///
    /// No checks for duplicate data, insert directly to DB
    ///
    /// ```
    /// Harvest now, clean (decrypt) later.
    ///  - NSA
    /// ```
    pub async fn insert_trades(&self, trades: &[Trade]) -> Result<()> {
        let mut insert = self.client.insert(format!("{}.trades", self.db).as_str())?;
        for trade in trades {
            insert.write(trade).await?;
        }
        insert.end().await.unwrap();
        Ok(())
    }
}

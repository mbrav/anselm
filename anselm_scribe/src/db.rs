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
                CREATE TABLE IF NOT EXISTS ?.engines(
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
        // Create sesstion for multiple insertions
        let mut insert = self
            .client
            .insert(format!("{}.engines", self.db).as_str())?;

        for engine in engines {
            // Check if Engine with name already exists
            let count = self
                .client
                .query("SELECT count() FROM ?.engines WHERE name=?")
                .bind(sql::Identifier(self.db.as_str()))
                .bind(&engine.name)
                .fetch_one::<u64>()
                .await?;

            if count == 0 {
                println!("Inserting DB: Engine '{}'", engine.name);
                insert.write(engine).await?;
            } else {
                println!("Exists in DB: Engine '{}'", engine.name);
            }
        }
        insert.end().await.unwrap();
        Ok(())
    }

    /// # Insert a batch of Market Records into database
    pub async fn insert_markets(&self, markets: &[Market]) -> Result<()> {
        // Create sesstion for multiple insertions
        let mut insert = self
            .client
            .insert(format!("{}.markets", self.db).as_str())?;

        for market in markets {
            // Check if Market already exists
            let count = self
                .client
                .query("SELECT count() FROM ?.markets WHERE name=? AND engine=?")
                .bind(sql::Identifier(self.db.as_str()))
                .bind(&market.name)
                .bind(&market.engine)
                .fetch_one::<u64>()
                .await?;

            if count == 0 {
                println!(
                    "Inserting DB: Market '{}' for Engine '{}'",
                    market.name, market.engine
                );
                insert.write(market).await?;
            } else {
                println!(
                    "Exists in DB: Market '{}' for Engine '{}'",
                    market.name, market.engine
                );
            }
        }

        insert.end().await.unwrap();
        Ok(())
    }

    /// # Insert a batch of Board Records into database
    pub async fn insert_boards(&self, boards: &[Board]) -> Result<()> {
        // Create sesstion for multiple insertions
        let mut insert = self.client.insert(format!("{}.boards", self.db).as_str())?;

        for board in boards {
            // Check if Board already exists
            let count = self
                .client
                .query("SELECT count() FROM ?.boards WHERE boardid=? AND market=? AND engine=?")
                .bind(sql::Identifier(self.db.as_str()))
                .bind(&board.boardid)
                .bind(&board.market)
                .bind(&board.engine)
                .fetch_one::<u64>()
                .await?;

            if count == 0 {
                println!(
                    "Inserting DB: Board '{}' for Market '{}' for Engine '{}'",
                    board.boardid, board.market, board.engine,
                );
                insert.write(board).await?;
            } else {
                println!(
                    "Exists in DB: Board '{}' for Market '{}' for Engine '{}'",
                    board.boardid, board.market, board.engine,
                );
            }
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

use crate::models::Security;
use crate::{config::Config, models::CandleRecord};
use clickhouse::{error::Result, sql, Client, Row};

/// Clickhouse Clickhouse Database struct
pub struct ClickhouseDatabase {
    client: Client,
    url: String,
    db: String,
    user: String,
    password: String,
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
            url: conf.ch_url.clone(),
            db: conf.ch_db.clone(),
            user: conf.ch_user.clone(),
            password: conf.ch_password.clone(),
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
        self.client
            .query(
                "
                CREATE TABLE IF NOT EXISTS ?.securities(
                    secid String DEFAULT '',
                    boardid String DEFAULT '',
                    shortname String DEFAULT '',
                    status String DEFAULT '',
                    marketcode String DEFAULT '',
                )
                ENGINE = MergeTree
                PRIMARY KEY secid
                ",
            )
            .bind(sql::Identifier(self.db.as_str()))
            .execute()
            .await?;
        self.client
            .query(
                "
                CREATE TABLE IF NOT EXISTS ?.candles(
                    secid String DEFAULT '',
                    timeframe Int16 DEFAULT 0,
                    open Float64 DEFAULT 0.0,
                    close Float64 DEFAULT 0.0,
                    high Float64 DEFAULT 0.0,
                    low Float64 DEFAULT 0.0,
                    value Float64 DEFAULT 0.0,
                    volume Float64 DEFAULT 0.0,
                    begin DateTime DEFAULT now(),
                    end DateTime DEFAULT now(),
                )
                ENGINE = MergeTree
                PRIMARY KEY uuid
                ",
            )
            .bind(sql::Identifier(self.db.as_str()))
            .execute()
            .await?;
        Ok(())
    }

    /// Creates new security instance in database
    pub async fn create_security(&self, security: &Security) -> Result<()> {
        self.client
            .query("INSERT INTO ?.securities (*) VALUES ('?','?','?','?','?')")
            .bind(sql::Identifier(self.db.as_str()))
            .bind(sql::Identifier(security.secid.as_str()))
            .bind(sql::Identifier(security.boardid.as_str()))
            .bind(sql::Identifier(security.shortname.as_str()))
            .bind(sql::Identifier(security.status.as_str()))
            .bind(sql::Identifier(security.marketcode.as_str()))
            .execute()
            .await?;

        println!("Inserted security {} into db", security.secid);
        Ok(())
    }
    ///// Creates new security instance in database
    //pub async fn create_candle(&self, candle: &CandleRecord) -> Result<()> {
    //    self.client
    //        .query("INSERT INTO ?.candles (*) VALUES ('?','?','?','?','?')")
    //        .bind(sql::Identifier(self.db.as_str()))
    //        .bind(sql::Identifier(candle.end))
    //        .bind(sql::Identifier(candle.secid.as_str()))
    //        .execute()
    //        .await?;
    //
    //    println!("Inserted security {} into db", security.secid);
    //    Ok(())
    //}
}

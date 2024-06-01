use crate::config::Config;
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
                    secid String,
                    boardid String,
                    shortname String,
                    status String,
                    marketcode String,
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
                CREATE TABLE IF NOT EXISTS ?.candle(
                    timeframe Int16,
                    open Float64,
                    close Float64,
                    high Float64,
                    low Float64,
                    value Float64,
                    volume Float64,
                    begin DateTime,
                    end DateTime,
                )
                ENGINE = MergeTree
                PRIMARY KEY (begin,end)
                ",
            )
            .bind(sql::Identifier(self.db.as_str()))
            .execute()
            .await?;
        Ok(())
    }
}

use anselm_scribe::models::{get_all_securities, Security};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let securities = get_all_securities().await?;

    for mut sec in securities {
        println!("{sec:?}");
        sec.fetch_candles(1, "2020-01-01".to_string()).await?;
        for candle in &sec.candles {
            println!("{:?}", candle);
        }
    }

    Ok(())
}

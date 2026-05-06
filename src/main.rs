mod config;
mod decomposer;
mod strategies;
mod strategy;
mod util;

use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use tokio::fs::File;

use crate::config::*;
use crate::strategies::base_strategy::BaseStrategy;
use crate::strategy::Strategy;

#[tokio::main]
async fn main() {
    let config = get_config();

    println!("{:?}", config.schema.primary_key);

    let file = File::open("db/2026/05/titanic-2.parquet").await.unwrap();

    let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();
    println!("Converted arrow schema is: {}", builder.schema());

    let reader = builder.build().unwrap();

    BaseStrategy::encode_day(&config, 2026, 5, 2, reader).await;
}

//macro_rules! compare_datatype {}

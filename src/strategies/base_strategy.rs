use crate::decomposer::Decomposer;
use crate::strategy::Strategy;
use crate::util::RowReader;
use crate::util::*;
use crate::Config;
use arrow::compute::filter;
use arrow_array::BooleanArray;
use arrow_array::RecordBatch;
use futures::StreamExt;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_writer::AsyncArrowWriter;
use tokio::fs::File;

pub struct BaseStrategy {}

impl BaseStrategy {
    pub fn new() -> Self {
        Self {}
    }
}

impl Strategy for BaseStrategy {
    async fn encode_day<T>(
        config: &Config,
        year: usize,
        month: usize,
        day: usize,
        mut data_stream: ParquetRecordBatchStream<T>,
    ) where
        T: AsyncFileReader + Unpin + Send + 'static,
    {
        let decomposer = Decomposer::new(data_stream.schema(), &config.schema);

        let volatile_file = File::create(format!(
            "db/{:04}/{:02}/volatile/{:02}.parquet",
            year, month, day
        ))
        .await
        .unwrap();

        let non_volatile_file = File::create(format!(
            "db/{:04}/{:02}/non-volatile/{:02}.parquet",
            year, month, day
        ))
        .await
        .unwrap();

        let base_file = File::open(format!(
            "db/{:04}/{:02}/non-volatile/01.parquet",
            year, month
        ))
        .await
        .unwrap();

        let mut volatile_writer =
            AsyncArrowWriter::try_new(volatile_file, decomposer.get_volatile_schema(), None)
                .unwrap();
        let mut non_volatile_writer = AsyncArrowWriter::try_new(
            non_volatile_file,
            decomposer.get_non_volatile_schema(),
            None,
        )
        .unwrap();

        let base_file_reader = ParquetRecordBatchStreamBuilder::new(base_file)
            .await
            .unwrap()
            .build()
            .unwrap();

        let mut base_row_reader = RowReader::new(base_file_reader).await;

        while let Some(data_batch) = data_stream.next().await {
            let data_batch: RecordBatch = data_batch.unwrap();
            let (volatile, non_volatile) = decomposer.decompose(data_batch);

            /*
             * Determine necessary rows
             * Get columns from necessary rows
             * Create Recordbatch
             */
            let mut keep_rows = Vec::new();
            for row_index in 0..non_volatile.num_rows() {
                if let Some(base_batch) = base_row_reader.get_current() {
                    while is_before_row(
                        &base_batch,
                        &non_volatile,
                        base_row_reader.current_index,
                        row_index,
                        &config.schema,
                        // base(index) before data(index) get new base_index
                    ) {
                        base_row_reader.next().await;
                    }
                    if !equal_row(
                        &base_batch,
                        &non_volatile,
                        base_row_reader.current_index,
                        row_index,
                    ) {
                        keep_rows.push(true);
                    } else {
                        keep_rows.push(false);
                    }
                } else {
                    keep_rows.push(true);
                }
            }
            let filter_array = BooleanArray::from(keep_rows);
            let non_volatile_delta = RecordBatch::try_new(
                decomposer.get_non_volatile_schema(),
                non_volatile
                    .columns()
                    .iter()
                    .map(|array| filter(array.as_ref(), &filter_array).unwrap())
                    .collect(),
            )
            .unwrap();

            volatile_writer.write(&volatile).await.unwrap();
            non_volatile_writer
                .write(&non_volatile_delta)
                .await
                .unwrap();
        }
        volatile_writer.close().await.unwrap();
        non_volatile_writer.close().await.unwrap();
    }
}

use crate::Config;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::async_reader::ParquetRecordBatchStream;

pub trait Strategy {
    async fn encode_day<T>(
        config: &Config,
        year: usize,
        month: usize,
        day: usize,
        data_stream: ParquetRecordBatchStream<T>,
    ) where
        T: AsyncFileReader + Unpin + Send + 'static,
    {
    }
}

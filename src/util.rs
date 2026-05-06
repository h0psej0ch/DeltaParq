use crate::config::Schema;
use arrow_array::RecordBatch;
use arrow_schema::DataType;
use futures::StreamExt;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use std::sync::Arc;

macro_rules! compare_value {
    ($col1:expr, $col2:expr, $i1:expr, $i2:expr, $type:ty) => {
        $col1.as_any().downcast_ref::<$type>().unwrap().value($i1)
            == $col2.as_any().downcast_ref::<$type>().unwrap().value($i2)
    };
}

pub fn equal_row(
    rec1: &Arc<RecordBatch>,
    rec2: &RecordBatch,
    index1: usize,
    index2: usize,
) -> bool {
    rec1.columns().iter().enumerate().all(|(i, col1)| {
        let col2 = rec2.column(i);
        match col1.data_type() {
            DataType::Int64 => compare_value!(col1, col2, index1, index2, arrow_array::Int64Array),
            DataType::Utf8 => compare_value!(col1, col2, index1, index2, arrow_array::StringArray),
            DataType::Float64 => {
                compare_value!(col1, col2, index1, index2, arrow_array::Float64Array)
            }
            _ => false,
        }
    })
}

macro_rules! smaller_than_value {
    ($col1:expr, $col2:expr, $i1:expr, $i2:expr, $type:ty) => {
        $col1.as_any().downcast_ref::<$type>().unwrap().value($i1)
            < $col2.as_any().downcast_ref::<$type>().unwrap().value($i2)
    };
}

pub fn is_before_row(
    rec1: &Arc<RecordBatch>,
    rec2: &RecordBatch,
    index1: usize,
    index2: usize,
    config_schema: &Schema,
) -> bool {
    config_schema.primary_key.iter().any(|pkey| {
        let col1 = rec1.column_by_name(pkey).unwrap();
        let col2 = rec2.column_by_name(pkey).unwrap();

        match col1.data_type() {
            DataType::Int64 => {
                smaller_than_value!(col1, col2, index1, index2, arrow_array::Int64Array)
            }
            DataType::Utf8 => {
                smaller_than_value!(col1, col2, index1, index2, arrow_array::StringArray)
            }
            DataType::Float64 => {
                smaller_than_value!(col1, col2, index1, index2, arrow_array::Float64Array)
            }
            _ => false,
        }
    })
}

pub struct RowReader<T>
where
    T: AsyncFileReader + Unpin + Send + 'static,
{
    data_stream: ParquetRecordBatchStream<T>,
    pub current: Option<Arc<RecordBatch>>,
    pub current_index: usize,
}

impl<T> RowReader<T>
where
    T: AsyncFileReader + Unpin + Send + 'static,
{
    pub async fn new(mut data_stream: ParquetRecordBatchStream<T>) -> Self {
        let current = if let Some(current_batch) = data_stream.next().await {
            Some(Arc::new(current_batch.unwrap()))
        } else {
            None
        };
        Self {
            data_stream,
            current,
            current_index: 0,
        }
    }

    pub async fn next(&mut self) {
        if let Some(batch) = &self.current {
            self.current_index += 1;
            if self.current_index == batch.num_rows() {
                self.current_index = 0;
                self.current = if let Some(current_batch) = self.data_stream.next().await {
                    Some(Arc::new(current_batch.unwrap()))
                } else {
                    None
                };
            }
        }
    }
    pub fn get_current(&self) -> Option<Arc<RecordBatch>> {
        if let Some(batch) = &self.current {
            Some(batch.clone())
        } else {
            None
        }
    }
}

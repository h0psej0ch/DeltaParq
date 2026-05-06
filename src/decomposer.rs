use crate::config::Schema as ConfigSchema;
use arrow_array::RecordBatch;
use arrow_schema::Field;
use arrow_schema::Fields;
use arrow_schema::Schema;
use arrow_schema::SchemaRef;
use futures::StreamExt;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use parquet::arrow::async_writer::AsyncArrowWriter;
use std::sync::Arc;
use tokio::fs::File;

pub struct Decomposer {
    volatile: Vec<String>,
    volatile_schema: SchemaRef,
    non_volatile: Vec<String>,
    non_volatile_schema: SchemaRef,
}

impl Decomposer {
    pub fn new(schema: &SchemaRef, config_schema: &ConfigSchema) -> Self {
        let mut vol_fields: Vec<Arc<Field>> = Vec::new();
        let mut nvol_fields: Vec<Arc<Field>> = Vec::new();
        schema.fields().iter().for_each(|field| {
            if config_schema.primary_key.contains(field.name()) {
                vol_fields.push(field.clone());
                nvol_fields.push(field.clone());
            } else if config_schema.volatiles.contains(field.name()) {
                vol_fields.push(field.clone());
            } else {
                nvol_fields.push(field.clone());
            }
        });

        let volatile = vol_fields.iter().map(|f| f.name().clone()).collect();
        let non_volatile = nvol_fields.iter().map(|f| f.name().clone()).collect();

        println!("Vol: {:?}", volatile);
        println!("Non-Vol: {:?}", non_volatile);

        let volatile_schema = SchemaRef::new(Schema::new(Fields::from(vol_fields)));
        let non_volatile_schema = SchemaRef::new(Schema::new(Fields::from(nvol_fields)));

        Decomposer {
            volatile,
            volatile_schema,
            non_volatile,
            non_volatile_schema,
        }
    }

    pub fn get_volatile_schema(&self) -> SchemaRef {
        self.volatile_schema.clone()
    }

    pub fn get_non_volatile_schema(&self) -> SchemaRef {
        self.non_volatile_schema.clone()
    }

    pub fn decompose(&self, batch: RecordBatch) -> (RecordBatch, RecordBatch) {
        let vol_collumns = self
            .volatile
            .iter()
            .map(|name| batch.column_by_name(name.as_str()).unwrap().clone())
            .collect();

        let nvol_collumns = self
            .non_volatile
            .iter()
            .map(|name| batch.column_by_name(name.as_str()).unwrap().clone())
            .collect();

        println!("{}", self.non_volatile_schema);

        (
            RecordBatch::try_new(self.volatile_schema.clone(), vol_collumns).unwrap(),
            RecordBatch::try_new(self.non_volatile_schema.clone(), nvol_collumns).unwrap(),
        )
    }

    pub async fn decompose_parquet<T>(&self, mut stream: ParquetRecordBatchStream<T>)
    where
        T: AsyncFileReader + Unpin + Send + 'static,
    {
        let file = File::create("db/2026/05/volatile/02.parquet")
            .await
            .unwrap();
        let file2 = File::create("db/2026/05/non-volatile/02.parquet")
            .await
            .unwrap();
        let mut writer =
            AsyncArrowWriter::try_new(file, self.volatile_schema.clone(), None).unwrap();
        let mut writer2 =
            AsyncArrowWriter::try_new(file2, self.non_volatile_schema.clone(), None).unwrap();
        while let Some(batch) = stream.next().await {
            let batch: RecordBatch = batch.unwrap();
            let (volatile, non_volatile) = self.decompose(batch);
            writer.write(&volatile).await.unwrap();
            writer2.write(&non_volatile).await.unwrap();
        }
        writer.close().await.unwrap();
        writer2.close().await.unwrap();
    }
}

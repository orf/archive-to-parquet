use crate::batch::arrow_schema;
use crate::hasher::HASH_SIZE;
use crate::ConvertionOptions;
use arrow::array::{Array, AsArray, BooleanArray};
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use std::collections::HashSet;
use std::io::Write;

#[derive(Debug, Clone, Copy, Eq, PartialEq, clap::ValueEnum, strum::EnumString, strum::Display)]
#[strum(serialize_all = "lowercase", ascii_case_insensitive)]
#[derive(Default)]
pub enum IncludeType {
    #[default]
    All,
    Text,
    Binary,
}

pub fn new_parquet_writer<T: Write + Send>(
    writer: T,
    compression: Compression,
) -> parquet::errors::Result<ArrowWriter<T>> {
    let schema = arrow_schema();
    let mut props = WriterProperties::builder()
        .set_compression(compression)
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_dictionary_enabled(false)
        .set_bloom_filter_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_column_encoding("hash".into(), parquet::basic::Encoding::PLAIN)
        .set_write_batch_size(1024)
        .set_data_page_size_limit(1024 * 1024)
        .set_data_page_row_count_limit(20_00)
        .set_max_row_group_size(1024 * 1024);

    const BLOOM_FILTER_FIELDS: &[&str] = &["source", "path", "hash"];
    const STATISTICS_FIELDS: &[&str] = &["source", "path", "size", "hash"];
    const DICTIONARY_FIELDS: &[&str] = &["source", "path"];

    for field in BLOOM_FILTER_FIELDS {
        props = props.set_column_bloom_filter_enabled((*field).into(), true);
    }
    for field in STATISTICS_FIELDS {
        props = props.set_column_statistics_enabled((*field).into(), EnabledStatistics::Page);
    }
    for field in DICTIONARY_FIELDS {
        props = props.set_column_dictionary_enabled((*field).into(), true);
    }

    ArrowWriter::try_new(writer, schema, Some(props.build()))
}

pub struct ParquetSink<'a, T: Write + Send> {
    writer: &'a mut ArrowWriter<T>,
    seen_hashes: Option<HashSet<[u8; HASH_SIZE]>>,
}

impl<'a, T: Write + Send> ParquetSink<'a, T> {
    pub fn new(writer: &'a mut ArrowWriter<T>, options: ConvertionOptions) -> Self {
        let seen_hashes = if options.unique {
            Some(HashSet::new())
        } else {
            None
        };
        Self {
            writer,
            seen_hashes,
        }
    }

    fn deduplicate_batch(
        record_batch: RecordBatch,
        seen_hashes: &mut HashSet<[u8; HASH_SIZE]>,
    ) -> parquet::errors::Result<RecordBatch> {
        let hashes = record_batch
            .column_by_name("hash")
            .expect("hash column not found")
            .as_fixed_size_binary();
        let mut unique_indexes = Vec::new();
        assert_eq!(
            hashes.value_length(),
            HASH_SIZE as i32,
            "Hash column size != {HASH_SIZE}"
        );
        assert!(!hashes.is_nullable(), "Hash column is nullable");

        for (idx, hash) in hashes.iter().enumerate() {
            let hash: [u8; HASH_SIZE] = hash.unwrap().try_into().unwrap();
            if seen_hashes.insert(hash) {
                unique_indexes.push(idx);
            }
        }

        let select_mask = BooleanArray::from_iter(
            (0..record_batch.num_rows()).map(|idx| Some(unique_indexes.contains(&idx))),
        );

        Ok(filter_record_batch(&record_batch, &select_mask)?)
    }

    pub fn write_batch(&mut self, batch: RecordBatch) -> parquet::errors::Result<WriteBatchOutput> {
        let batch = match &mut self.seen_hashes {
            None => batch,
            Some(seen_hashes) => Self::deduplicate_batch(batch, seen_hashes)?,
        };

        let output = WriteBatchOutput {
            num_rows: batch.num_rows() as u64,
            bytes: batch.get_array_memory_size() as u64,
        };
        self.writer.write(&batch)?;
        Ok(output)
    }

    pub fn flush(&mut self) -> parquet::errors::Result<()> {
        self.writer.flush()
    }
}

#[derive(Debug)]
pub struct WriteBatchOutput {
    pub num_rows: u64,
    pub bytes: u64,
}

#[cfg(test)]
mod tests {
    use crate::IncludeType;
    use std::str::FromStr;

    #[test]
    fn test_include_type() {
        let include_type = IncludeType::from_str("all").unwrap();
        assert_eq!(include_type, IncludeType::All);
        let include_type = IncludeType::from_str("text").unwrap();
        assert_eq!(include_type, IncludeType::Text);
        let include_type = IncludeType::from_str("binary").unwrap();
        assert_eq!(include_type, IncludeType::Binary);
    }
}

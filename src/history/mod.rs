use anyhow::{Context, Result};
use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::sync::Arc;
use std::{collections::HashSet, fs, path::Path};

/// Load the set of processed ZIP filenames (without extension) from the history directory.
pub fn load_processed(history_dir: impl AsRef<Path>) -> Result<HashSet<String>> {
    let mut processed = HashSet::new();
    for entry in fs::read_dir(history_dir.as_ref())
        .with_context(|| format!("reading history directory {:?}", history_dir.as_ref()))?
    {
        let entry = entry?;
        if let Some(stem) = entry.path().file_stem().and_then(|s| s.to_str()) {
            processed.insert(stem.to_string());
        }
    }
    Ok(processed)
}

/// Record a processed ZIP by writing a Parquet file with its name to the history directory.
pub fn record_processed(history_dir: impl AsRef<Path>, name: &str) -> Result<()> {
    let history_dir = history_dir.as_ref();
    fs::create_dir_all(history_dir)
        .with_context(|| format!("creating history directory {:?}", history_dir))?;

    let hist_file = history_dir.join(format!("{}.parquet", name));
    let schema = Schema::new(vec![Field::new("zip_name", DataType::Utf8, false)]);
    let arr = StringArray::from(vec![name.to_string()]);
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(arr)])
        .context("creating record batch")?;
    let file = File::create(&hist_file)
        .with_context(|| format!("creating history file {:?}", hist_file))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props))
        .context("creating Arrow writer")?;
    writer.write(&batch).context("writing record batch")?;
    writer.close().context("closing Arrow writer")?;
    Ok(())
}

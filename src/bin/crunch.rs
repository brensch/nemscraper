use anyhow::{Context, Result};
use chrono::{FixedOffset, NaiveDate};
use clap::Parser;
use glob::glob;
use polars::datatypes::time_zone::{parse_fixed_offset, parse_time_zone};
use polars::lazy::dsl::concat;
use polars::prelude::*;
use std::fs::{create_dir_all, File};
use std::ops::Neg;
use std::path::PathBuf;
use tracing::info;

#[derive(Parser)]
#[command(
    author,
    version,
    about = "AWEFS Alternative Reality FPP Recalculation Pipeline (Step-by-Step)"
)]
struct Args {
    #[arg(short, long)]
    date: String,
    #[arg(long, default_value = "./assets/parquet")]
    input: String,
    #[arg(long, default_value = "./output")]
    output: String,
}

fn main() -> Result<()> {
    // Initialize logging to log info with the tracing package
    tracing_subscriber::fmt().with_env_filter("info").init();

    info!("Starting FPP pipeline...");

    let args = Args::parse();
    create_dir_all(&args.output)?;
    info!("Starting full FPP pipeline for date: {}", args.date);

    let fm_path = PathBuf::from(&args.output).join("01_frequency_measure.parquet");
    run_step_1_frequency_measure(&args.input, &args.date, &fm_path)?;

    let trajectory_path = PathBuf::from(&args.output).join("02_reference_trajectory.parquet");
    run_step_2_reference_trajectory(&args.input, &args.date, &trajectory_path)?;

    let deviation_path = PathBuf::from(&args.output).join("03_unit_deviations.parquet");
    run_step_3_unit_deviations(&args.input, &trajectory_path, &args.date, &deviation_path)?;

    let performance_path = PathBuf::from(&args.output).join("04_unit_performance.parquet");
    run_step_4_performance(&fm_path, &deviation_path, &performance_path)?;

    info!("Running Step 5: Final Charge Calculation (Placeholder)");
    let mut final_charges = run_step_5_final_charges(&performance_path)?;
    let final_path = PathBuf::from(&args.output).join("05_final_charges.parquet");
    ParquetWriter::new(&mut File::create(final_path)?).finish(&mut final_charges)?;

    info!("Pipeline finished successfully.");
    Ok(())
}

// Alternative helper function using lazy evaluation
fn read_parquet_files(pattern: &str) -> Result<DataFrame> {
    let paths: Vec<PathBuf> = glob(pattern)
        .context(format!("Failed to parse glob pattern: {}", pattern))?
        .filter_map(|p| p.ok())
        .collect();

    if paths.is_empty() {
        anyhow::bail!("No parquet files found matching pattern: {}", pattern);
    }

    info!("Found {} files matching pattern: {}", paths.len(), pattern);

    if paths.len() == 1 {
        // Single file case
        let df = LazyFrame::scan_parquet(&paths[0], ScanArgsParquet::default())
            .context(format!("Failed to scan parquet file: {:?}", paths[0]))?
            .collect()
            .context("Failed to collect lazy dataframe")?;
        Ok(df)
    } else {
        // Multiple files case - use lazy concat which handles schema differences better
        let lazy_frames: Result<Vec<LazyFrame>> = paths
            .iter()
            .map(|path| {
                LazyFrame::scan_parquet(path, ScanArgsParquet::default())
                    .context(format!("Failed to scan parquet file: {:?}", path))
            })
            .collect();

        let lazy_frames = lazy_frames?;

        // Concatenate lazy frames - this handles schema alignment automatically
        let result = concat(
            lazy_frames,
            UnionArgs {
                parallel: true,
                rechunk: false,
                to_supertypes: true, // This helps with type mismatches
                diagonal: true,      // This handles missing columns
                from_partitioned_ds: false,
                maintain_order: false,
            },
        )
        .context("Failed to concatenate lazy frames")?
        .collect()
        .context("Failed to collect concatenated dataframe")?;

        Ok(result)
    }
}

fn run_step_1_frequency_measure(input_dir: &str, date: &str, output_path: &PathBuf) -> Result<()> {
    info!("Running Step 1: Frequency Measure Calculation");
    let scada_path = format!(
        "{}/CAUSER_PAYS_SCADA---NETWORK---1/date={}/*.parquet",
        input_dir, date
    );

    let df = read_parquet_files(&scada_path)?;

    let mut result = df
        .lazy()
        .select([
            col("MEASUREMENTTIME").alias("ts"),
            col("FREQUENCYDEVIATION").alias("freq_dev"),
        ])
        .with_column(
            col("freq_dev")
                .neg()
                .ewm_mean(EWMOptions {
                    alpha: 0.05,
                    adjust: false,
                    ..Default::default()
                })
                .alias("freq_measure"),
        )
        .select([col("ts"), col("freq_measure")])
        .collect()?;

    ParquetWriter::new(&mut File::create(output_path)?).finish(&mut result)?;
    info!("Successfully saved to {:?}", output_path);
    Ok(())
}

fn run_step_2_reference_trajectory(
    input_dir: &str,
    date: &str,
    output_path: &PathBuf,
) -> Result<()> {
    info!("Running Step 2: Reference Trajectory Calculation");
    let targets_path = format!(
        "{}/DISPATCH---UNIT_SOLUTION---5/date={}/*.parquet",
        input_dir, date
    );

    let targets_df = read_parquet_files(&targets_path)?
        .lazy()
        .select([
            col("SETTLEMENTDATE").alias("ts_5m"), // Changed from INTERVAL_DATETIME to SETTLEMENTDATE
            col("DUID"),
            col("TOTALCLEARED"),
        ])
        .sort_by_exprs([col("ts_5m")], SortMultipleOptions::default())
        .collect()?;

    let start_date = NaiveDate::parse_from_str(date, "%Y-%m-%d")?;
    let start_naive = start_date.and_hms_opt(0, 0, 0).unwrap();
    let end_naive = start_naive + chrono::Duration::days(1) - chrono::Duration::seconds(4);

    let tz_name = parse_fixed_offset("+10:00")?;
    let tz = parse_time_zone(&tz_name)?;
    // Create time spine
    let ts_series = polars::time::date_range(
        "ts".into(),
        start_naive,
        end_naive,
        polars::prelude::Duration::parse("4s"),
        ClosedWindow::Both,
        TimeUnit::Milliseconds,
        Some(&tz),
    )?;

    let time_spine_df = DataFrame::new(vec![ts_series.into_series().into()])?;

    // Get unique DUIDs
    let duids_df = targets_df
        .clone()
        .lazy()
        .select([col("DUID")])
        .unique_stable(None, UniqueKeepStrategy::First)
        .collect()?;

    // Create base grid with cross join
    let base_grid = time_spine_df.cross_join(&duids_df, None, None)?;

    // Prepare previous targets
    let prev_targets = targets_df
        .clone()
        .lazy()
        .select([
            col("ts_5m"),
            col("DUID").alias("DUID_prev"), // Rename to avoid conflicts
            col("TOTALCLEARED").alias("prev_target"),
        ])
        .collect()?;

    // Join with previous targets
    let with_prev_target = base_grid
        .lazy()
        .join(
            prev_targets.lazy(),
            [col("ts").dt().truncate(lit("5m")), col("DUID")],
            [col("ts_5m"), col("DUID_prev")],
            JoinArgs::new(JoinType::Left),
        )
        .drop(["DUID_prev", "ts_5m"]) // Clean up unnecessary columns
        .collect()?;

    // Prepare next targets
    let next_targets = targets_df
        .lazy()
        .with_column(col("ts_5m") - lit(polars::prelude::Duration::parse("5m")))
        .select([
            col("ts_5m"),
            col("DUID").alias("DUID_next"), // Rename to avoid conflicts
            col("TOTALCLEARED").alias("next_target"),
        ])
        .collect()?;

    // Join with next targets
    let with_both_targets = with_prev_target
        .lazy()
        .join(
            next_targets.lazy(),
            [col("ts").dt().truncate(lit("5m")), col("DUID")],
            [col("ts_5m"), col("DUID_next")],
            JoinArgs::new(JoinType::Left),
        )
        .drop(["DUID_next", "ts_5m"]) // Clean up unnecessary columns
        .collect()?;

    // Calculate interpolation
    let interval_fraction = (col("ts").dt().timestamp(TimeUnit::Milliseconds)
        - col("ts")
            .dt()
            .truncate(lit("5m"))
            .dt()
            .timestamp(TimeUnit::Milliseconds))
    .cast(DataType::Float64)
        / lit(300_000.0);

    let mut trajectory_df = with_both_targets
        .lazy()
        .with_column(
            (col("prev_target").fill_null(0.0)
                + (col("next_target").fill_null(col("prev_target"))
                    - col("prev_target").fill_null(0.0))
                    * interval_fraction.fill_null(0.0))
            .alias("reference_mw"),
        )
        .select([col("ts"), col("DUID"), col("reference_mw")])
        .sort_by_exprs([col("ts"), col("DUID")], SortMultipleOptions::default())
        .collect()?;

    ParquetWriter::new(&mut File::create(output_path)?).finish(&mut trajectory_df)?;
    info!("Successfully saved to {:?}", output_path);
    Ok(())
}

fn run_step_3_unit_deviations(
    input_dir: &str,
    trajectory_path: &PathBuf,
    date: &str,
    output_path: &PathBuf,
) -> Result<()> {
    info!("Running Step 3: Unit Deviation Calculation");

    // Read trajectory data
    let trajectory_df = ParquetReader::new(&mut File::open(trajectory_path).context(format!(
        "Failed to open trajectory file: {:?}",
        trajectory_path
    ))?)
    .finish()
    .context(format!(
        "Failed to read trajectory parquet: {:?}",
        trajectory_path
    ))?;

    info!("Trajectory data shape: {:?}", trajectory_df.shape());

    // Debug: Show some trajectory data
    if trajectory_df.height() > 0 {
        info!("First few trajectory rows:");
        println!("{}", trajectory_df.head(Some(5)));
    }

    // Read unit MW data - let's first check what SCADA files exist
    let scada_path = format!("{}/FPP---UNIT_MW---1/date={}/*.parquet", input_dir, date);
    info!("Looking for SCADA files at: {}", scada_path);

    // Check if files exist
    let scada_files: Vec<_> = glob(&scada_path)
        .context("Failed to parse SCADA glob pattern")?
        .filter_map(|p| p.ok())
        .collect();

    info!("Found {} SCADA files", scada_files.len());

    if scada_files.is_empty() {
        // Let's check what FPP directories actually exist
        let fpp_base_path = format!("{}/FPP*", input_dir);
        let fpp_dirs: Vec<_> = glob(&fpp_base_path)
            .context("Failed to parse FPP glob pattern")?
            .filter_map(|p| p.ok())
            .collect();

        info!("Available FPP directories: {:?}", fpp_dirs);

        // Also check for any UNIT_MW related files
        let unit_mw_path = format!("{}/**/*UNIT_MW*", input_dir);
        let unit_mw_files: Vec<_> = glob(&unit_mw_path)
            .context("Failed to parse UNIT_MW glob pattern")?
            .filter_map(|p| p.ok())
            .take(10) // Just show first 10
            .collect();

        info!("Any UNIT_MW related files found: {:?}", unit_mw_files);

        anyhow::bail!("No SCADA files found at path: {}", scada_path);
    }

    let scada_df = read_parquet_files(&scada_path)?
        .lazy()
        .select([
            col("INTERVAL_DATETIME").alias("ts"), // This table has INTERVAL_DATETIME
            col("FPP_UNITID").alias("DUID"),      // Map FPP_UNITID to DUID
            col("MEASURED_MW").alias("SCADAVALUE"), // Map MEASURED_MW to SCADAVALUE
        ])
        .collect()?;

    info!("SCADA data shape: {:?}", scada_df.shape());

    // Debug: Show some SCADA data
    if scada_df.height() > 0 {
        info!("First few SCADA rows:");
        println!("{}", scada_df.head(Some(10)));
    }

    // right before your join in run_step_3:
    let ref_small = trajectory_df.clone().lazy().select([col("ts")]).collect()?;
    println!("Reference ts sample:\n{}", ref_small.head(Some(10)));

    let scada_small = scada_df.clone().lazy().select([col("ts")]).collect()?;
    println!("  SCADA ts sample:\n{}", scada_small.head(Some(10)));

    // Try LEFT join first to see what's not matching
    let joined_df = trajectory_df
        .clone()
        .lazy()
        .join(
            scada_df.clone().lazy(),
            [col("ts"), col("DUID")],
            [col("ts"), col("DUID")],
            JoinArgs::new(JoinType::Left), // Changed to LEFT join for debugging
        )
        .collect()?;

    info!("After LEFT join shape: {:?}", joined_df.shape());

    // Count non-null SCADAVALUE entries
    let non_null_count = joined_df.column("SCADAVALUE")?.null_count();
    let total_rows = joined_df.height();
    info!(
        "SCADAVALUE: {} non-null out of {} total rows ({:.2}% match rate)",
        total_rows - non_null_count,
        total_rows,
        (total_rows - non_null_count) as f64 / total_rows as f64 * 100.0
    );

    if total_rows - non_null_count == 0 {
        info!("No matches found! This suggests timestamp or DUID mismatch.");

        // Show sample of what we're trying to join
        if trajectory_df.height() > 0 {
            info!("Sample trajectory data for join debugging:");
            println!("{}", trajectory_df.select(["ts", "DUID"])?.head(Some(3)));
        }

        if scada_df.height() > 0 {
            info!("Sample SCADA data for join debugging:");
            println!("{}", scada_df.select(["ts", "DUID"])?.head(Some(3)));
        }
    }

    // Continue with INNER join for actual processing, but now we know why it might be empty
    let mut deviations_df = trajectory_df
        .clone()
        .lazy()
        .join(
            scada_df.clone().lazy(),
            [col("ts"), col("DUID")],
            [col("ts"), col("DUID")],
            JoinArgs::new(JoinType::Inner),
        )
        .with_column((col("SCADAVALUE") - col("reference_mw")).alias("deviation_mw"))
        .select([col("ts"), col("DUID"), col("deviation_mw")])
        .collect()?;

    info!("Final deviations shape: {:?}", deviations_df.shape());

    ParquetWriter::new(&mut File::create(output_path)?).finish(&mut deviations_df)?;
    info!("Successfully saved to {:?}", output_path);
    Ok(())
}
fn run_step_4_performance(
    fm_path: &PathBuf,
    deviation_path: &PathBuf,
    output_path: &PathBuf,
) -> Result<()> {
    info!("Running Step 4: Unit Performance Calculation");

    // Read frequency measure data
    let fm_df = ParquetReader::new(&mut File::open(fm_path).context(format!(
        "Failed to open frequency measure file: {:?}",
        fm_path
    ))?)
    .finish()
    .context(format!(
        "Failed to read frequency measure parquet: {:?}",
        fm_path
    ))?;

    // Read deviation data
    let deviation_df = ParquetReader::new(&mut File::open(deviation_path).context(format!(
        "Failed to open deviation file: {:?}",
        deviation_path
    ))?)
    .finish()
    .context(format!(
        "Failed to read deviation parquet: {:?}",
        deviation_path
    ))?;

    // Join and calculate performance
    let mut final_perf_df = fm_df
        .lazy()
        .join(
            deviation_df.lazy(),
            [col("ts")],
            [col("ts")],
            JoinArgs::new(JoinType::Inner),
        )
        .with_columns([
            when(col("freq_measure").gt(lit(0.0)))
                .then(col("freq_measure") * col("deviation_mw"))
                .otherwise(lit(0.0))
                .alias("raise_perf"),
            when(col("freq_measure").lt(lit(0.0)))
                .then(col("freq_measure") * col("deviation_mw"))
                .otherwise(lit(0.0))
                .alias("lower_perf"),
        ])
        .select([col("ts"), col("DUID"), col("raise_perf"), col("lower_perf")])
        .collect()?;

    ParquetWriter::new(&mut File::create(output_path)?).finish(&mut final_perf_df)?;
    info!("Successfully saved to {:?}", output_path);
    Ok(())
}

fn run_step_5_final_charges(performance_path: &PathBuf) -> Result<DataFrame> {
    info!("Running Step 5: Final Charge Calculation (Placeholder)");

    // Read performance data
    let performance_df = ParquetReader::new(&mut File::open(performance_path).context(format!(
        "Failed to open performance file: {:?}",
        performance_path
    ))?)
    .finish()
    .context(format!(
        "Failed to read performance parquet: {:?}",
        performance_path
    ))?;

    Ok(performance_df)
}

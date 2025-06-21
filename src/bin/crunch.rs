use anyhow::{bail, Context, Result};
use chrono::FixedOffset;
use chrono::{Duration as ChronoDuration, NaiveDate};
use clap::Parser;
use glob::glob;
use polars::datatypes::time_zone::{parse_fixed_offset, parse_time_zone};
use polars::lazy::dsl::concat;
use polars::prelude::*;
use tracing::{warn, Level};

use std::fs::{self, create_dir_all, File};
use std::ops::Neg;
use std::path::PathBuf;
// use time_tz::{parse_fixed_offset, parse_time_zone};
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

    let trajectory_path = PathBuf::from(&args.output).join("02_reference_trajectories.parquet");
    run_step_2_reference_trajectory(&args.input, &args.date, &trajectory_path)?;

    let deviation_path = PathBuf::from(&args.output).join("03_unit_deviations.parquet");
    run_step_3_unit_deviations(&args.input, &args.date, &deviation_path, &trajectory_path)?;

    let performance_path = PathBuf::from(&args.output).join("05_unit_performance_proof.parquet");
    run_fpp_performance_calculation(&args.input, &args.date, &performance_path)?;

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

// note i'm just recomputing for the sake of it here. i actually don't think we need to do this since it doesn't change
// whether we submitted or awefs.
// FPP computed values are in:
// FPP---REGION_FREQ_MEASURE---1
// Columns: FREQ_MEASURE_HZ, FREQ_DEVIATION_HZ
fn run_step_1_frequency_measure(input_dir: &str, date: &str, output_path: &PathBuf) -> Result<()> {
    info!("Running Step 1: Region‐level Frequency Measure with explicit EWMA");

    // glob over the FPP regional freq‐measure files for this date
    let scada_pattern = format!(
        "{}/FPP---REGION_FREQ_MEASURE---1/date={}/*.parquet",
        input_dir, date
    );
    info!("Reading regional frequency data from: {}", scada_pattern);

    // https://aemo.com.au/-/media/files/stakeholder_consultation/consultations/nem-consultations/2022/frequency-contribution-factors-procedure/final-documents/turning-parameters-and-input-sources.pdf?la=en
    let alpha = 2.0 / 9.0;

    // read & concat all the parquet files
    let mut df = read_parquet_files(&scada_pattern)?
        .lazy()
        // keep only good‐quality samples
        .filter(col("HZ_QUALITY_FLAG").eq(lit(1)))
        // select the raw fields
        .select([
            col("MEASUREMENT_DATETIME").alias("ts"),
            col("REGIONID").alias("region"),
            col("FREQ_DEVIATION_HZ").alias("freq_dev"),
            col("FREQ_MEASURE_HZ").alias("aemo_freq_measure"),
        ])
        .collect()
        .context("Failed to collect regional frequency‐measure DataFrame")?;

    // Sort by region and timestamp to ensure proper order for EWMA calculation
    df = df.sort(["region", "ts"], SortMultipleOptions::default())?;

    // Apply explicit EWMA formula: FM_t = (1 - α) * FM_{t-1} + α * (-FD_t)
    let freq_dev_series = df.column("freq_dev")?.f64()?;
    let region_series = df.column("region")?.str()?;

    let mut freq_measure_values = Vec::with_capacity(df.height());
    let mut current_region: Option<&str> = None;
    let mut fm_prev = 0.0; // Initialize FM_{t-1} to 0 for first value in each region

    for i in 0..df.height() {
        let region = region_series.get(i).unwrap();
        let freq_dev = freq_dev_series.get(i);

        // Reset accumulator when we encounter a new region
        if current_region != Some(region) {
            current_region = Some(region);
            fm_prev = 0.0;
        }

        if let Some(fd) = freq_dev {
            // Apply the explicit EWMA formula: FM_t = (1 - α) * FM_{t-1} + α * (-FD_t)
            let fm_current = (1.0 - alpha) * fm_prev + alpha * (-fd);
            freq_measure_values.push(Some(fm_current));
            fm_prev = fm_current;
        } else {
            // Handle null values
            freq_measure_values.push(None);
        }
    }

    // Add the calculated frequency measure column to the dataframe
    let freq_measure_series = Series::new("freq_measure".into(), freq_measure_values);
    df.with_column(freq_measure_series)?;

    // Select final layout: timestamp, region, raw deviation, AEMO's measure, explicit EWMA
    let mut df = df
        .lazy()
        .select([
            col("ts"),
            col("region"),
            col("freq_dev"),
            col("aemo_freq_measure"),
            col("freq_measure"),
        ])
        .collect()
        .context("Failed to collect final DataFrame")?;

    // write to Parquet
    let mut out =
        File::create(output_path).context(format!("Could not create {:?}", output_path))?;
    ParquetWriter::new(&mut out)
        .finish(&mut df)
        .context("Failed to write regional frequency‐measure parquet")?;

    info!(
        "Saved regional freq_measure + comparison to {:?}",
        output_path
    );
    Ok(())
}

/// Builds a combined reference trajectory for all units in the NEM.
/// Reference trajectories are the points between 5-minute forecasts, interpolated to 4-second resolution.
/// Note these currently don't match up, which is concerning.
fn run_step_2_reference_trajectory(
    input_dir: &str,
    date: &str,
    output_path: &PathBuf,
) -> anyhow::Result<()> {
    info!("Step 2: building combined reference trajectory");

    let pred_path = format!(
        "{}/DEMAND---INTERMITTENT_DS_PRED---1/date={}/*.parquet",
        input_dir, date
    );

    // 1) Scan and filter the dataset once to get the latest, highest-priority forecasts.
    let latest_forecasts_lf = LazyFrame::scan_parquet(&pred_path, ScanArgsParquet::default())?
        .filter(col("ORIGIN").eq(lit("AWEFS_ASEFS")))
        .sort_by_exprs(
            vec![col("FORECAST_PRIORITY"), col("OFFERDATETIME")],
            SortMultipleOptions {
                descending: vec![true, true],
                ..Default::default()
            },
        )
        .unique(
            Some(vec![
                "RUN_DATETIME".to_string(),
                "DUID".to_string(),
                "ORIGIN".to_string(),
            ]),
            UniqueKeepStrategy::First,
        )
        .cache();

    // 2) Build the 4s time spine once.
    let start_dt = NaiveDate::parse_from_str(date, "%Y-%m-%d")?
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let end_dt = start_dt + ChronoDuration::days(1) - ChronoDuration::seconds(4);
    let tz_name = "+10:00"; // NEM time
    let tz_parsed = {
        let tz_string = parse_fixed_offset(tz_name)?;
        parse_time_zone(&tz_string)?
    };
    let time_spine = polars::time::date_range(
        "ts".into(),
        start_dt,
        end_dt,
        polars::prelude::Duration::parse("4s"),
        polars::prelude::ClosedWindow::Both,
        polars::prelude::TimeUnit::Milliseconds,
        Some(&tz_parsed),
    )?;
    let time_spine_df = DataFrame::new(vec![time_spine.into_series().into()])?;

    // 3) Get all unique (DUID, ORIGIN) pairs from the entire filtered dataset.
    info!("Finding all unique DUID/ORIGIN pairs.");
    let duid_origin_lf = latest_forecasts_lf
        .clone()
        .select([col("DUID"), col("ORIGIN")])
        .unique_stable(None, UniqueKeepStrategy::First);

    // 4) Create a base grid by cross-joining the time spine with all DUID/ORIGIN pairs.
    info!("Creating base time-series grid for all entities.");
    let base_lf = time_spine_df.lazy().cross_join(duid_origin_lf, None);

    // 5) Prepare the forecast data, selecting only the necessary columns.
    let forecast_data_lf = latest_forecasts_lf.select([
        col("INTERVAL_DATETIME").alias("ts_5m"),
        col("DUID"),
        col("ORIGIN"),
        col("FORECAST_POE50").alias("target_mw"),
    ]);

    // ================== CHANGE START ==================

    // 6) Create 'prev' and 'next' frames, renaming the target column BEFORE the join.
    let prev_lf = forecast_data_lf.clone().select([
        col("ts_5m"),
        col("DUID"),
        col("ORIGIN"),
        col("target_mw").alias("prev_target"),
    ]);

    let next_lf = forecast_data_lf
        .clone()
        .with_column((col("ts_5m") - lit(polars::prelude::Duration::parse("5m"))).alias("ts_5m"))
        .select([
            col("ts_5m"),
            col("DUID"),
            col("ORIGIN"),
            col("target_mw").alias("next_target"),
        ]);

    // 7) Join the base grid. CRITICAL FIX: Add a unique suffix for each join to prevent collision.
    info!("Joining forecast data to the time-series grid.");
    let joined_lf = base_lf
        .join(
            prev_lf,
            [
                col("ts").dt().truncate(lit("5m")),
                col("DUID"),
                col("ORIGIN"),
            ],
            [col("ts_5m"), col("DUID"), col("ORIGIN")],
            JoinArgs {
                how: JoinType::Left,
                suffix: Some("_prev".into()), // Add unique suffix
                ..Default::default()
            },
        )
        .join(
            next_lf,
            [
                col("ts").dt().truncate(lit("5m")),
                col("DUID"),
                col("ORIGIN"),
            ],
            [col("ts_5m"), col("DUID"), col("ORIGIN")],
            JoinArgs {
                how: JoinType::Left,
                suffix: Some("_next".into()), // Add unique suffix
                ..Default::default()
            },
        );

    // ================== CHANGE END ==================

    // 8) Interpolate between the 5-minute forecast points for the entire dataset.
    info!("Interpolating to 4-second resolution.");
    let frac = {
        let ms = col("ts").dt().timestamp(TimeUnit::Milliseconds);
        let floored = col("ts")
            .dt()
            .truncate(lit("5m"))
            .dt()
            .timestamp(TimeUnit::Milliseconds);
        (ms - floored).cast(DataType::Float64) / lit(300_000.0)
    };

    // The final `select` will discard the intermediate, suffixed columns like `DUID_prev`.
    let final_lf = joined_lf
        .with_column(
            (col("prev_target").fill_null(0.0)
                + (col("next_target").fill_null(col("prev_target"))
                    - col("prev_target").fill_null(0.0))
                    * frac.fill_null(0.0))
            .alias("reference_mw"),
        )
        .select([col("ts"), col("DUID"), col("ORIGIN"), col("reference_mw")])
        .sort_by_exprs(
            [col("ts"), col("DUID"), col("ORIGIN")],
            SortMultipleOptions::default(),
        );

    // 9) Collect the result and write to a single Parquet file.
    info!("Collecting final DataFrame to write to single file...");
    let mut df_out = final_lf.collect()?;

    info!("Writing to {:?}", output_path);
    let mut f = File::create(&output_path)?;
    ParquetWriter::new(&mut f)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut df_out)?;

    info!("Successfully wrote combined trajectory file.");
    Ok(())
}

/// Calculates the deviation between the reference trajectory and actual unit power measurements.
///
/// This function assumes the following inputs exist from previous steps:
/// - A single reference trajectory file at `{input_dir}/02_reference_trajectories.parquet`.
/// - Unit measurement (SCADA) files at `{input_dir}/FPP---UNIT_MW---1/date={date}/*.parquet`.
///
/// It performs an inner join on timestamp and DUID, calculates the deviation, and saves
/// the result to the specified output path.
/// Calculates the deviation between the reference trajectory and actual unit power measurements.
fn run_step_3_unit_deviations(
    input_dir: &str,
    date: &str,
    output_path: &PathBuf,
    trajectory_path: &PathBuf,
) -> Result<()> {
    info!("Running Step 3: Unit Deviation Calculation");

    // 1. Calculate previous and next dates to expand the read window.
    let current_date =
        NaiveDate::parse_from_str(date, "%Y-%m-%d").context("Failed to parse date string")?;
    let prev_date = current_date - chrono::Duration::days(1);
    let next_date = current_date + chrono::Duration::days(1);

    // 2. Lazily scan the reference trajectory file.
    info!("Scanning trajectory data from: {:?}", trajectory_path);
    let trajectory_lf = LazyFrame::scan_parquet(trajectory_path, Default::default()).context(
        format!("Failed to scan trajectory parquet: {:?}", trajectory_path),
    )?;

    // 3. Scan each date partition individually and then concatenate them into a single LazyFrame.
    // This is more robust than using a single complex glob string with brace expansion.
    info!(
        "Scanning unit MW data from partitions: {}, {}, {}",
        prev_date, date, next_date
    );
    let scada_frames_to_concat = vec![
        LazyFrame::scan_parquet(
            &format!(
                "{}/FPP---UNIT_MW---1/date={}/*.parquet",
                input_dir,
                prev_date.format("%Y-%m-%d")
            ),
            Default::default(),
        )
        .context("Scan failed for previous day")?,
        LazyFrame::scan_parquet(
            &format!("{}/FPP---UNIT_MW---1/date={}/*.parquet", input_dir, date),
            Default::default(),
        )
        .context("Scan failed for current day")?,
        LazyFrame::scan_parquet(
            &format!(
                "{}/FPP---UNIT_MW---1/date={}/*.parquet",
                input_dir,
                next_date.format("%Y-%m-%d")
            ),
            Default::default(),
        )
        .context("Scan failed for next day")?,
    ];

    let scada_lf = concat(&scada_frames_to_concat, Default::default())?.select([
        col("MEASUREMENT_DATETIME").alias("ts"),
        col("FPP_UNITID").alias("DUID"),
        col("MEASURED_MW"),
    ]);

    // 4. Perform a standard INNER JOIN on the exact timestamp and DUID.
    info!("Performing INNER join to align SCADA data to trajectory...");
    let deviations_lf = trajectory_lf
        .join(
            scada_lf,
            [col("ts"), col("DUID")],
            [col("ts"), col("DUID")],
            JoinArgs::new(JoinType::Inner),
        )
        .with_column((col("MEASURED_MW") - col("reference_mw")).alias("deviation_mw"))
        .select([col("ts"), col("DUID"), col("ORIGIN"), col("deviation_mw")])
        .sort(["ts", "ORIGIN", "DUID"], Default::default());

    // 5. Collect the final result into a DataFrame.
    info!("Collecting final deviations DataFrame...");
    let mut final_df = deviations_lf.collect()?;
    info!("Final deviations shape: {:?}", final_df.shape());

    // 6. Write the result to the specified output file if it's not empty.
    if !final_df.is_empty() {
        info!("Writing final deviations to: {:?}", output_path);
        ParquetWriter::new(&mut File::create(output_path)?)
            .with_compression(ParquetCompression::Snappy)
            .finish(&mut final_df)?;
        info!("Successfully completed Step 3.");
    } else {
        info!("Join resulted in an empty DataFrame. No file written.");
    }

    Ok(())
}

/// Calculates FPP raise/lower performance components using pre-calculated deviations.
///
/// This function reads AEMO's official data for unit deviation and frequency measure,
/// then for each 4-second interval, calculates:
/// - `p_raise = max(0, FREQ_MEASURE_HZ) * DEVIATION_MW`
/// - `p_lower = min(0, FREQ_MEASURE_HZ) * DEVIATION_MW`
///
/// The results are saved to the specified output path.
fn run_fpp_performance_calculation(
    input_dir: &str,
    date: &str,
    output_path: &PathBuf,
) -> Result<()> {
    info!("Running FPP Raise/Lower Performance Calculation");

    // 1. Define the date window.
    let current_date =
        NaiveDate::parse_from_str(date, "%Y-%m-%d").context("Failed to parse date string")?;
    let prev_date = current_date - chrono::Duration::days(1);
    let next_date = current_date + chrono::Duration::days(1);

    // 2. Scan the 4-second FPP Unit MW data to get DEVIATION_MW.
    info!(
        "Scanning FPP Unit MW data from partitions: {}, {}, {}",
        prev_date, date, next_date
    );
    let unit_mw_paths: Vec<PathBuf> = [prev_date, current_date, next_date]
        .iter()
        .map(|d| {
            PathBuf::from(format!(
                "{}/FPP---UNIT_MW---1/date={}/*.parquet",
                input_dir,
                d.format("%Y-%m-%d")
            ))
        })
        .collect();

    let unit_mw_lf = LazyFrame::scan_parquet_files(unit_mw_paths.into(), Default::default())?
        .select([
            col("MEASUREMENT_DATETIME").alias("ts"),
            col("FPP_UNITID").alias("DUID"),
            col("DEVIATION_MW"),
            col("MEASURED_MW"),
        ]);

    // 3. Scan the 4-second frequency measure data.
    info!(
        "Scanning Frequency Measure data from partitions: {}, {}, {}",
        prev_date, date, next_date
    );
    let fm_paths: Vec<PathBuf> = [prev_date, current_date, next_date]
        .iter()
        .map(|d| {
            PathBuf::from(format!(
                "{}/FPP---REGION_FREQ_MEASURE---1/date={}/*.parquet",
                input_dir,
                d.format("%Y-%m-%d")
            ))
        })
        .collect();

    let fm_lf = LazyFrame::scan_parquet_files(fm_paths.into(), Default::default())?
        .filter(col("REGIONID").eq(lit("NSW1")))
        .select([
            col("MEASUREMENT_DATETIME").alias("ts"),
            col("FREQ_MEASURE_HZ"),
        ]);

    // 4. Join unit data with frequency measure.
    info!("Performing LEFT join to align Frequency Measure to unit data...");
    let performance_lf = unit_mw_lf
        .join(
            fm_lf,
            [col("ts")],
            [col("ts")],
            JoinArgs::new(JoinType::Left),
        )
        // --- CHANGE START: Calculate p_lower and p_raise ---
        .with_columns([
            // Calculate p_lower = min(0, FM) * Dev
            (col("FREQ_MEASURE_HZ")
                .fill_null(0.0)
                .clip_max(lit(0.0)) // This performs min(0, FM)
                * col("DEVIATION_MW"))
            .alias("p_lower"),
            // Calculate p_raise = max(0, FM) * Dev
            (col("FREQ_MEASURE_HZ")
                .fill_null(0.0)
                .clip_min(lit(0.0)) // This performs max(0, FM)
                * col("DEVIATION_MW"))
            .alias("p_raise"),
        ])
        .select([
            col("ts"),
            col("DUID"),
            col("MEASURED_MW"),
            col("DEVIATION_MW"),
            col("FREQ_MEASURE_HZ"), // Keep original FM for context
            col("p_lower"),
            col("p_raise"),
        ])
        // --- CHANGE END ---
        .sort(["ts", "DUID"], Default::default());

    // 5. Collect and write the final result.
    info!("Collecting final performance DataFrame...");
    let mut final_df = performance_lf.collect()?;
    info!("Final performance shape: {:?}", final_df.shape());

    if !final_df.is_empty() {
        info!("Writing final performance data to: {:?}", output_path);
        ParquetWriter::new(&mut File::create(output_path)?)
            .with_compression(ParquetCompression::Snappy)
            .finish(&mut final_df)?;
        info!("Successfully completed FPP Performance Calculation.");
    } else {
        info!("Join resulted in an empty DataFrame. No performance file written.");
    }

    Ok(())
}

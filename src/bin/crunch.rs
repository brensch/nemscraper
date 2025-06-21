use anyhow::{Context, Result};
use chrono::{Duration as ChronoDuration, NaiveDate};
use clap::Parser;
use glob::glob;
// Added imports needed for the restored/fixed functions
use polars::datatypes::time_zone::{parse_fixed_offset, parse_time_zone};
use polars::lazy::dsl::concat;
use polars::prelude::*;
use std::fs::{create_dir_all, File};
use std::path::PathBuf;
use tracing::info;

#[derive(Parser)]
#[command(
    author,
    version,
    about = "AWEFS Alternative Reality vs. Actual FPP Performance Pipeline"
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
    tracing_subscriber::fmt().with_env_filter("info").init();
    info!("Starting FPP pipeline...");

    let args = Args::parse();
    create_dir_all(&args.output)?;
    info!("Starting FPP pipeline for date: {}", args.date);

    // --- PIPELINE ORCHESTRATION ---

    // STEP 1: Calculate region-level frequency measure with explicit EWMA
    let fm_path = PathBuf::from(&args.output).join("01_frequency_measure.parquet");
    run_step_1_frequency_measure(&args.input, &args.date, &fm_path)?;

    // STEP 2: Generate the hypothetical AWEFS trajectory
    info!("--- Starting Hypothetical AWEFS Scenario ---");
    let trajectory_path = PathBuf::from(&args.output).join("02_hypothetical_trajectory.parquet");
    run_step_2_reference_trajectory(&args.input, &args.date, &trajectory_path)?;

    // STEP 3: Calculate deviation for the hypothetical trajectory
    let hypothetical_deviation_path =
        PathBuf::from(&args.output).join("03_hypothetical_deviations.parquet");
    run_step_3_unit_deviations(
        &args.input,
        &args.date,
        &hypothetical_deviation_path,
        &trajectory_path,
    )?;

    // STEP 4A: Calculate performance for the HYPOTHETICAL deviations
    info!("--- Calculating Performance for Hypothetical AWEFS Deviations ---");
    let hypothetical_deviations_lf =
        LazyFrame::scan_parquet(&hypothetical_deviation_path, Default::default())?
            // We only need these columns for the calculation
            .select([col("ts"), col("DUID"), col("deviation_mw")]);

    let hypothetical_performance_path =
        PathBuf::from(&args.output).join("04_hypothetical_performance.parquet");
    run_performance_calculation(
        &args.input,
        &args.date,
        hypothetical_deviations_lf,
        &hypothetical_performance_path,
    )?;

    // STEP 4B: Calculate performance for the ACTUAL AEMO deviations
    info!("--- Calculating Performance for Actual AEMO Deviations ---");
    let actual_deviations_lf = get_actual_deviations_lf(&args.input, &args.date)?;
    let actual_performance_path = PathBuf::from(&args.output).join("05_actual_performance.parquet");
    run_performance_calculation(
        &args.input,
        &args.date,
        actual_deviations_lf,
        &actual_performance_path,
    )?;

    info!("Pipeline finished successfully.");
    Ok(())
}

// ===============================================================================================
// CORE REUSABLE FUNCTION
// ===============================================================================================

/// A generic function to calculate FPP performance from any source of deviation data.
fn run_performance_calculation(
    input_dir: &str,
    date: &str,
    deviations_lf: LazyFrame,
    output_path: &PathBuf,
) -> Result<()> {
    info!(
        "Running generic performance calculation, saving to: {:?}",
        output_path
    );

    let current_date =
        NaiveDate::parse_from_str(date, "%Y-%m-%d").context("Failed to parse date string")?;
    let prev_date = current_date - ChronoDuration::days(1);
    let next_date = current_date + ChronoDuration::days(1);

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

    let performance_lf = deviations_lf
        .join(
            fm_lf,
            [col("ts")],
            [col("ts")],
            JoinArgs::new(JoinType::Left),
        )
        .with_columns([
            (when(col("FREQ_MEASURE_HZ").lt(lit(0.0)))
                .then(col("FREQ_MEASURE_HZ"))
                .otherwise(lit(0.0))
                * col("deviation_mw"))
            .alias("p_lower"),
            (when(col("FREQ_MEASURE_HZ").gt(lit(0.0)))
                .then(col("FREQ_MEASURE_HZ"))
                .otherwise(lit(0.0))
                * col("deviation_mw"))
            .alias("p_raise"),
        ])
        .select([
            col("ts"),
            col("DUID"),
            col("deviation_mw"),
            col("FREQ_MEASURE_HZ"),
            col("p_lower"),
            col("p_raise"),
        ])
        .sort(["ts", "DUID"], Default::default());

    let mut final_df = performance_lf.collect()?;
    info!("Final performance shape: {:?}", final_df.shape());

    if !final_df.is_empty() {
        ParquetWriter::new(&mut File::create(output_path)?)
            .with_compression(ParquetCompression::Snappy)
            .finish(&mut final_df)?;
        info!("Successfully wrote performance data to {:?}", output_path);
    } else {
        info!(
            "Join resulted in an empty DataFrame. No performance file written to {:?}",
            output_path
        );
    }

    Ok(())
}

// ===============================================================================================
// HELPER AND STEP-SPECIFIC FUNCTIONS
// ===============================================================================================

/// (RESTORED) Helper function to read multiple parquet files that might have schema differences.
fn read_parquet_files(pattern: &str) -> Result<DataFrame> {
    let paths: Vec<PathBuf> = glob(pattern)
        .context(format!("Failed to parse glob pattern: {}", pattern))?
        .filter_map(|p| p.ok())
        .collect();

    if paths.is_empty() {
        anyhow::bail!("No parquet files found matching pattern: {}", pattern);
    }

    info!("Found {} files matching pattern: {}", paths.len(), pattern);

    let lazy_frames: Result<Vec<LazyFrame>> = paths
        .iter()
        .map(|path| {
            LazyFrame::scan_parquet(path, Default::default())
                .context(format!("Failed to scan parquet file: {:?}", path))
        })
        .collect();

    let result = concat(
        lazy_frames?,
        UnionArgs {
            parallel: true,
            rechunk: true, // Set to true for better performance after concat
            to_supertypes: true,
            ..Default::default()
        },
    )
    .context("Failed to concatenate lazy frames")?
    .collect()
    .context("Failed to collect concatenated dataframe")?;

    Ok(result)
}

/// (RESTORED) Calculates region frequency measure using an explicit EWMA loop.
fn run_step_1_frequency_measure(input_dir: &str, date: &str, output_path: &PathBuf) -> Result<()> {
    info!("Running Step 1: Region-level Frequency Measure with explicit EWMA");

    let scada_pattern = format!(
        "{}/FPP---REGION_FREQ_MEASURE---1/date={}/*.parquet",
        input_dir, date
    );
    info!("Reading regional frequency data from: {}", scada_pattern);

    let alpha = 2.0 / 9.0;

    let mut df = read_parquet_files(&scada_pattern)?
        .lazy()
        .filter(col("HZ_QUALITY_FLAG").eq(lit(1)))
        .select([
            col("MEASUREMENT_DATETIME").alias("ts"),
            col("REGIONID").alias("region"),
            col("FREQ_DEVIATION_HZ").alias("freq_dev"),
            col("FREQ_MEASURE_HZ").alias("aemo_freq_measure"),
        ])
        .collect()
        .context("Failed to collect regional frequency-measure DataFrame")?;

    df = df.sort(["region", "ts"], SortMultipleOptions::default())?;

    let freq_dev_series = df.column("freq_dev")?.f64()?;
    let region_series = df.column("region")?.str()?;
    let mut freq_measure_values = Vec::with_capacity(df.height());
    let mut current_region: Option<&str> = None;
    let mut fm_prev = 0.0;

    for i in 0..df.height() {
        let region = region_series.get(i).unwrap();
        let freq_dev = freq_dev_series.get(i);

        if current_region != Some(region) {
            current_region = Some(region);
            fm_prev = 0.0;
        }

        if let Some(fd) = freq_dev {
            let fm_current = (1.0 - alpha) * fm_prev + alpha * (-fd);
            freq_measure_values.push(Some(fm_current));
            fm_prev = fm_current;
        } else {
            freq_measure_values.push(None);
        }
    }

    let freq_measure_series = Series::new("freq_measure".into(), freq_measure_values);
    df.with_column(freq_measure_series)?;

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

    let mut out =
        File::create(output_path).context(format!("Could not create {:?}", output_path))?;
    ParquetWriter::new(&mut out)
        .finish(&mut df)
        .context("Failed to write regional frequency-measure parquet")?;

    info!(
        "Saved regional freq_measure + comparison to {:?}",
        output_path
    );
    Ok(())
}

/// Helper to load the "actual" deviation data directly from FPP---UNIT_MW---1.
fn get_actual_deviations_lf(input_dir: &str, date: &str) -> Result<LazyFrame> {
    let current_date =
        NaiveDate::parse_from_str(date, "%Y-%m-%d").context("Failed to parse date string")?;
    let prev_date = current_date - ChronoDuration::days(1);
    let next_date = current_date + ChronoDuration::days(1);

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

    let lf = LazyFrame::scan_parquet_files(unit_mw_paths.into(), Default::default())?.select([
        col("MEASUREMENT_DATETIME").alias("ts"),
        col("FPP_UNITID").alias("DUID"),
        col("DEVIATION_MW").alias("deviation_mw"),
    ]);
    Ok(lf)
}

/// Builds a combined reference trajectory for AWEFS units.
fn run_step_2_reference_trajectory(
    input_dir: &str,
    date: &str,
    output_path: &PathBuf,
) -> Result<()> {
    info!("Step 2: Building AWEFS reference trajectory");

    let pred_path = format!(
        "{}/DEMAND---INTERMITTENT_DS_PRED---1/date={}/*.parquet",
        input_dir, date
    );

    let latest_forecasts_lf = LazyFrame::scan_parquet(&pred_path, Default::default())?
        .filter(col("ORIGIN").eq(lit("AWEFS_ASEFS")))
        .sort_by_exprs(
            &[col("RUN_DATETIME")],
            // Note: with_order_descending(true) also works and is slightly cleaner for a single bool
            SortMultipleOptions::default().with_order_descending(true),
        )
        .unique(
            Some(vec!["DUID".to_string(), "INTERVAL_DATETIME".to_string()]),
            UniqueKeepStrategy::First,
        )
        .cache();

    let start_dt = NaiveDate::parse_from_str(date, "%Y-%m-%d")?
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let end_dt = start_dt + ChronoDuration::days(1) - ChronoDuration::seconds(4);

    let tz_name = "+10:00";
    let tz_parsed = {
        let tz_string = parse_fixed_offset(tz_name)?;
        parse_time_zone(&tz_string)?
    };

    let time_spine_df = df!(
        "ts" => date_range(
            "ts".into(),
            start_dt,
            end_dt,
            Duration::parse("4s"),
            ClosedWindow::Both,
            TimeUnit::Milliseconds,
            Some(&tz_parsed),
        )?
    )?;

    let duid_lf = latest_forecasts_lf
        .clone()
        .select([col("DUID")])
        .unique_stable(None, UniqueKeepStrategy::First);

    let base_lf = time_spine_df.lazy().cross_join(duid_lf, None);

    let forecast_data_lf = latest_forecasts_lf.select([
        col("INTERVAL_DATETIME").alias("ts_5m"),
        col("DUID"),
        col("FORECAST_POE50").alias("target_mw"),
    ]);

    let prev_lf = forecast_data_lf
        .clone()
        .with_column(col("target_mw").alias("prev_target"))
        .drop(["target_mw"]);

    let next_lf = forecast_data_lf
        .clone()
        .with_column((col("ts_5m") - lit(Duration::parse("5m"))).alias("ts_5m"))
        .with_column(col("target_mw").alias("next_target"))
        .drop(["target_mw"]);

    let join_cols = [col("ts").dt().truncate(lit("5m")), col("DUID")];
    let other_cols = [col("ts_5m"), col("DUID")];

    let joined_lf = base_lf
        .join(
            prev_lf,
            &join_cols,
            &other_cols,
            JoinArgs {
                how: JoinType::Left,
                // Add a unique suffix to prevent column name collisions
                suffix: Some("_prev".into()),
                ..Default::default()
            },
        )
        .join(
            next_lf,
            &join_cols,
            &other_cols,
            JoinArgs {
                how: JoinType::Left,
                // Add a different unique suffix for the second join
                suffix: Some("_next".into()),
                ..Default::default()
            },
        );

    let frac = (col("ts").dt().timestamp(TimeUnit::Milliseconds)
        - col("ts")
            .dt()
            .truncate(lit("5m"))
            .dt()
            .timestamp(TimeUnit::Milliseconds))
    .cast(DataType::Float64)
        / lit(300_000.0);

    let next_filled = col("next_target").fill_null(col("prev_target"));
    let final_lf = joined_lf
        .with_column(
            (col("prev_target").fill_null(0.0)
                + (next_filled - col("prev_target").fill_null(0.0)) * frac.fill_null(0.0))
            .alias("reference_mw"),
        )
        // The final select implicitly drops the suffixed intermediate columns
        .select([col("ts"), col("DUID"), col("reference_mw")]);

    let mut df_out = final_lf.collect()?;
    ParquetWriter::new(&mut File::create(output_path)?)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut df_out)?;

    info!(
        "Successfully wrote AWEFS trajectory file to {:?}",
        output_path
    );
    Ok(())
}

/// Calculates the deviation between the AWEFS trajectory and actual SCADA measurements.
fn run_step_3_unit_deviations(
    input_dir: &str,
    date: &str,
    output_path: &PathBuf,
    trajectory_path: &PathBuf,
) -> Result<()> {
    info!("Step 3: Calculating hypothetical unit deviations");

    let current_date =
        NaiveDate::parse_from_str(date, "%Y-%m-%d").context("Failed to parse date string")?;
    let prev_date = current_date - ChronoDuration::days(1);
    let next_date = current_date + ChronoDuration::days(1);

    let trajectory_lf = LazyFrame::scan_parquet(trajectory_path, Default::default())?.select([
        col("ts"),
        col("DUID"),
        col("reference_mw"),
    ]);

    let scada_paths: Vec<PathBuf> = [prev_date, current_date, next_date]
        .iter()
        .map(|d| {
            PathBuf::from(format!(
                "{}/FPP---UNIT_MW---1/date={}/*.parquet",
                input_dir,
                d.format("%Y-%m-%d")
            ))
        })
        .collect();

    let scada_lf = LazyFrame::scan_parquet_files(scada_paths.into(), Default::default())?.select([
        col("MEASUREMENT_DATETIME").alias("ts"),
        col("FPP_UNITID").alias("DUID"),
        col("MEASURED_MW"),
    ]);

    let deviations_lf = trajectory_lf
        .join(
            scada_lf,
            [col("ts"), col("DUID")],
            [col("ts"), col("DUID")],
            JoinArgs::new(JoinType::Inner),
        )
        .with_column((col("MEASURED_MW") - col("reference_mw")).alias("deviation_mw"))
        .select([col("ts"), col("DUID"), col("deviation_mw")]);

    let mut final_df = deviations_lf.collect()?;
    ParquetWriter::new(&mut File::create(output_path)?)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut final_df)?;

    info!(
        "Successfully wrote hypothetical deviations to {:?}",
        output_path
    );
    Ok(())
}

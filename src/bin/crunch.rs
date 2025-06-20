use anyhow::{Context, Result};
use chrono::FixedOffset;
use chrono::{Duration as ChronoDuration, NaiveDate};
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

    let trajectory_path = PathBuf::from(&args.output);
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
    info!("Running Step 1: Region‐level Frequency Measure with EWMA");

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
        // this is not working. i think we can just use the FREQ_MEASURE_HZ value though.
        .with_column(
            col("freq_dev")
                .neg()
                .ewm_mean(EWMOptions {
                    alpha,
                    adjust: false,
                    ..Default::default()
                })
                .alias("freq_measure"),
        )
        // final layout: timestamp, region, raw deviation, AEMO’s measure, your EWMA
        .select([
            col("ts"),
            col("region"),
            col("freq_dev"),
            col("aemo_freq_measure"),
            col("freq_measure"),
        ])
        .collect()
        .context("Failed to collect regional frequency‐measure DataFrame")?;

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

fn run_step_2_reference_trajectory(
    input_dir: &str,
    date: &str,
    output_dir: &PathBuf,
) -> anyhow::Result<()> {
    info!("Step 2: streaming reference trajectories by origin");

    // 1) Read just the ORIGIN column to get all distinct origins
    let pred_path = format!(
        "{}/DEMAND---INTERMITTENT_DS_PRED---1/date={}/*.parquet",
        input_dir, date
    );
    let tiny = LazyFrame::scan_parquet(&pred_path, ScanArgsParquet::default())?
        .select([col("ORIGIN")])
        .unique_stable(None, UniqueKeepStrategy::First)
        .collect()?;
    let origins = {
        let ca = tiny
            .column("ORIGIN")?
            .str()
            .context("ORIGIN column not UTF8")?;
        ca.into_no_null_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
    };
    info!("Found {} origins: {:?}", origins.len(), origins);

    // 2) Build the 4s time spine once
    let start_dt = NaiveDate::parse_from_str(date, "%Y-%m-%d")?
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let end_dt = start_dt + ChronoDuration::days(1) - ChronoDuration::seconds(4);
    let tz_name = "+10:00"; // NEM time

    // Convert fixed offset to timezone database format, then parse
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

    // 3) For each origin, build and write just that slice
    for origin in origins {
        info!("Processing origin {}", origin);

        // interpolation fraction expr - recreate for each iteration to avoid move
        let frac = {
            let ms = col("ts")
                .dt()
                .timestamp(polars::prelude::TimeUnit::Milliseconds);
            let floored = col("ts")
                .dt()
                .truncate(lit("5m"))
                .dt()
                .timestamp(TimeUnit::Milliseconds);
            (ms - floored).cast(DataType::Float64) / lit(300_000.0)
        };

        // a) Lazy scan for just this origin - clone origin to avoid move
        let origin_pred = LazyFrame::scan_parquet(&pred_path, ScanArgsParquet::default())?
            .filter(col("ORIGIN").eq(lit(origin.clone())))
            .select([
                col("INTERVAL_DATETIME").alias("ts_5m"),
                col("DUID"),
                col("ORIGIN"),
                col("FORECAST_POE50").alias("target_mw"),
            ]);

        // b) unique (DUID, ORIGIN) pairs for this origin
        let duid_origin_df = origin_pred
            .clone()
            .select([col("DUID"), col("ORIGIN")])
            .unique_stable(None, UniqueKeepStrategy::First)
            .collect()?;

        // c) cross-join time × (DUID, ORIGIN)
        let base = time_spine_df.cross_join(&duid_origin_df, None, None)?;

        // d) pull in prev & next 5 min forecasts
        let prev = origin_pred.clone().select([
            col("ts_5m"),
            col("DUID").alias("DUID_prev"),
            col("ORIGIN").alias("ORIGIN_prev"),
            col("target_mw").alias("prev_target"),
        ]);

        let next = origin_pred
            .clone()
            .with_column(
                (col("ts_5m") - lit(polars::prelude::Duration::parse("5m"))).alias("ts_5m"),
            )
            .select([
                col("ts_5m"),
                col("DUID").alias("DUID_next"),
                col("ORIGIN").alias("ORIGIN_next"),
                col("target_mw").alias("next_target"),
            ]);

        let joined = base
            .lazy()
            .join(
                prev,
                &[
                    col("ts").dt().truncate(lit("5m")),
                    col("DUID"),
                    col("ORIGIN"),
                ],
                &["ts_5m".into(), "DUID_prev".into(), "ORIGIN_prev".into()],
                JoinArgs::new(JoinType::Left),
            )
            .drop(["DUID_prev", "ORIGIN_prev", "ts_5m"])
            .join(
                next,
                &[
                    col("ts").dt().truncate(lit("5m")),
                    col("DUID"),
                    col("ORIGIN"),
                ],
                &["ts_5m".into(), "DUID_next".into(), "ORIGIN_next".into()],
                JoinArgs::new(JoinType::Left),
            )
            .drop(["DUID_next", "ORIGIN_next", "ts_5m"]);

        // e) interpolate & write one small DataFrame
        let df_out = joined
            .with_column(
                (col("prev_target").fill_null(0.0)
                    + (col("next_target").fill_null(col("prev_target"))
                        - col("prev_target").fill_null(0.0))
                        * frac.fill_null(0.0))
                .alias("reference_mw"),
            )
            .select([col("ts"), col("DUID"), col("ORIGIN"), col("reference_mw")])
            .sort_by_exprs([col("ts"), col("DUID")], Default::default())
            .collect()?; // this is small: (#timestamps×#duids_for_this_origin)

        let out_path = output_dir.join(format!("02_trajectory_{}.parquet", origin));
        let mut f = File::create(&out_path)?;
        ParquetWriter::new(&mut f)
            .with_compression(ParquetCompression::Snappy)
            .finish(&mut df_out.clone())?; // clone here is tiny

        // Drop df_out before next loop iteration to free memory
    }

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

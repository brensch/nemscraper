use anyhow::Result;
use clap::Parser;
use polars::lazy::dsl::*;
use polars::prelude::ParquetWriter;
use polars::prelude::*;
use std::{
    fs::{create_dir_all, File},
    path::PathBuf,
}; // col, lit, when, etc.

#[derive(Parser)]
#[command(
    author,
    version,
    about = "AWEFS Alternative Reality FPP Recalculation Pipeline"
)]
struct Args {
    /// Date to process (YYYY-MM-DD)
    #[arg(short, long)]
    date: String,
    /// Input base path for parquet folders
    #[arg(long, default_value = "./assets/parquet")]
    input: String,
    /// Output directory for results
    #[arg(long, default_value = "./output")]
    output: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let date = &args.date;
    let input = PathBuf::from(&args.input);
    let output = PathBuf::from(&args.output);
    create_dir_all(&output)?;

    // 1. Raw SCADA
    let raw = load_raw_scada(&input, date)?;
    write_df(&raw, output.join("1_raw_scada.parquet"))?;

    // 2. EWMA
    let scada_ewm = compute_ewm(raw)?;
    write_df(&scada_ewm, output.join("2_scada_ewma.parquet"))?;

    // 3. Residual join
    let residuals = join_residuals(&input, date)?;
    write_df(&residuals, output.join("3_residuals.parquet"))?;

    // 4. Split perf
    let split = split_performance(residuals)?;
    write_df(&split, output.join("4_perf_split.parquet"))?;

    // 5. Aggregate
    let summary = aggregate_residuals(split)?;
    write_df(&summary, output.join("5_residual_summary.parquet"))?;

    // 6. Constraint freq
    let constraint = load_constraint_freq(&input, date)?;
    write_df(&constraint, output.join("6_constraint_freq.parquet"))?;

    // 7. Contribution factor
    let with_cf = join_contribution_factor(summary, &input, date)?;
    write_df(&with_cf, output.join("7_residual_with_cf.parquet"))?;

    // 8. Overlay DCF
    let with_dcf = overlay_dcf(with_cf, &input, date)?;
    write_df(&with_dcf, output.join("8_with_dcf.parquet"))?;

    // 9. Weighted CF
    let weighted = calculate_weighted_cf(with_dcf)?;
    write_df(&weighted, output.join("9_weighted_residual_cf.parquet"))?;

    // 10. Charges
    let charges = compute_charges(weighted, &input, date)?;
    write_df(&charges, output.join("10_interval_charges.parquet"))?;

    // 11. Summary
    let final_summary = summarize_total_charge(charges)?;
    write_df(&final_summary, output.join("11_fpp_charge_summary.parquet"))?;

    Ok(())
}

// Write by reference—clone internally so original stays usable
fn write_df(df: &DataFrame, path: PathBuf) -> Result<()> {
    let mut file = File::create(path)?;
    let mut df_clone = df.clone();
    ParquetWriter::new(&mut file).finish(&mut df_clone)?;
    Ok(())
}

fn load_raw_scada(input: &PathBuf, date: &str) -> Result<DataFrame> {
    let pattern = format!(
        "{}/CAUSER_PAYS_SCADA---NETWORK---1/date={}/**/*.parquet",
        input.display(),
        date
    );
    let df = LazyFrame::scan_parquet(&pattern, ScanArgsParquet::default())?
        .select(&[
            col("MEASUREMENTTIME").alias("ts"),
            col("FREQUENCYDEVIATION").alias("freq_dev"),
        ])
        // Use a SortOptions slice, not &[bool]
        .sort_by_exprs(&[col("ts")], &[SortOptions::default()])
        .collect()?;
    Ok(df)
}

fn compute_ewm(df: DataFrame) -> Result<DataFrame> {
    let freq = df.column("freq_dev")?.f64()?;
    let mut vals = Vec::with_capacity(freq.len());
    for (i, v) in freq.into_no_null_iter().enumerate() {
        let neg = -v;
        let e = if i == 0 {
            0.05 * neg
        } else {
            0.05 * neg + 0.95 * vals[i - 1]
        };
        vals.push(e);
    }
    let mut out = df;
    out.with_column(Series::new("freq_measure".into(), vals))?;
    Ok(out)
}

fn join_residuals(input: &PathBuf, date: &str) -> Result<DataFrame> {
    let scada_lf = LazyFrame::scan_parquet(
        &format!(
            "{}/CAUSER_PAYS_SCADA---NETWORK---1/date={}/**/*.parquet",
            input.display(),
            date
        ),
        ScanArgsParquet::default(),
    )?;
    let runs_lf = LazyFrame::scan_parquet(
        &format!(
            "{}/DEMAND---INTERMITTENT_DS_RUN---1/date={}/**/*.parquet",
            input.display(),
            date
        ),
        ScanArgsParquet::default(),
    )?;

    let df = runs_lf
        .filter(
            col("ORIGIN")
                .eq(lit("AWEFS"))
                .or(col("ORIGIN").eq(lit("ASEFS"))),
        )
        .select(&[
            col("RUN_DATETIME").alias("ts"),
            col("DUID"),
            (-col("SUPPRESSED_PARTICIPANT")).alias("deviation_mw"),
        ])
        .join(
            scada_lf.select(&[
                col("MEASUREMENTTIME").alias("ts"),
                col("FREQUENCYDEVIATION").alias("freq_dev"),
            ]),
            &[col("ts")],
            &[col("ts")],
            JoinType::Inner.into(),
        )
        .collect()?;
    Ok(df)
}

fn split_performance(df: DataFrame) -> Result<DataFrame> {
    let out = df
        .lazy()
        .with_columns(&[
            when(col("freq_measure").gt(lit(0.0)))
                .then(col("deviation_mw"))
                .otherwise(lit(0.0))
                .alias("raise_perf"),
            when(col("freq_measure").lt(lit(0.0)))
                .then(col("deviation_mw"))
                .otherwise(lit(0.0))
                .alias("lower_perf"),
        ])
        .collect()?;
    Ok(out)
}

fn aggregate_residuals(df: DataFrame) -> Result<DataFrame> {
    let out = df
        .lazy()
        .group_by([col("ts")])
        .agg(&[
            col("raise_perf").sum().alias("raise_residual_perf"),
            col("lower_perf").sum().alias("lower_residual_perf"),
        ])
        .collect()?;
    Ok(out)
}

fn load_constraint_freq(input: &PathBuf, date: &str) -> Result<DataFrame> {
    let pattern = format!(
        "{}/FPP---CONSTRAINT_FREQ_MEASURE---1/date={}/**/*.parquet",
        input.display(),
        date
    );
    let df = LazyFrame::scan_parquet(&pattern, ScanArgsParquet::default())?
        .select(&[
            col("CONSTRAINTID"),
            col("INTERVAL_DATETIME").alias("ts"),
            col("FREQ_MEASURE_HZ").alias("freq_measure_hz"),
        ])
        .collect()?;
    Ok(df)
}

fn join_contribution_factor(res: DataFrame, input: &PathBuf, date: &str) -> Result<DataFrame> {
    let cf = LazyFrame::scan_parquet(
        &format!(
            "{}/FPP---CONTRIBUTION_FACTOR---1/date={}/**/*.parquet",
            input.display(),
            date
        ),
        ScanArgsParquet::default(),
    )?
    .select(&[
        col("CONSTRAINTID"),
        col("INTERVAL_DATETIME").alias("ts"),
        col("CONTRIBUTION_FACTOR"),
    ])
    .collect()?;

    let out = res
        .lazy()
        .join(
            cf.lazy(),
            &[col("ts"), col("CONSTRAINTID")],
            &[col("ts"), col("CONSTRAINTID")],
            JoinType::Left.into(),
        )
        .collect()?;
    Ok(out)
}

fn overlay_dcf(df: DataFrame, input: &PathBuf, date: &str) -> Result<DataFrame> {
    let def = LazyFrame::scan_parquet(
        &format!(
            "{}/FPP---FORECAST_DEFAULT_CF---1/date={}/**/*.parquet",
            input.display(),
            date
        ),
        ScanArgsParquet::default(),
    )?
    .select(&[
        col("CONSTRAINTID"),
        col("INTERVAL_DATETIME").alias("ts"),
        col("DEFAULT_CONTRIBUTION_FACTOR"),
    ])
    .collect()?;

    let res_dcf = LazyFrame::scan_parquet(
        &format!(
            "{}/FPP---FORECAST_RESIDUAL_DCF---1/date={}/**/*.parquet",
            input.display(),
            date
        ),
        ScanArgsParquet::default(),
    )?
    .select(&[
        col("CONSTRAINTID"),
        col("INTERVAL_DATETIME").alias("ts"),
        col("RESIDUAL_DCF"),
    ])
    .collect()?;

    let out = df
        .lazy()
        .join(
            def.lazy(),
            &[col("ts"), col("CONSTRAINTID")],
            &[col("ts"), col("CONSTRAINTID")],
            JoinType::Left.into(),
        )
        .join(
            res_dcf.lazy(),
            &[col("ts"), col("CONSTRAINTID")],
            &[col("ts"), col("CONSTRAINTID")],
            JoinType::Left.into(),
        )
        .with_column(
            when(col("RESIDUAL_DCF").is_not_null())
                .then(col("RESIDUAL_DCF"))
                .otherwise(col("DEFAULT_CONTRIBUTION_FACTOR"))
                .alias("used_dcf"),
        )
        .collect()?;
    Ok(out)
}

fn calculate_weighted_cf(df: DataFrame) -> Result<DataFrame> {
    let out = df
        .lazy()
        .with_columns(&[
            (col("raise_residual_perf") * col("used_dcf")).alias("weighted_raise_cf"),
            (col("lower_residual_perf") * col("used_dcf")).alias("weighted_lower_cf"),
        ])
        .collect()?;
    Ok(out)
}

fn compute_charges(df: DataFrame, input: &PathBuf, date: &str) -> Result<DataFrame> {
    let perf_rates = LazyFrame::scan_parquet(
        &format!(
            "{}/FPP---EST_PERF_COST_RATE---1/date={}/**/*.parquet",
            input.display(),
            date
        ),
        ScanArgsParquet::default(),
    )?
    .select(&[
        col("CONSTRAINTID"),
        col("INTERVAL_DATETIME").alias("ts"),
        col("FPP_PAYMENT_RATE"),
    ])
    .collect()?;

    let res_rates = LazyFrame::scan_parquet(
        &format!(
            "{}/FPP---EST_RESIDUAL_COST_RATE---1/date={}/**/*.parquet",
            input.display(),
            date
        ),
        ScanArgsParquet::default(),
    )?
    .select(&[
        col("CONSTRAINTID"),
        col("INTERVAL_DATETIME").alias("ts"),
        col("FPP_RECOVERY_RATE"),
    ])
    .collect()?;

    let out = df
        .lazy()
        .join(
            perf_rates.lazy(),
            &[col("ts"), col("CONSTRAINTID")],
            &[col("ts"), col("CONSTRAINTID")],
            JoinType::Left.into(),
        )
        .join(
            res_rates.lazy(),
            &[col("ts"), col("CONSTRAINTID")],
            &[col("ts"), col("CONSTRAINTID")],
            JoinType::Left.into(),
        )
        .with_columns(&[
            (col("weighted_raise_cf") * col("FPP_PAYMENT_RATE")).alias("raise_charge"),
            (col("weighted_lower_cf") * col("FPP_RECOVERY_RATE")).alias("lower_charge"),
        ])
        .with_column((col("raise_charge") + col("lower_charge")).alias("interval_charge"))
        .collect()?;
    Ok(out)
}

fn summarize_total_charge(df: DataFrame) -> Result<DataFrame> {
    let out = df
        .lazy()
        .select(&[col("interval_charge").sum().alias("total_fpp_charge")])
        .collect()?;
    Ok(out)
}

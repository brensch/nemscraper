use anyhow::Result;
use clap::Parser;
use polars::lazy::prelude::*;
use polars::prelude::{EWMOptions, ParquetCompression, ParquetWriter};
use std::ops::Neg;
use std::{
    fs::{create_dir_all, File},
    path::PathBuf,
};

#[derive(Parser)]
#[command(
    author,
    version,
    about = "AWEFS Alternative Reality FPP Recalculation Pipeline (modular wide)"
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
    let args = Args::parse();
    create_dir_all(&args.output)?;

    let pipeline = build_pipeline(&args.input, &args.date)?;
    let mut df = pipeline.collect()?;

    let out_path = PathBuf::from(&args.output).join("fpp_full_wide.parquet");
    let mut out_file = File::create(out_path)?;
    ParquetWriter::new(&mut out_file)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut df)?;

    Ok(())
}

fn build_pipeline(input: &str, date: &str) -> Result<LazyFrame> {
    // base SCADA + runs join
    let scada = load_scada(input, date)?;
    let runs = load_runs(input, date)?;
    let base = scada.join(
        runs,
        &[col("ts")],
        &[col("ts")],
        JoinArgs::new(JoinType::Inner),
    );

    // add EWMA measurement
    let with_ewma = base.with_column(
        col("freq_dev")
            .neg()
            .ewm_mean(EWMOptions {
                alpha: 0.05,
                adjust: false,
                ..Default::default()
            })
            .alias("freq_measure"),
    );

    // split and aggregate residuals
    let split = split_performance(with_ewma);
    let aggregated = aggregate_residuals(split);

    // joins for FPP tables
    let constraint = join_constraint(aggregated, input, date)?;
    let with_cf = join_contribution_factor(constraint, input, date)?;
    let with_dcf = overlay_dcf(with_cf, input, date)?;
    let with_rates = join_rates(with_dcf, input, date)?;

    // compute interval charges
    let charged = compute_charges(with_rates);
    Ok(charged.select(&[col("*")]))
}

fn load_scada(input: &str, date: &str) -> Result<LazyFrame> {
    let path = format!(
        "{}/CAUSER_PAYS_SCADA---NETWORK---1/date={}/*.parquet",
        input, date
    );
    Ok(
        LazyFrame::scan_parquet(&path, ScanArgsParquet::default())?.select(&[
            col("MEASUREMENTTIME").alias("ts"),
            col("FREQUENCYDEVIATION").alias("freq_dev"),
        ]),
    )
}

fn load_runs(input: &str, date: &str) -> Result<LazyFrame> {
    let path = format!(
        "{}/DEMAND---INTERMITTENT_DS_RUN---1/date={}/*.parquet",
        input, date
    );
    Ok(LazyFrame::scan_parquet(&path, ScanArgsParquet::default())?
        .filter(
            col("ORIGIN")
                .eq(lit("AWEFS"))
                .or(col("ORIGIN").eq(lit("ASEFS"))),
        )
        .select(&[
            col("RUN_DATETIME").alias("ts"),
            col("DUID"),
            (-col("SUPPRESSED_PARTICIPANT")).alias("deviation_mw"),
        ]))
}

fn split_performance(df: LazyFrame) -> LazyFrame {
    df.with_columns(&[
        when(col("freq_measure").gt(lit(0.0)))
            .then(col("deviation_mw"))
            .otherwise(lit(0))
            .alias("raise_perf"),
        when(col("freq_measure").lt(lit(0.0)))
            .then(col("deviation_mw"))
            .otherwise(lit(0))
            .alias("lower_perf"),
    ])
}

fn aggregate_residuals(df: LazyFrame) -> LazyFrame {
    df.group_by([col("ts")]).agg(&[
        col("raise_perf").sum().alias("raise_residual_perf"),
        col("lower_perf").sum().alias("lower_residual_perf"),
    ])
}

fn join_constraint(df: LazyFrame, input: &str, date: &str) -> Result<LazyFrame> {
    let path = format!(
        "{}/FPP---CONSTRAINT_FREQ_MEASURE---1/date={}/*.parquet",
        input, date
    );
    let lf = LazyFrame::scan_parquet(&path, ScanArgsParquet::default())?.select(&[
        col("CONSTRAINTID"),
        col("INTERVAL_DATETIME").alias("ts"),
        col("FREQ_MEASURE_HZ"),
    ]);
    Ok(df.join(
        lf,
        &[col("ts")],
        &[col("ts")],
        JoinArgs::new(JoinType::Left),
    ))
}

fn join_contribution_factor(df: LazyFrame, input: &str, date: &str) -> Result<LazyFrame> {
    let path = format!(
        "{}/FPP---CONTRIBUTION_FACTOR---1/date={}/*.parquet",
        input, date
    );
    let lf = LazyFrame::scan_parquet(&path, ScanArgsParquet::default())?.select(&[
        col("CONSTRAINTID"),
        col("INTERVAL_DATETIME").alias("ts"),
        col("CONTRIBUTION_FACTOR"),
    ]);
    Ok(df.join(
        lf,
        &[col("ts"), col("CONSTRAINTID")],
        &[col("ts"), col("CONSTRAINTID")],
        JoinArgs::new(JoinType::Left),
    ))
}

fn overlay_dcf(df: LazyFrame, input: &str, date: &str) -> Result<LazyFrame> {
    let def_path = format!(
        "{}/FPP---FORECAST_DEFAULT_CF---1/date={}/*.parquet",
        input, date
    );
    let dcf_path = format!(
        "{}/FPP---FORECAST_RESIDUAL_DCF---1/date={}/*.parquet",
        input, date
    );

    let def_lf = LazyFrame::scan_parquet(&def_path, ScanArgsParquet::default())?.select(&[
        col("CONSTRAINTID"),
        col("INTERVAL_DATETIME").alias("ts"),
        col("DEFAULT_CONTRIBUTION_FACTOR").alias("default_cf"),
    ]);
    let dcf_lf = LazyFrame::scan_parquet(&dcf_path, ScanArgsParquet::default())?.select(&[
        col("CONSTRAINTID"),
        col("INTERVAL_DATETIME").alias("ts"),
        col("RESIDUAL_DCF"),
    ]);

    Ok(df
        .join(
            def_lf,
            &[col("ts"), col("CONSTRAINTID")],
            &[col("ts"), col("CONSTRAINTID")],
            JoinArgs::new(JoinType::Left),
        )
        .join(
            dcf_lf,
            &[col("ts"), col("CONSTRAINTID")],
            &[col("ts"), col("CONSTRAINTID")],
            JoinArgs::new(JoinType::Left),
        )
        .with_column(
            when(col("RESIDUAL_DCF").is_not_null())
                .then(col("RESIDUAL_DCF"))
                .otherwise(col("default_cf"))
                .alias("used_dcf"),
        ))
}

fn join_rates(df: LazyFrame, input: &str, date: &str) -> Result<LazyFrame> {
    let perf_path = format!(
        "{}/FPP---EST_PERF_COST_RATE---1/date={}/*.parquet",
        input, date
    );
    let res_path = format!(
        "{}/FPP---EST_RESIDUAL_COST_RATE---1/date={}/*.parquet",
        input, date
    );

    let perf_lf = LazyFrame::scan_parquet(&perf_path, ScanArgsParquet::default())?.select(&[
        col("CONSTRAINTID"),
        col("INTERVAL_DATETIME").alias("ts"),
        col("FPP_PAYMENT_RATE"),
    ]);
    let res_lf = LazyFrame::scan_parquet(&res_path, ScanArgsParquet::default())?.select(&[
        col("CONSTRAINTID"),
        col("INTERVAL_DATETIME").alias("ts"),
        col("FPP_RECOVERY_RATE"),
    ]);

    Ok(df
        .join(
            perf_lf,
            &[col("ts"), col("CONSTRAINTID")],
            &[col("ts"), col("CONSTRAINTID")],
            JoinArgs::new(JoinType::Left),
        )
        .join(
            res_lf,
            &[col("ts"), col("CONSTRAINTID")],
            &[col("ts"), col("CONSTRAINTID")],
            JoinArgs::new(JoinType::Left),
        ))
}

fn compute_charges(df: LazyFrame) -> LazyFrame {
    df.with_columns(&[
        (col("raise_residual_perf") * col("used_dcf") * col("FPP_PAYMENT_RATE"))
            .alias("raise_charge"),
        (col("lower_residual_perf") * col("used_dcf") * col("FPP_RECOVERY_RATE"))
            .alias("lower_charge"),
    ])
    .with_column((col("raise_charge") + col("lower_charge")).alias("interval_charge"))
}

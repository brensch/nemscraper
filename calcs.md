# AWEFS Alternative Reality FPP Recalculation Pipeline

This README describes each stage required to recalculate Frequency Performance Payment (FPP) charges as if AWEFS/ASEFS submissions had been used instead of ARARATWF. It includes high-level descriptions and detailed pseudocode for the logic.

---

## Prerequisites

* **Data sources**: Local Hive‐style directories containing parquet files under `./assets/parquet/<folder>/`:

  * `./assets/parquet/CAUSER_PAYS_SCADA---NETWORK---1/` (SCADA frequency deviations)
  * `./assets/parquet/DEMAND---INTERMITTENT_DS_RUN---1/` (participant residual deviations)
  * `./assets/parquet/FPP---CONSTRAINT_FREQ_MEASURE---1/` (constraint frequency measurements)
  * `./assets/parquet/FPP---CONTRIBUTION_FACTOR---1/` (contribution factors)
  * `./assets/parquet/FPP---FORECAST_DEFAULT_CF---1/` (default forecast CF)
  * `./assets/parquet/FPP---FORECAST_RESIDUAL_DCF---1/` (residual forecast DCF)
  * `./assets/parquet/FPP---RESIDUAL_CF---1/` (residual CF)
  * `./assets/parquet/FPP---EST_PERF_COST_RATE---1/` (performance cost rates)
  * `./assets/parquet/FPP---EST_RESIDUAL_COST_RATE---1/` (residual cost rates)
  * `./assets/parquet/FPP---FCAS_SUMMARY---1/` (FCAS summary rates)
* **Environment**: Rust toolchain with Polars 0.49.1 (or Python/SQL equivalent).
* **Output**: Intermediate and final parquet files under `./output/`, plus the final FPP charge summary.

---

## Step 1: Load Raw SCADA Frequency Data

**Objective**: Ingest SCADA frequency deviations for the target day (e.g. `2025-06-01`).

### Pseudocode

```text
scada_frames = []
for file in list_parquet("./assets/parquet/CAUSER_PAYS_SCADA---NETWORK---1/date=2025-06-01"):
    df = read_parquet(file)
    df = df.select(["measurementtime" as ts, "frequencydeviation" as freq_dev])
    scada_frames.append(df)
raw_scada = concat(scada_frames)
write_parquet(raw_scada, "./output/1_raw_scada.parquet")
```

**Logic**: extract only the timestamp and deviation, then merge partitions.

---

## Step 2: Compute 4‑second EWMA Frequency Measure

**Objective**: Apply Exponentially Weighted Moving Average on `-freq_dev` with α=0.05.

### Pseudocode

```text
scada_sorted = raw_scada.sort(by="ts")
ewm_vals = []
for i, row in enumerate(scada_sorted.rows()):
    neg = -row.freq_dev
    if i == 0:
        ewm = 0.05 * neg
    else:
        ewm = 0.05 * neg + 0.95 * ewm_vals[i-1]
    ewm_vals.append(ewm)
scada_sorted = scada_sorted.with_column("freq_measure", ewm_vals)
write_parquet(scada_sorted, "./output/2_scada_ewma.parquet")
```

**Logic**: rolling calculation avoids expensive window functions.

---

## Step 3: Join Residual Deviations

**Objective**: Combine EWMA results with participant residuals from DS-run.

### Pseudocode

```text
runs = []
for file in list_parquet("./assets/parquet/DEMAND---INTERMITTENT_DS_RUN---1/date=2025-06-01"):
    df = read_parquet(file)
    df = df.filter(df.origin in ["AWEFS","ASEFS"])
    df = df.select(["run_datetime" as ts, "duid", "suppressed_participant"] )
    df = df.with_column("deviation_mw", -df.suppressed_participant)
    runs.append(df)
runs_df = concat(runs)

scada_ewma = read_parquet("./output/2_scada_ewma.parquet")
residuals = runs_df.join(scada_ewma, on="ts", how="inner")
write_parquet(residuals, "./output/3_residuals.parquet")
```

**Logic**: inner‐join on matching timestamps; negation aligns sign.

---

## Step 4: Split Raise vs Lower Performance

**Objective**: Classify contributions as "raise" or "lower" based on `freq_measure` sign.

### Pseudocode

```text
res = read_parquet("./output/3_residuals.parquet")
perf_split = res.with_columns([
    if res.freq_measure >  0 then res.deviation_mw else 0 as raise_perf,
    if res.freq_measure <  0 then res.deviation_mw else 0 as lower_perf
])
write_parquet(perf_split, "./output/4_perf_split.parquet")
```

**Logic**: segregate positive vs negative contributions.

---

## Step 5: Aggregate Residuals Per Interval

**Objective**: Sum `raise_perf` and `lower_perf` across units for each `ts`.

### Pseudocode

```text
df = read_parquet("./output/4_perf_split.parquet")
res_summary = df.groupby("ts").agg([
    sum("raise_perf") as raise_residual_perf,
    sum("lower_perf") as lower_residual_perf
])
write_parquet(res_summary, "./output/5_residual_summary.parquet")
```

**Logic**: interval‐level summary of MW deviations.

---

## Step 6: Load Constraint Frequency Measurements

**Objective**: Read `FPP---CONSTRAINT_FREQ_MEASURE---1` for measured bounds.

### Pseudocode

```text
cfm = []
for file in list_parquet("./assets/parquet/FPP---CONSTRAINT_FREQ_MEASURE---1/date=2025-06-01"):
    df = read_parquet(file)
    df = df.select(["constraintid", "interval_datetime" as ts, "freq_measure_hz"])
    cfm.append(df)
cfm_df = concat(cfm)
write_parquet(cfm_df, "./output/6_constraint_freq.parquet")
```

**Logic**: prepare per‐constraint frequency reference.

---

## Step 7: Compute Contribution Factors

**Objective**: Ingest `FPP---CONTRIBUTION_FACTOR---1` and align with residuals.

### Pseudocode

```text
cf_tables = []
for file in list_parquet("./assets/parquet/FPP---CONTRIBUTION_FACTOR---1/date=2025-06-01"):
    df = read_parquet(file)
    df = df.filter(df.origin in ["AWEFS","ASEFS"])
        .select(["constraintid", "interval_datetime" as ts, "contribution_factor"] )
    cf_tables.append(df)
cf_df = concat(cf_tables)
res_cf = res_summary.join(cf_df, on=["ts","constraintid"], how="left")
write_parquet(res_cf, "./output/7_residual_with_cf.parquet")
```

**Logic**: map each residual to its CF.

---

## Step 8: Overlay Default & Residual DCF

**Objective**: Merge `FPP---FORECAST_DEFAULT_CF---1` and `FPP---FORECAST_RESIDUAL_DCF---1`.

### Pseudocode

```text
def_cf = read_parquet("./assets/parquet/FPP---FORECAST_DEFAULT_CF---1/date=2025-06-01/*.parquet")
res_dcf = read_parquet("./assets/parquet/FPP---FORECAST_RESIDUAL_DCF---1/date=2025-06-01/*.parquet")
combined = res_cf
    .join(def_cf, on=["constraintid","ts"], how="left")
    .join(res_dcf, on=["constraintid","ts"], how="left")
    .with_column(
        "used_dcf",
        if combined.residual_dcf is not null then combined.residual_dcf else combined.default_contribution_factor
    )
write_parquet(combined, "./output/8_with_dcf.parquet")
```

**Logic**: fallback to default when residual DCF is missing.

---

## Step 9: Calculate Weighted Residual CF

**Objective**: Multiply raise/lower residual MW by the chosen DCF.

### Pseudocode

```text
input = read_parquet("./output/8_with_dcf.parquet")
res_cf_calc = input.with_columns([
    input.raise_residual_perf  * input.used_dcf as weighted_raise_cf,
    input.lower_residual_perf  * input.used_dcf as weighted_lower_cf
])
write_parquet(res_cf_calc, "./output/9_weighted_residual_cf.parquet")
```

**Logic**: yields MW‐weighted CF values.

---

## Step 10: Compute Interval Charges

**Objective**: Join `FPP---EST_PERF_COST_RATE---1` and `FPP---EST_RESIDUAL_COST_RATE---1` then apply.

### Pseudocode

```text
perf_rates = read_parquet("./assets/parquet/FPP---EST_PERF_COST_RATE---1/date=2025-06-01/*.parquet")
res_rates  = read_parquet("./assets/parquet/FPP---EST_RESIDUAL_COST_RATE---1/date=2025-06-01/*.parquet")
input10   = res_cf_calc
    .join(perf_rates, on=["constraintid","ts"], how="left")
    .join(res_rates,  on=["constraintid","ts"], how="left")
charges = input10.with_columns([
    input10.weighted_raise_cf * input10.fpp_payment_rate  as raise_charge,
    input10.weighted_lower_cf * input10.fpp_recovery_rate as lower_charge
])
charges = charges.with_column("interval_charge", raise_charge + lower_charge)
write_parquet(charges, "./output/10_interval_charges.parquet")
```

**Logic**: converts CF‐weighted MW into dollar amounts.

---

## Step 11: Summarize Total FPP Charge

**Objective**: Sum `interval_charge` over all intervals for final daily amount.

### Pseudocode

```text
final = read_parquet("./output/10_interval_charges.parquet")
summary = final.groupby().agg([sum("interval_charge") as total_fpp_charge])
write_parquet(summary, "./output/11_fpp_charge_summary.parquet")
```

**Logic**: produces single‐row charge summary for billing.

---

## Notes & Optimization

* **Partition pruning**: always filter on `date=...` when reading.
* **Streaming/Efficient I/O**: use Polars lazy or DuckDB to push down filters.
* **Validation**: at each stage, verify row counts, null rates.
* **Parallelism**: enable multi‐threaded file reads/writes.

*End of README*


here are the columns for you to be accurate
    
causer_pays_scada---network---1,FI,FLOAT64

causer_pays_scada---network---1,FREQUENCYDEVIATION,FLOAT64

causer_pays_scada---network---1,MEASUREMENTTIME,TIMESTAMP

causer_pays_scada---network---1,NETWORKID,STRING

causer_pays_scada---network---1,date,DATE


fpp---constraint_freq_measure---1,BIDTYPE,STRING

fpp---constraint_freq_measure---1,CONSTRAINTID,STRING

fpp---constraint_freq_measure---1,CORRELATION_FLAG,FLOAT64

fpp---constraint_freq_measure---1,FM_LOWER_HZ,FLOAT64

fpp---constraint_freq_measure---1,FM_RAISE_HZ,FLOAT64

fpp---constraint_freq_measure---1,INTERVAL_DATETIME,TIMESTAMP

fpp---constraint_freq_measure---1,MEASUREMENT_DATETIME,TIMESTAMP

fpp---constraint_freq_measure---1,USED_IN_RCR_FLAG,FLOAT64

fpp---constraint_freq_measure---1,VERSIONNO,FLOAT64

fpp---constraint_freq_measure---1,date,DATE

fpp---contribution_factor---1,BIDTYPE,STRING

fpp---contribution_factor---1,CF_ABS_NEGATIVE_PERF_TOTAL,FLOAT64

fpp---contribution_factor---1,CF_ABS_POSITIVE_PERF_TOTAL,FLOAT64

fpp---contribution_factor---1,CF_REASON_FLAG,FLOAT64

fpp---contribution_factor---1,CONSTRAINTID,STRING

fpp---contribution_factor---1,CONTRIBUTION_FACTOR,FLOAT64

fpp---contribution_factor---1,DEFAULT_CONTRIBUTION_FACTOR,FLOAT64

fpp---contribution_factor---1,FPP_UNITID,STRING

fpp---contribution_factor---1,INTERVAL_DATETIME,TIMESTAMP

fpp---contribution_factor---1,NCF_ABS_NEGATIVE_PERF_TOTAL,FLOAT64

fpp---contribution_factor---1,NEGATIVE_CONTRIBUTION_FACTOR,FLOAT64

fpp---contribution_factor---1,PARTICIPANTID,STRING

fpp---contribution_factor---1,SETTLEMENTS_UNITID,STRING

fpp---contribution_factor---1,VERSIONNO,FLOAT64

fpp---contribution_factor---1,date,DATE

fpp---est_perf_cost_rate---1,BIDTYPE,STRING

fpp---est_perf_cost_rate---1,CONSTRAINTID,STRING

fpp---est_perf_cost_rate---1,FPP_PAYMENT_RATE,FLOAT64

fpp---est_perf_cost_rate---1,FPP_RECOVERY_RATE,FLOAT64

fpp---est_perf_cost_rate---1,INTERVAL_DATETIME,TIMESTAMP

fpp---est_perf_cost_rate---1,RELEVANT_REGIONS,STRING

fpp---est_perf_cost_rate---1,UNUSED_FCAS_RATE,FLOAT64

fpp---est_perf_cost_rate---1,USED_FCAS_RATE,FLOAT64

fpp---est_perf_cost_rate---1,VERSIONNO,FLOAT64

fpp---est_perf_cost_rate---1,date,DATE

fpp---est_residual_cost_rate---1,BIDTYPE,STRING

fpp---est_residual_cost_rate---1,CONSTRAINTID,STRING

fpp---est_residual_cost_rate---1,FPP,FLOAT64

fpp---est_residual_cost_rate---1,INTERVAL_DATETIME,TIMESTAMP

fpp---est_residual_cost_rate---1,RELEVANT_REGIONS,STRING

fpp---est_residual_cost_rate---1,UNUSED_FCAS,FLOAT64

fpp---est_residual_cost_rate---1,USED_FCAS,FLOAT64

fpp---est_residual_cost_rate---1,VERSIONNO,FLOAT64

fpp---est_residual_cost_rate---1,date,DATE

fpp---fcas_summary---1,BASE_COST,FLOAT64

fpp---fcas_summary---1,BIDTYPE,STRING

fpp---fcas_summary---1,CONSTRAINTID,STRING

fpp---fcas_summary---1,CONSTRAINT_MARGINAL_VALUE,FLOAT64

fpp---fcas_summary---1,INTERVAL_DATETIME,TIMESTAMP

fpp---fcas_summary---1,P_REGULATION,FLOAT64

fpp---fcas_summary---1,RCR,FLOAT64

fpp---fcas_summary---1,REGULATION_MW,FLOAT64

fpp---fcas_summary---1,RELEVANT_REGIONS,STRING

fpp---fcas_summary---1,RUNNO,FLOAT64

fpp---fcas_summary---1,RUN_DATETIME,TIMESTAMP

fpp---fcas_summary---1,TOTAL_FPP,FLOAT64

fpp---fcas_summary---1,TSFCAS,FLOAT64

fpp---fcas_summary---1,USAGE,FLOAT64

fpp---fcas_summary---1,USAGE_VALUE,FLOAT64

fpp---fcas_summary---1,VERSIONNO,FLOAT64

fpp---fcas_summary---1,date,DATE

fpp---forecast_default_cf---1,BIDTYPE,STRING

fpp---forecast_default_cf---1,CONSTRAINTID,STRING

fpp---forecast_default_cf---1,DCF_ABS_NEGATIVE_PERF_TOTAL,FLOAT64

fpp---forecast_default_cf---1,DCF_REASON_FLAG,FLOAT64

fpp---forecast_default_cf---1,DEFAULT_CONTRIBUTION_FACTOR,FLOAT64

fpp---forecast_default_cf---1,EFFECTIVE_END_DATETIME,TIMESTAMP

fpp---forecast_default_cf---1,EFFECTIVE_START_DATETIME,TIMESTAMP

fpp---forecast_default_cf---1,FPP_UNITID,STRING

fpp---forecast_default_cf---1,REGIONID,STRING

fpp---forecast_default_cf---1,SETTLEMENTS_UNITID,STRING

fpp---forecast_default_cf---1,VERSIONNO,FLOAT64

fpp---forecast_default_cf---1,date,DATE

fpp---forecast_residual_dcf---1,BIDTYPE,STRING

fpp---forecast_residual_dcf---1,CONSTRAINTID,STRING

fpp---forecast_residual_dcf---1,DCF_ABS_NEGATIVE_PERF_TOTAL,FLOAT64

fpp---forecast_residual_dcf---1,EFFECTIVE_END_DATETIME,TIMESTAMP

fpp---forecast_residual_dcf---1,EFFECTIVE_START_DATETIME,TIMESTAMP

fpp---forecast_residual_dcf---1,RESIDUAL_DCF,FLOAT64

fpp---forecast_residual_dcf---1,RESIDUAL_DCF_REASON_FLAG,FLOAT64

fpp---forecast_residual_dcf---1,VERSIONNO,FLOAT64

fpp---forecast_residual_dcf---1,date,DATE

fpp---fpp_performance---1,FPP_UNITID,STRING

fpp---fpp_performance---1,INTERVAL_DATETIME,TIMESTAMP

fpp---fpp_performance---1,LOWER_PERFORMANCE,FLOAT64

fpp---fpp_performance---1,LOWER_REASON_FLAG,FLOAT64

fpp---fpp_performance---1,PARTICIPANTID,STRING

fpp---fpp_performance---1,RAISE_PERFORMANCE,FLOAT64

fpp---fpp_performance---1,RAISE_REASON_FLAG,FLOAT64

fpp---fpp_performance---1,VERSIONNO,FLOAT64

fpp---fpp_performance---1,date,DATE

fpp---fpp_rcr---1,BIDTYPE,STRING

fpp---fpp_rcr---1,CONSTRAINTID,STRING

fpp---fpp_rcr---1,INTERVAL_DATETIME,TIMESTAMP

fpp---fpp_rcr---1,RCR,FLOAT64

fpp---fpp_rcr---1,RCR_REASON_FLAG,FLOAT64

fpp---fpp_rcr---1,VERSIONNO,FLOAT64

fpp---fpp_rcr---1,date,DATE

fpp---fpp_run---1,AUTHORISED_DATETIME,TIMESTAMP

fpp---fpp_run---1,FPPRUN_DATETIME,TIMESTAMP

fpp---fpp_run---1,INTERVAL_DATETIME,TIMESTAMP

fpp---fpp_run---1,RUN_STATUS,STRING

fpp---fpp_run---1,VERSIONNO,FLOAT64

fpp---fpp_run---1,date,DATE

fpp---fpp_usage---1,BIDTYPE,STRING

fpp---fpp_usage---1,CONSTRAINTID,STRING

fpp---fpp_usage---1,INTERVAL_DATETIME,TIMESTAMP

fpp---fpp_usage---1,REGULATION_MW,FLOAT64

fpp---fpp_usage---1,USAGE,FLOAT64

fpp---fpp_usage---1,USAGE_REASON_FLAG,FLOAT64

fpp---fpp_usage---1,USAGE_VALUE,FLOAT64

fpp---fpp_usage---1,USED_MW,FLOAT64

fpp---fpp_usage---1,VERSIONNO,FLOAT64

fpp---fpp_usage---1,date,DATE

fpp---hist_performance---1,EFFECTIVE_END_DATETIME,TIMESTAMP

fpp---hist_performance---1,EFFECTIVE_START_DATETIME,TIMESTAMP

fpp---hist_performance---1,FPP_HIST_LOWER_PERFORMANCE,FLOAT64

fpp---hist_performance---1,FPP_HIST_RAISE_PERFORMANCE,FLOAT64

fpp---hist_performance---1,FPP_UNITID,STRING

fpp---hist_performance---1,HIST_PERIOD_END_DATETIME,TIMESTAMP

fpp---hist_performance---1,HIST_PERIOD_START_DATETIME,TIMESTAMP

fpp---hist_performance---1,REG_HIST_LOWER_PERFORMANCE,FLOAT64

fpp---hist_performance---1,REG_HIST_RAISE_PERFORMANCE,FLOAT64

fpp---hist_performance---1,VERSIONNO,FLOAT64

fpp---hist_performance---1,date,DATE

fpp---region_freq_measure---1,FM_ALIGNMENT_FLAG,FLOAT64

fpp---region_freq_measure---1,FREQ_DEVIATION_HZ,FLOAT64

fpp---region_freq_measure---1,FREQ_MEASURE_HZ,FLOAT64

fpp---region_freq_measure---1,HZ_QUALITY_FLAG,FLOAT64

fpp---region_freq_measure---1,INTERVAL_DATETIME,TIMESTAMP

fpp---region_freq_measure---1,MEASUREMENT_DATETIME,TIMESTAMP

fpp---region_freq_measure---1,REGIONID,STRING

fpp---region_freq_measure---1,VERSIONNO,FLOAT64

fpp---region_freq_measure---1,date,DATE

fpp---residual_cf---1,BIDTYPE,STRING

fpp---residual_cf---1,CF_ABS_NEGATIVE_PERF_TOTAL,FLOAT64

fpp---residual_cf---1,CF_ABS_POSITIVE_PERF_TOTAL,FLOAT64

fpp---residual_cf---1,CONSTRAINTID,STRING

fpp---residual_cf---1,INTERVAL_DATETIME,TIMESTAMP

fpp---residual_cf---1,NCF_ABS_NEGATIVE_PERF_TOTAL,FLOAT64

fpp---residual_cf---1,NEGATIVE_RESIDUAL_CF,FLOAT64

fpp---residual_cf---1,RESIDUAL_CF,FLOAT64

fpp---residual_cf---1,RESIDUAL_CF_REASON_FLAG,FLOAT64

fpp---residual_cf---1,RESIDUAL_DCF,FLOAT64

fpp---residual_cf---1,VERSIONNO,FLOAT64

fpp---residual_cf---1,date,DATE

fpp---residual_performance---1,INTERVAL_DATETIME,TIMESTAMP

fpp---residual_performance---1,LOWER_PERFORMANCE,FLOAT64

fpp---residual_performance---1,LOWER_REASON_FLAG,FLOAT64

fpp---residual_performance---1,RAISE_PERFORMANCE,FLOAT64

fpp---residual_performance---1,RAISE_REASON_FLAG,FLOAT64

fpp---residual_performance---1,REGIONID,STRING

fpp---residual_performance---1,VERSIONNO,FLOAT64

fpp---residual_performance---1,date,DATE

fpp---unit_mw---1,DEVIATION_MW,FLOAT64

fpp---unit_mw---1,FPP_UNITID,STRING

fpp---unit_mw---1,INTERVAL_DATETIME,TIMESTAMP

fpp---unit_mw---1,MEASURED_MW,FLOAT64

fpp---unit_mw---1,MEASUREMENT_DATETIME,TIMESTAMP

fpp---unit_mw---1,MW_QUALITY_FLAG,FLOAT64

fpp---unit_mw---1,PARTICIPANTID,STRING

fpp---unit_mw---1,SCHEDULED_MW,FLOAT64

fpp---unit_mw---1,VERSIONNO,FLOAT64

fpp---unit_mw---1,date,DATE

demand---intermittent_ds_run---1,DUID,STRING

demand---intermittent_ds_run---1,FORECAST_PRIORITY,FLOAT64

demand---intermittent_ds_run---1,LASTCHANGED,TIMESTAMP

demand---intermittent_ds_run---1,OFFERDATETIME,TIMESTAMP

demand---intermittent_ds_run---1,ORIGIN,STRING

demand---intermittent_ds_run---1,PARTICIPANT_TIMESTAMP,TIMESTAMP

demand---intermittent_ds_run---1,RUN_DATETIME,TIMESTAMP

demand---intermittent_ds_run---1,SUPPRESSED_AEMO,FLOAT64

demand---intermittent_ds_run---1,SUPPRESSED_PARTICIPANT,FLOAT64

demand---intermittent_ds_run---1,TRANSACTION_ID,STRING

demand---intermittent_ds_run---1,date,DATE

dispatch---unit_solution---4,AGCSTATUS,FLOAT64

dispatch---unit_solution---4,AVAILABILITY,FLOAT64

dispatch---unit_solution---4,CONFORMANCE_MODE,FLOAT64

dispatch---unit_solution---4,CONNECTIONPOINTID,STRING

dispatch---unit_solution---4,DISPATCHINTERVAL,FLOAT64

dispatch---unit_solution---4,DISPATCHMODE,FLOAT64

dispatch---unit_solution---4,DISPATCHMODETIME,FLOAT64

dispatch---unit_solution---4,DOWNEPF,STRING

dispatch---unit_solution---4,DUID,STRING

dispatch---unit_solution---4,INITIALMW,FLOAT64

dispatch---unit_solution---4,INTERVENTION,FLOAT64

dispatch---unit_solution---4,LASTCHANGED,TIMESTAMP

dispatch---unit_solution---4,LOWER1SEC,FLOAT64

dispatch---unit_solution---4,LOWER1SECACTUALAVAILABILITY,FLOAT64

dispatch---unit_solution---4,LOWER1SECFLAGS,FLOAT64

dispatch---unit_solution---4,LOWER5MIN,FLOAT64

dispatch---unit_solution---4,LOWER5MINACTUALAVAILABILITY,FLOAT64

dispatch---unit_solution---4,LOWER5MINFLAGS,FLOAT64

dispatch---unit_solution---4,LOWER60SEC,FLOAT64

dispatch---unit_solution---4,LOWER60SECACTUALAVAILABILITY,FLOAT64

dispatch---unit_solution---4,LOWER60SECFLAGS,FLOAT64

dispatch---unit_solution---4,LOWER6SEC,FLOAT64

dispatch---unit_solution---4,LOWER6SECACTUALAVAILABILITY,FLOAT64

dispatch---unit_solution---4,LOWER6SECFLAGS,FLOAT64

dispatch---unit_solution---4,LOWERREG,FLOAT64

dispatch---unit_solution---4,LOWERREGACTUALAVAILABILITY,FLOAT64

dispatch---unit_solution---4,LOWERREGAVAILABILITY,FLOAT64

dispatch---unit_solution---4,LOWERREGENABLEMENTMAX,FLOAT64

dispatch---unit_solution---4,LOWERREGENABLEMENTMIN,FLOAT64

dispatch---unit_solution---4,LOWERREGFLAGS,FLOAT64

dispatch---unit_solution---4,MARGINAL5MINVALUE,STRING

dispatch---unit_solution---4,MARGINAL60SECVALUE,STRING

dispatch---unit_solution---4,MARGINAL6SECVALUE,STRING

dispatch---unit_solution---4,MARGINALVALUE,STRING

dispatch---unit_solution---4,RAISE1SEC,FLOAT64

dispatch---unit_solution---4,RAISE1SECACTUALAVAILABILITY,FLOAT64

dispatch---unit_solution---4,RAISE1SECFLAGS,FLOAT64

dispatch---unit_solution---4,RAISE5MIN,FLOAT64

dispatch---unit_solution---4,RAISE5MINACTUALAVAILABILITY,FLOAT64

dispatch---unit_solution---4,RAISE5MINFLAGS,FLOAT64

dispatch---unit_solution---4,RAISE60SEC,FLOAT64

dispatch---unit_solution---4,RAISE60SECACTUALAVAILABILITY,FLOAT64

dispatch---unit_solution---4,RAISE60SECFLAGS,FLOAT64

dispatch---unit_solution---4,RAISE6SEC,FLOAT64

dispatch---unit_solution---4,RAISE6SECACTUALAVAILABILITY,FLOAT64

dispatch---unit_solution---4,RAISE6SECFLAGS,FLOAT64

dispatch---unit_solution---4,RAISEREG,FLOAT64

dispatch---unit_solution---4,RAISEREGACTUALAVAILABILITY,FLOAT64

dispatch---unit_solution---4,RAISEREGAVAILABILITY,FLOAT64

dispatch---unit_solution---4,RAISEREGENABLEMENTMAX,FLOAT64

dispatch---unit_solution---4,RAISEREGENABLEMENTMIN,FLOAT64

dispatch---unit_solution---4,RAISEREGFLAGS,FLOAT64

dispatch---unit_solution---4,RAMPDOWNRATE,FLOAT64

dispatch---unit_solution---4,RAMPUPRATE,FLOAT64

dispatch---unit_solution---4,RUNNO,FLOAT64

dispatch---unit_solution---4,SEMIDISPATCHCAP,FLOAT64

dispatch---unit_solution---4,SETTLEMENTDATE,TIMESTAMP

dispatch---unit_solution---4,TOTALCLEARED,FLOAT64

dispatch---unit_solution---4,TRADETYPE,FLOAT64

dispatch---unit_solution---4,UIGF,FLOAT64

dispatch---unit_solution---4,UPEPF,STRING

dispatch---unit_solution---4,VIOLATION5MINDEGREE,STRING

dispatch---unit_solution---4,VIOLATION60SECDEGREE,STRING

dispatch---unit_solution---4,VIOLATION6SECDEGREE,STRING

dispatch---unit_solution---4,VIOLATIONDEGREE,STRING

dispatch---unit_solution---4,date,DATE

dispatch---unit_solution---5,AGCSTATUS,FLOAT64

dispatch---unit_solution---5,AVAILABILITY,FLOAT64

dispatch---unit_solution---5,CONFORMANCE_MODE,FLOAT64

dispatch---unit_solution---5,CONNECTIONPOINTID,STRING

dispatch---unit_solution---5,DISPATCHINTERVAL,FLOAT64

dispatch---unit_solution---5,DISPATCHMODE,FLOAT64

dispatch---unit_solution---5,DISPATCHMODETIME,FLOAT64

dispatch---unit_solution---5,DOWNEPF,STRING

dispatch---unit_solution---5,DUID,STRING

dispatch---unit_solution---5,ENERGY_STORAGE,STRING

dispatch---unit_solution---5,INITIALMW,FLOAT64

dispatch---unit_solution---5,INITIAL_ENERGY_STORAGE,FLOAT64

dispatch---unit_solution---5,INTERVENTION,FLOAT64

dispatch---unit_solution---5,LASTCHANGED,TIMESTAMP

dispatch---unit_solution---5,LOWER1SEC,FLOAT64

dispatch---unit_solution---5,LOWER1SECACTUALAVAILABILITY,FLOAT64

dispatch---unit_solution---5,LOWER1SECFLAGS,FLOAT64

dispatch---unit_solution---5,LOWER5MIN,FLOAT64

dispatch---unit_solution---5,LOWER5MINACTUALAVAILABILITY,FLOAT64

dispatch---unit_solution---5,LOWER5MINFLAGS,FLOAT64

dispatch---unit_solution---5,LOWER60SEC,FLOAT64

dispatch---unit_solution---5,LOWER60SECACTUALAVAILABILITY,FLOAT64

dispatch---unit_solution---5,LOWER60SECFLAGS,FLOAT64

dispatch---unit_solution---5,LOWER6SEC,FLOAT64

dispatch---unit_solution---5,LOWER6SECACTUALAVAILABILITY,FLOAT64

dispatch---unit_solution---5,LOWER6SECFLAGS,FLOAT64

dispatch---unit_solution---5,LOWERREG,FLOAT64

dispatch---unit_solution---5,LOWERREGACTUALAVAILABILITY,FLOAT64

dispatch---unit_solution---5,LOWERREGAVAILABILITY,FLOAT64

dispatch---unit_solution---5,LOWERREGENABLEMENTMAX,FLOAT64

dispatch---unit_solution---5,LOWERREGENABLEMENTMIN,FLOAT64

dispatch---unit_solution---5,LOWERREGFLAGS,FLOAT64

dispatch---unit_solution---5,MARGINAL5MINVALUE,STRING

dispatch---unit_solution---5,MARGINAL60SECVALUE,STRING

dispatch---unit_solution---5,MARGINAL6SECVALUE,STRING

dispatch---unit_solution---5,MARGINALVALUE,STRING

dispatch---unit_solution---5,MIN_AVAILABILITY,FLOAT64

dispatch---unit_solution---5,RAISE1SEC,FLOAT64

dispatch---unit_solution---5,RAISE1SECACTUALAVAILABILITY,FLOAT64

dispatch---unit_solution---5,RAISE1SECFLAGS,FLOAT64

dispatch---unit_solution---5,RAISE5MIN,FLOAT64

dispatch---unit_solution---5,RAISE5MINACTUALAVAILABILITY,FLOAT64

dispatch---unit_solution---5,RAISE5MINFLAGS,FLOAT64

dispatch---unit_solution---5,RAISE60SEC,FLOAT64

dispatch---unit_solution---5,RAISE60SECACTUALAVAILABILITY,FLOAT64

dispatch---unit_solution---5,RAISE60SECFLAGS,FLOAT64

dispatch---unit_solution---5,RAISE6SEC,FLOAT64

dispatch---unit_solution---5,RAISE6SECACTUALAVAILABILITY,FLOAT64

dispatch---unit_solution---5,RAISE6SECFLAGS,FLOAT64

dispatch---unit_solution---5,RAISEREG,FLOAT64

dispatch---unit_solution---5,RAISEREGACTUALAVAILABILITY,FLOAT64

dispatch---unit_solution---5,RAISEREGAVAILABILITY,FLOAT64

dispatch---unit_solution---5,RAISEREGENABLEMENTMAX,FLOAT64

dispatch---unit_solution---5,RAISEREGENABLEMENTMIN,FLOAT64

dispatch---unit_solution---5,RAISEREGFLAGS,FLOAT64

dispatch---unit_solution---5,RAMPDOWNRATE,FLOAT64

dispatch---unit_solution---5,RAMPUPRATE,FLOAT64

dispatch---unit_solution---5,RUNNO,FLOAT64

dispatch---unit_solution---5,SEMIDISPATCHCAP,FLOAT64

dispatch---unit_solution---5,SETTLEMENTDATE,TIMESTAMP

dispatch---unit_solution---5,TOTALCLEARED,FLOAT64

dispatch---unit_solution---5,TRADETYPE,FLOAT64

dispatch---unit_solution---5,UIGF,FLOAT64

dispatch---unit_solution---5,UPEPF,STRING

dispatch---unit_solution---5,VIOLATION5MINDEGREE,STRING

dispatch---unit_solution---5,VIOLATION60SECDEGREE,STRING

dispatch---unit_solution---5,VIOLATION6SECDEGREE,STRING

dispatch---unit_solution---5,VIOLATIONDEGREE,STRING

dispatch---unit_solution---5,date,DATE


please think through this, and write an application that executes this. i would like it to be as efficient as possible. i have written the rest of the app in rust. i don't think it has to be done in rust but that would be best. explore the different options and come up with a complete solution using the latest version of all packages used that compiles and generates the output files.

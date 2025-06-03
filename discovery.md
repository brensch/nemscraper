```
SELECT
  MEASUREMENT_DATETIME, SCHEDULED_MW, DEVIATION_MW, Measured_mw
FROM read_parquet('/data/parquet/*FPP_UNIT_MW*.parquet',  union_by_name = true)
WHERE $__timeFilter(measurement_datetime)
and fpp_unitid = 'ARWF1'
GROUP BY all
ORDER BY 1, 2 -- Optional, but good for inspection
```

Shows the deviation. think we just need to 
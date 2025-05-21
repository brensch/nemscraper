SELECT
  time_bucket($__interval, measurement_datetime) AS time,
  CAST(participantid AS VARCHAR) AS metric, -- or TEXT, depending on your SQL dialect
  AVG(measured_mw) AS value
FROM memory.main.parquet_date
WHERE $__timeFilter(measurement_datetime)
GROUP BY 1, 2 -- Group by the aliased columns (time and the new metric)
ORDER BY 1, 2 -- Optional, but good for inspection
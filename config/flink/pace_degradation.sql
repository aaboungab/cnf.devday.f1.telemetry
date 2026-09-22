INSERT INTO `f1.alerts`
  (event_type, severity, car_index, lap_number, `value`, previous_value, detected_at)
WITH laps AS (
  SELECT
    car_index,
    lap_number,
    last_lap_time_ms,
    $rowtime AS rt,
    LAG(last_lap_time_ms) OVER (
      PARTITION BY car_index ORDER BY $rowtime
    ) AS prev_last_lap_ms,
    MIN(last_lap_time_ms) OVER (
      PARTITION BY car_index ORDER BY $rowtime
    ) AS best_lap_ms
  FROM `f1.telemetry`
  WHERE last_lap_time_ms IS NOT NULL AND last_lap_time_ms > 0
)
SELECT
  'pace_degradation' AS event_type,
  CASE
    WHEN last_lap_time_ms - best_lap_ms >= 5000 THEN 'CRITICAL'
    ELSE 'WARNING'
  END AS severity,
  car_index,
  lap_number - 1 AS lap_number,
  CAST(last_lap_time_ms AS BIGINT) AS `value`,
  CAST(best_lap_ms AS BIGINT) AS previous_value,
  rt AS detected_at
FROM laps
WHERE (prev_last_lap_ms IS NULL OR last_lap_time_ms <> prev_last_lap_ms)
  AND last_lap_time_ms - best_lap_ms >= 1500;

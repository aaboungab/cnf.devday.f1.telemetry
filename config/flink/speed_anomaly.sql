CREATE TABLE `f1.alerts` AS
WITH drops AS (
  SELECT
    car_index,
    speed,
    lap_number,
    $rowtime AS rt,
    MAX(speed) OVER (
      PARTITION BY car_index
      ORDER BY $rowtime
      RANGE BETWEEN INTERVAL '2' SECOND PRECEDING AND CURRENT ROW
    ) AS peak_speed
  FROM `f1.telemetry.processed`
),
incidents AS (
  SELECT
    car_index,
    MAX(lap_number) AS lap_number,
    CAST(MIN(speed) AS BIGINT) AS min_speed,
    CAST(MAX(peak_speed) AS BIGINT) AS peak_speed,
    MAX(peak_speed - speed) AS speed_drop,
    window_time AS wt,
    window_end AS detected_at
  FROM TABLE(
    TUMBLE(TABLE drops, DESCRIPTOR(rt), INTERVAL '5' SECOND)
  )
  GROUP BY window_start, window_end, window_time, car_index
  HAVING MAX(peak_speed - speed) >= 150 AND MIN(speed) <= 30
)
SELECT
  CAST('speed_anomaly' AS STRING) AS event_type,
  CAST(CASE
    WHEN speed_drop >= 220 THEN 'CRITICAL'
    ELSE 'WARNING'
  END AS STRING) AS severity,
  car_index,
  lap_number,
  min_speed AS `value`,
  peak_speed AS previous_value,
  detected_at
FROM (
  SELECT
    incidents.*,
    LAG(wt) OVER (PARTITION BY car_index ORDER BY wt) AS prev_wt
  FROM incidents
)
WHERE prev_wt IS NULL OR wt > prev_wt + INTERVAL '10' SECOND;

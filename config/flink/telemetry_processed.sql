CREATE TABLE `f1.telemetry.processed` AS
SELECT
  `timestamp`,
  car_index,
  speed,
  CAST(ROUND(throttle * 100) AS INT) AS throttle_pct,
  CAST(ROUND(brake * 100) AS INT) AS brake_pct,
  gear,
  engine_rpm,
  lap_number,
  car_position
FROM `f1.telemetry`
WHERE speed > 0;

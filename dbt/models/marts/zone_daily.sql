SELECT
    CAST(t.pickup_at AS DATE) AS day,
    t.month,
    t.pickup_zone,
    z.Zone AS zone,
    z.Borough AS borough,
    count(*) AS trip_count,
    sum(t.total) AS total_revenue,
    avg(t.distance_miles) AS avg_distance_miles,
    avg(date_diff('second', t.pickup_at, t.dropoff_at) / 60.0) AS avg_duration_minutes
FROM {{ source('warehouse', 'silver_trips') }} AS t
LEFT JOIN {{ source('warehouse', 'taxi_zones') }} AS z
    ON t.pickup_zone = z.LocationID
GROUP BY 1, 2, 3, 4, 5

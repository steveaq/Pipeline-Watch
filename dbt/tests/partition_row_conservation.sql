SELECT month
FROM {{ source('warehouse', 'silver_trips') }}
GROUP BY month
HAVING count(*) <> count(DISTINCT row_id)

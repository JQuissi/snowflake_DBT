SELECT 
    resource_type
    , count(status)
    , TO_CHAR(TO_TIMESTAMP(started_at), 'DD-MM-YYYY') AS data_formatada
FROM DBT.PUBLIC.DBT_RESULTS
WHERE resource_type = 'model'
    --AND data_formatada = '01-01-1753'
GROUP BY resource_type, data_formatada;



WITH StatusCounts AS (
    SELECT 
        resource_type,
        status,
        TO_CHAR(TO_TIMESTAMP(started_at), 'DD-MM-YYYY') AS data_formatada,
        COUNT(status) AS total_status,
        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_count
    FROM DBT.PUBLIC.DBT_RESULTS
    WHERE resource_type = 'model'
    GROUP BY resource_type, status, data_formatada
)

SELECT 
    resource_type,
    status,
    total_status,
    data_formatada,
    success_count,
    CASE WHEN total_status = 0 THEN null ELSE success_count::FLOAT / total_status * 100 END AS porcentagem
FROM StatusCounts;








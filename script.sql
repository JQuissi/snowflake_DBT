SELECT 
    resource_type
    , count(status)
    , TO_CHAR(TO_TIMESTAMP(started_at), 'DD-MM-YYYY') AS data_formatada
FROM DBT.PUBLIC.DBT_RESULTS
WHERE resource_type = 'model'
    --AND data_formatada = '01-01-1753'
GROUP BY resource_type, data_formatada;

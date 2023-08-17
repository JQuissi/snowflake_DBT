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

------------------------

'{{ parsed_result_dict.get('status') }}',
                            '{{ parsed_result_dict.get('message') | replace("'", "''") }}',




-- Crie uma CTE com todas as combinações possíveis
WITH AllCombinations AS (
    SELECT DISTINCT
        'model' AS resource_type,
        'error' AS status,
        TO_CHAR(DATEADD(DAY, seq4(), '1753-01-01'), 'DD-MM-YYYY') AS data_formatada
    FROM TABLE(GENERATOR(ROWCOUNT => 2000)) -- Isso gera datas até o ano atual, você pode ajustar conforme necessário
)

-- Faça um LEFT JOIN entre as combinações e os resultados reais
SELECT 
    ac.resource_type,
    ac.status,
    ac.data_formatada,
    COUNT(dr.status) AS status_count
FROM AllCombinations ac
LEFT JOIN DBT.PUBLIC.DBT_RESULTS dr ON ac.resource_type = dr.resource_type
                                  AND ac.status = dr.status
                                  AND ac.data_formatada = TO_CHAR(TO_TIMESTAMP(dr.started_at), 'DD-MM-YYYY')
WHERE ac.resource_type = 'model'
  AND ac.status = 'error'
GROUP BY ac.resource_type, ac.status, ac.data_formatada
ORDER BY ac.data_formatada;

------------------



SELECT 
    unique_id
    , database_name
    , schema_name
    , name
    , resource_type
    , status
FROM DBT.PUBLIC.DBT_RESULTS
WHERE resource_type= 'test';


SELECT 
    unique_id
    , database_name
    , schema_name
    , name
    , resource_type
    , status
    , started_at
FROM DBT.PUBLIC.DBT_RESULTS
WHERE resource_type= 'test'
    AND status = 'success';

SELECT 
    unique_id
    , database_name
    , schema_name
    , name
    , resource_type
    , status
    , started_at
FROM DBT.PUBLIC.DBT_RESULTS
WHERE resource_type= 'test'
    AND status = 'error';

    ----------------

    '{{ parsed_result_dict.get('status') }}',
                            '{{ parsed_result_dict.get('message') | replace("'", "''") }}',






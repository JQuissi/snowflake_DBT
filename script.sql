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



-------------------
    
    StatusProjeto = 
VAR UltimaDataExecucao = 'TabelaProjetos'[DataUltimaExecucao]
VAR ProjetosComErro =
    CALCULATETABLE(
        SUMMARIZECOLUMNS(
            'TabelaProjetos'[NomeProjeto],
            'TabelaStatusModelos'[DataStatus],
            "ErroPresente",
            IF(
                COUNTROWS(
                    FILTER(
                        'TabelaStatusModelos',
                        'TabelaStatusModelos'[DataStatus] = UltimaDataExecucao &&
                        'TabelaStatusModelos'[Status] = "Erro"
                    )
                ) > 0,
                1,
                0
            )
        ),
        'TabelaStatusModelos'[DataStatus] = UltimaDataExecucao
    )
RETURN
IF(
    SUM(ProjetosComErro[ErroPresente]) > 0,
    "Erro",
    "Sucesso"
)

----------------------


StatusProjeto = 
VAR UltimaDataExecucao = 'TabelaProjetos'[DataUltimaExecucao]
VAR StatusModelos =
    SUMMARIZE(
        'TabelaStatusModelos',
        'TabelaStatusModelos'[Projeto],
        'TabelaStatusModelos'[DataStatus],
        'TabelaStatusModelos'[Status]
    )
VAR ProjetosComErro =
    FILTER(
        StatusModelos,
        [DataStatus] = UltimaDataExecucao &&
        [Status] = "Erro"
    )
RETURN
IF(
    COUNTROWS(ProjetosComErro) > 0 || 
    COUNTROWS(FILTER(StatusModelos, [Status] = "Sucesso")) = 0,
    "Erro",
    "Sucesso"
)


-----------------------------------

StatusProjeto = 
VAR ProjetoAtual = 'TabelaProjetos'[NomeProjeto]
VAR UltimaDataExecucaoProjeto = CALCULATE(MAX('TabelaProjetos'[DataUltimaExecucao]), 'TabelaProjetos'[NomeProjeto] = ProjetoAtual)
VAR ProjetosComErro =
    CALCULATETABLE(
        FILTER(
            'TabelaStatusModelos',
            'TabelaStatusModelos'[Projeto] = ProjetoAtual &&
            'TabelaStatusModelos'[DataStatus] = UltimaDataExecucaoProjeto &&
            'TabelaStatusModelos'[Status] = "Erro"
        ),
        'TabelaProjetos'[NomeProjeto] = ProjetoAtual
    )
RETURN
IF(
    COUNTROWS(ProjetosComErro) > 0 || 
    COUNTROWS(FILTER('TabelaStatusModelos', 'TabelaStatusModelos'[Projeto] = ProjetoAtual && 'TabelaStatusModelos'[Status] = "Sucesso")) = 0,
    "Erro",
    "Sucesso"
)

---------------------------------------------------


// A coluna calculada
VAR status_execucao =
IF(
  SUM(
    FILTER(
      log_results,
      log_results[projeto] = controle_processamento[projeto]
      AND log_results[data_execucao] = controle_processamento[data_ultima_execucao]
    )[status] = 0,
    "success",
    "error"
  ),
  "success",
  "error"
)

// A tabela principal com a coluna calculada
RETURN
  controle_processamento
    WITH
    [status] = status_execucao

-----------------------------------

SELECT
  projeto,
  data_ultima_execucao,
  -- A coluna calculada
  status_execucao AS status
FROM
  controle_processamento
LEFT JOIN
  log_results
ON
  controle_processamento.projeto = log_results.projeto
AND
  controle_processamento.data_ultima_execucao = log_results.data_execucao
GROUP BY
  controle_processamento.projeto,
  controle_processamento.data_ultima_execucao
HAVING
  -- Se algum modelo der erro, o status será erro
  COUNT(CASE WHEN status = 'error' THEN 1 END) > 0

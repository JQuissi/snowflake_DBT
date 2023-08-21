{% macro log_dbt_results(results) %}
    {%- if execute -%}
        {%- set parsed_results = parse_dbt_results(results) -%}
        {%- if parsed_results | length  > 0 -%}
    
            {%- set source_relation = adapter.get_relation(
                database = "DBT",
                schema = "public",
                identifier = "dbt_results") -%}

            {% set table_exists = source_relation is not none %}

            {% if not table_exists %}
                {% set query_select %}

                    create or replace table dbt_results (
                        result_id varchar,
                        invocation_id varchar,
                        unique_id varchar,
                        database_name varchar,
                        schema_name varchar,
                        name varchar,
                        resource_type varchar,
                        status varchar,
                        message varchar,
                        execution_time float,
                        rows_affected int,
                        started_at varchar
                    )

                {% endset %}
                {% set results = run_query(query_select) %}
            {% endif %}
            
            {% set insert_dbt_results_query -%}
                insert into dbt.public.dbt_results
                    (
                        result_id,
                        invocation_id,
                        unique_id,
                        database_name,
                        schema_name,
                        name,
                        resource_type,
                        status,
                        message, 
                        execution_time,
                        rows_affected, 
                        started_at
                ) values
                    {%- for parsed_result_dict in parsed_results -%}
                        (
                            '{{ parsed_result_dict.get('result_id') }}',
                            '{{ parsed_result_dict.get('invocation_id') }}',
                            '{{ parsed_result_dict.get('unique_id') }}',
                            '{{ parsed_result_dict.get('database_name') }}',
                            '{{ parsed_result_dict.get('schema_name') }}',
                            '{{ parsed_result_dict.get('name') }}',
                            '{{ parsed_result_dict.get('resource_type') }}',
                            '{{ parsed_result_dict.get('status') }}',
                            '{{ parsed_result_dict.get('message') | replace("'", "''") }}',
                            {{ parsed_result_dict.get('execution_time') }},
                            {{ parsed_result_dict.get('rows_affected') }},
                            CASE 
                                WHEN '{{ parsed_result_dict.get('started_at') }}' = '1753-01-01' THEN current_timestamp()
                                ELSE '{{ parsed_result_dict.get('started_at') }}'
                            END 
                        ) {{- "," if not loop.last else "" -}}
                    {%- endfor -%}
            {%- endset -%}
            
            {# Run the insert query #}
            {%- do run_query(insert_dbt_results_query) -%}
        {%- endif -%}
    {%- endif -%}
    
    -- This macro is called from an on-run-end hook and therefore must return a query txt to run. Returning an empty string will do the trick
    {{ return ('') }}
{% endmacro %}

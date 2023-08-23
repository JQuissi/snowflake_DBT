{% macro log_dbt_results(results) %}
    {%- if execute %}
        {%- set parsed_results = parse_dbt_results(results) %}
        {%- if parsed_results | length  > 0 %}
           
           

            {% if target_name == 'dev' %}
                {% set database_table = target.database %}
                {% set schema_table = target.schema %}  
                {% set database_view = target.database %}
                {% set schema_view = target.schema %}  

            {% elif target_name != 'dev' %}
                {% set database_table = 'BRONZE' %}
                {% set schema_table = 'PUBLIC' %} 
                {% set database_view = 'SILVER' %}
                {% set schema_view = 'PUBLIC' %}                    

            {% endif %}

            {%- set source_relation = adapter.get_relation(
                    database = database_table,
                    schema = schema_table,
                    identifier = "dbt_results_table"                
                ) %}

            {%- set source_relation_view = adapter.get_relation(
                    database = database_view,
                    schema = schema_view,
                    identifier = "dw_dbt_results_view"                
                ) %}
            
            {% set table_exists = source_relation is not none %}
            {% set view_exists = source_relation_view is not none %}

            {% if not table_exists %}
                {% set query_create_table %}
                    CREATE OR REPLACE TABLE {{ database_table }}.{{ schema_table }}.dbt_results_table (
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
                        depends_on_nodes varchar,
                        started_at varchar, 
                        project_name
                    )
                {% endset %}
                {% do run_query(query_create_table) %}
            {% endif %}                    
            
            {%- set insert_dbt_results_query %}
                insert into {{ database_table }}.{{ schema_table }}.dbt_results_table
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
                        depends_on_nodes,
                        started_at,
                        project_name
                ) values
                    {%- for parsed_result_dict in parsed_results %}
                        (
                            '{{ parsed_result_dict.get('result_id') }}',
                            '{{ parsed_result_dict.get('invocation_id') }}',
                            '{{ parsed_result_dict.get('unique_id') }}',
                            '{{ parsed_result_dict.get('database_name') }}',
                            '{{ parsed_result_dict.get('schema_name') }}',
                            '{{ parsed_result_dict.get('name') }}',
                            '{{ parsed_result_dict.get('resource_type') }}',
                            '{{ parsed_result_dict.get('status') }}',
                            '{{ parsed_result_dict.get('message') }}',
                            {{ parsed_result_dict.get('execution_time') }},
                            {{ parsed_result_dict.get('rows_affected') }},
                            '{{ parsed_result_dict.get('message') }}',
                            '{{ parsed_result_dict.get('depends_on_nodes') }}',
                            CASE 
                                WHEN '{{ parsed_result_dict.get('started_at') }}' = '1753-01-01' THEN current_timestamp()
                                ELSE '{{ parsed_result_dict.get('started_at') }}'
                            END 
                            'nome do projeto'
                        ) {{- "," if not loop.last else "" -}}
                    {%- endfor %}
            {%- endset %}
            

            {% if not view_exists %}
                {% set query_create_view %}
                    CREATE OR REPLACE VIEW {{database_view}}.{{schema_view}}.dw_dbt_results_view AS 
                        SELECT * FROM {{ database_table }}.{{ schema_table }}.dbt_results_table
                {% endset %}
                {% do run_query(query_create_view) %}
            {% endif %}           

            {# Run the insert query #}
            {%- do run_query(insert_dbt_results_query) %}
        {%- endif %}
    {%- endif %}
    
    {# -- This macro is called from an on-run-end hook and therefore must return a query txt to run. Returning an empty string will do the trick #}
    {{ return ('') }}
{% endmacro %}

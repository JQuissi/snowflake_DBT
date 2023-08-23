{% macro parse_dbt_results(results) %}
    -- Create a list of parsed results
    {%- set parsed_results = [] %}
    -- Flatten results and add to list
    {% for run_result in results %}
        -- Convert the run result object to a simple dictionary
        {% set run_result_dict = run_result.to_dict() %}
        -- Get the underlying dbt graph node that was executed
        {% set node = run_result_dict.get('node') %}

        {% set depends_on_nodes = run_result_dict.get('node', {}).get('depends_on', {}).get('nodes', []) %}

        {% set nodes = run_result_dict.get('adapter_response', {}).get('rows_affected', 0) %}
        {%- if not rows_affected -%}
            {% set rows_affected = 0 %}
        {%- endif -%}

        {% if run_result['timing'] %} 
            {% set second_timing = run_result['timing'][0] %} 
            {% set started_at =second_timing['started_at'] %}
        {% else %} 
            {% set started_at = '1753-01-01' %}
        {% endif %}

        {% set message =  run_result_dict.get('message') %} 

        {% set parsed_result_dict = {
                'result_id': invocation_id ~ '.' ~ node.get('unique_id'),
                'invocation_id': invocation_id,
                'unique_id': node.get('unique_id'),
                'database_name': node.get('database'),
                'schema_name': node.get('schema'),
                'name': node.get('name'),
                'resource_type': node.get('resource_type'),
                'status': run_result_dict.get('status'),                
                'message': message | replace('\'', ''),
                'execution_time': run_result_dict.get('execution_time'),             
                'rows_affected': rows_affected,
                'depends_on_nodes': depends_on_nodes | replace('\'', ''),
                'started_at': started_at
                }%}
        {% do parsed_results.append(parsed_result_dict) %}
    {% endfor %}
    {{ return(parsed_results) }}
{% endmacro %}





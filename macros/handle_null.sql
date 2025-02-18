{% macro get_column_names(table_name) %}
    {%- set query -%}
        select column_name
        from information_schema.columns
        where table_name = Upper('{{ table_name}}')
    {%- endset -%}

    {% if execute %}
        {%- set results = run_query(query) -%}
        {%- set column_names = [] -%}
        {%- for column in results.rows -%}
            {%- do column_names.append(column.COLUMN_NAME) -%}
        {%- endfor -%}
        
        {{ return(column_names) }}
    {% endif %}
{% endmacro %}

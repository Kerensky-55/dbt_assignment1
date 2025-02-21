{% macro create_audit_log() %}
    {% set sql %}
        CREATE TABLE {{ target.database }}.{{ target.schema }}.audit_log (
            audit_id INT AUTOINCREMENT PRIMARY KEY,
            model_name STRING,
            start_time TIMESTAMP,
            end_time TIMESTAMP
        );
    {% endset %}

    {% if execute %}
        {{ run_query(sql) }}
    {% endif %}
{% endmacro %}

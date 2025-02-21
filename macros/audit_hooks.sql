{% macro audit_pre_hook(model_name) %}
  {% if target.name == 'dev' %}
    INSERT INTO {{ target.database }}.{{ target.schema }}.audit_log (model_name, start_time) 
    VALUES ('{{ model_name }}', CURRENT_TIMESTAMP);
  {% endif %}
{% endmacro %}

{% macro audit_post_hook(model_name) %}
  {% if target.name == 'dev' %}
    
    {{ log('Executing audit_post_hook for model: ' ~ model_name, info=True) }}

    {% set update_query %}
      UPDATE audit_log SET end_time = CURRENT_TIMESTAMP WHERE model_name = '{{ model_name }}';
    {% endset %}

    {% if execute %}
        {% set results = run_query(update_query) %}
        {{ log('Audit log updated for model: ' ~ model_name, info=True) }}
    {% else %}
        {{ log('Skipping query execution in parsing mode', info=True) }}
    {% endif %}
    
  {% endif %}
{% endmacro %}

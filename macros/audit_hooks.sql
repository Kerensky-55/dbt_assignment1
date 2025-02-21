{% macro audit_pre_hook(model_name) %}
  {% if target.name == 'dev' %}
    INSERT INTO audit_log (model_name, start_time) VALUES ('{{ model_name }}', CURRENT_TIMESTAMP);
  {% endif %}
{% endmacro %}

{% macro audit_post_hook(model_name) %}
  {% if target.name == 'dev' %}
    UPDATE audit_log SET end_time = CURRENT_TIMESTAMP WHERE model_name = '{{ model_name }}';
  {% endif %}
{% endmacro %}

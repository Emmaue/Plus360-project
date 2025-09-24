{% macro generate_surrogate_key(cols) %}
    md5(
        concat_ws(
            '||',
            {% for col in cols %}
                coalesce(cast({{ col | safe }} as varchar), '∅')
                {%- if not loop.last %}, {% endif -%}
            {% endfor %}
        )
    )
{% endmacro %}

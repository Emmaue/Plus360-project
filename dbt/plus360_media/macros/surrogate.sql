{% macro generate_pk(cols) %}
    md5(
        concat_ws(
            '||',
            {% for col in cols %}
                coalesce(cast({{ col }} as varchar), '')
                {%- if not loop.last %}, {% endif -%}
            {% endfor %}
        )
    )
{% endmacro %}

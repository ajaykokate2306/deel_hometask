{% macro generate_schema_name(custom_schema_name, node) %}

    {%- set default_schema = target.schema -%}
    {%- set database_name = target.database -%}

    {%- if custom_schema_name is none -%}
        {{default_schema}}
    {%- else -%}
        {%- if database_name != "deel_raw" -%}
            {{custom_schema_name | trim}}
        {%- else -%}
            {{default_schema}}_{{custom_schema_name | trim}}
        {%- endif -%}
    {%- endif -%}

{% endmacro %}
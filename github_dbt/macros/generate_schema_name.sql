{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {# No custom schema defined → use the profile default (RAW) #}
        {{ default_schema }}

    {%- else -%}
        {# Custom schema defined → use it exactly as written, ignore prefix #}
        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
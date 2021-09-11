{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {%- if custom_schema_name not in ("stage", "intermediate") and "audit" in target.name -%}

            {{ default_schema }}_unaudited

        {%- else -%}

            {{ default_schema }}_{{ custom_schema_name | trim }}
        
        {%- endif -%}

    {%- endif -%}

{%- endmacro %}

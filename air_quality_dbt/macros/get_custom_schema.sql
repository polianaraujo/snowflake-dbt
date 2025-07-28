-- macros/get_custom_schema.sql

--personaliza a forma como o dbt decide em qual schema criar seus modelos, garantindo que as regras do seu arquivo dbt_project.yml tenham prioridade sobre o schema padrão do seu perfil.

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
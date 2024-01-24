{% macro databricks__get_incremental_append_sql(arg_dict) %}
  {% do return(get_insert_into_with_identity_sql(arg_dict["temp_relation"], arg_dict["target_relation"])) %}
{% endmacro %}

{% macro databricks__get_incremental_replace_where_sql(arg_dict) %}
  {% do return(get_replace_where_sql(arg_dict)) %}
{% endmacro %}

{% macro get_insert_into_with_identity_sql(source_relation, target_relation) %}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) | rejectattr("name", "equalto", "db_id") -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert into table {{ target_relation }} ( {{dest_cols_csv}} )
    select {{dest_cols_csv}} from {{ source_relation }}

{% endmacro %}

{% macro get_replace_where_sql(args_dict) -%}
    {%- set predicates = args_dict['incremental_predicates'] -%}
    {%- set target_relation = args_dict['target_relation'] -%}
    {%- set temp_relation = args_dict['temp_relation'] -%}
   
    {%- call statement('max_identity', fetch_result=True) -%}
        select max(db_id) as max_identity from {{ target_relation }}
    {% endcall %}
    
    {%- set starting_identity = load_result('max_identity')['data'][0][0] -%}
    {%- set temp_relation_with_identity = create_table_with_identity(temp_relation, target_relation, starting_identity + 1) -%}
        
    insert into {{ target_relation }}
    {% if predicates %}
      {% if predicates is sequence and predicates is not string %}
    replace where {{ predicates | join(' and ') }}
      {% else %}
    replace where {{ predicates }}
      {% endif %}
    {% endif %}
    table {{ temp_relation_with_identity }}

    {{ sync_identity(target_relation) }}
    {{ drop_table(temp_relation_with_identity) }}
{% endmacro %}

{% macro sync_identity(relation) %}
    alter table {{ relation }} alter column db_id sync identity
{% endmacro %}

{% macro drop_table(relation) %}
    drop table {{ relation }}
{% endmacro %}

{% macro create_table_with_identity(source_relation, target_relation, identity_start=1) %}
    {%- set identifier_with_identity = target_relation.identifier ~ '_with_identity__dbt_tmp' -%}
    {%- set temp_relation_with_identity = api.Relation.create(
        identifier = identifier_with_identity,
        schema = target_relation.schema,
        database = target_relation.database
        )
    -%}
    
    {%- set columns = adapter.get_columns_in_relation(target_relation) | rejectattr("name", "equalto", "db_id") | list -%}
    {%- set cols_csv = columns | map(attribute='quoted') | join(', ') -%}
  
    {{ log("Creating temp table " ~ identifier_with_identity ~ " in schema " ~ target_relation.schema ~ " in catalog " ~ target_relation.database ~ " with columns " ~ columns, info=True) }} 
    {%- call statement('create identity table') -%}
        create or replace table {{temp_relation_with_identity}} (
            db_id bigint generated by default as identity (START WITH {{identity_start}}),
            {%- for column in columns -%}
                {{column.quoted}} {{column.data_type}} {%- if not loop.last -%}, {%- endif -%}
            {%- endfor -%}
    )
    {%- endcall -%}
    
    {%- call statement('populate table') -%}
        insert into {{ temp_relation_with_identity}} ( {{ cols_csv }} )
        select {{ cols_csv }} from {{ source_relation }}
    {% endcall %}

    {{ return(temp_relation_with_identity) }}

{% endmacro %}

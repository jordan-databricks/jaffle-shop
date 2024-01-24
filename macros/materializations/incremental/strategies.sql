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
   
    {%- set temp_relation_with_identity = create_table_with_identity(temp_relation, target_relation) -%}
    
    insert into {{ target_relation }}
    {% if predicates %}
        {% if predicates is sequence and predicates is not string %}
            replace where {{ predicates | join(' and ') }}
        {% else %}
            replace where {{ predicates }}
        {% endif %}
    {% endif %}
    table {{ temp_relation_with_identity }};
    
{% endmacro %}

{% macro create_table_with_identity(source_relation, target_relation) %}
    {%- set identifier_with_identity = target_relation.identifier ~ '_with_identity__dbt_tmp' -%}
    {%- set temp_relation_with_identity = api.Relation.create(
        identifier = identifier_with_identity,
        schema = target_relation.schema,
        database = target_relation.database
        )
    -%}
    
    {%- call statement('drop temp table') -%}
        drop table {{temp_relation_with_identity}} -%}
    {%- endcall -%}

    {%- call statement('create identity table') -%}
        create table {{temp_relation_with_identity}} like {{ target_relation }}
    {%- endcall -%}
    
    {%- set insert_statement = get_insert_into_with_identity_sql(source_relation, temp_relation_with_identity) -%}
    {%- call statement('populate temp table with identity') -%}
        {{ insert_statement }}
    {% endcall %}

    {{ return(temp_relation_with_identity) }}

{% endmacro %}

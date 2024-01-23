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
    {%- set temp_relation_with_identity = create_table_with_identity(temp_relation, starting_identity + 1) -%}
        
    insert into {{ target_relation }}
    {% if predicates %}
      {% if predicates is sequence and predicates is not string %}
    replace where {{ predicates | join(' and ') }}
      {% else %}
    replace where {{ predicates }}
      {% endif %}
    {% endif %}
    table {{ temp_relation_with_identity }}

    {%- call statement('set identity metadata') -%}
        alter table {{ target_relation }} alter column db_id sync identity
    {% endcall %}
{% endmacro %}

{% macro create_table_with_identity(source_relation, identity_start=1) %}
    {%- set source_identifier_with_identity = source_relation.identifier ~ '_with_identity' -%}
    {%- set source_relation_with_identity = api.Relation.create(
        identifier = source_identifier_with_identity,
        schema = source_relation.schema,
        database = source_relation.database
        )
    -%}
    {{ log("Creating table: " ~ source_relation_with_identity ~ " in schema " ~ schema ~ " and database " ~ database ~ " and identity start " ~ identity_start, info=True) }}
    create table {{source_relation_with_identity}} (
        db_id bigint generated by default as identity (START WITH {{identity_start}}),
        {%- for column in adapter.get_columns_in_relation(source_relation) -%}
            {{column.quoted}} {{column.data_type}} {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
    )

    {%- call statement('populate table') -%}
        insert into {{ source_relation_with_identity}}
        select * from {{ source_relation }}
    {% endcall %}

    {{ return(source_relation_with_identity) }}

{% endmacro %}

{% macro databricks__get_incremental_append_sql(arg_dict) %}
  {% do return(get_insert_into_with_identity_sql(arg_dict["temp_relation"], arg_dict["target_relation"])) %}
{% endmacro %}

{% macro get_insert_into_with_identity_sql(source_relation, target_relation) %}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) | rejectattr("name", "equalto", "db_id") -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert into table {{ target_relation }} ( {{dest_cols_csv}} )
    select {{dest_cols_csv}} from {{ source_relation }}

{% endmacro %}

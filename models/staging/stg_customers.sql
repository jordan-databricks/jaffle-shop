{{
    config(
        materialized='incremental',
        incremental_strategy='replace_where',
        incremental_predicates="name = 'Gerald Odom'"),
        post_hook="alter table {{ this }} alter column db_id sync identity"
}}

with

source as (

    select * from {{ source('ecom', 'raw_customers') }}

),

renamed as (

    select

        ----------  ids
        id as customer_id,

        ---------- text
        name as customer_name

    from source
    {% if is_incremental() %}
        where id > '1'
    {% endif %}

)

select * from renamed

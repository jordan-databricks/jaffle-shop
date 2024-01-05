{{
    config(materialized='incremental')
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
        where customer_id > (select max(id) from {{this}})
    {% endif %}

)

select * from renamed

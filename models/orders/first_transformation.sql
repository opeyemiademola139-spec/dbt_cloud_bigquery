with source_orders as (

    select
        order_id,
        customer_id,
        order_date,
        order_status,
        order_amount
    from {{ source('bigquery_source', 'orders') }}

),

transformed as (

    select
        order_id,
        customer_id,

        -- ensure proper date format
        cast(order_date as date) as order_date,

        -- standardize status
        lower(trim(order_status)) as order_status,

        -- clean amount
        cast(order_amount as numeric) as order_amount,

        -- derived fields
        extract(year from order_date) as order_year,
        extract(month from order_date) as order_month,
        DATE_DIFF(CURRENT_DATE(),order_date,DAY) AS days_since_order,

        case
            when order_amount >= 200 then 'high_value'
            when order_amount >= 100 then 'medium_value'
            else 'low_value'
        end as order_value_segment,

        case
            when order_status = 'Delivered' THEN TRUE
            else FALSE
        end as is_completed

    from source_orders

)

select * from transformed
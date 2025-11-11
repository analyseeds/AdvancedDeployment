with 


customer_orders_payments as (
    select * from {{ref('customer_orders')}}
),

customers as (
    select * from {{ref('stg_customers')}}
),

dim_customers as (
    select

        a.customer_id,
        a.first_name,
        a.last_name,
        b.first_order_date,
        b.last_order_date,
        coalesce(b.number_of_orders, 0) as number_of_orders,
        b.total_amount

    from customers as a
    left join customer_orders_payments as b using (customer_id)
)

select * from dim_customers
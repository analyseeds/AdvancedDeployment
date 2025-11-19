-- esto es una pequeña prueba

with

customers as ( 
    select * from {{ref('stg_customers')}} 
),

orders_payments as ( 
    select * from {{ref('order_payments')}} 
),

customer_orders as (

    select
        a.customer_id,
        min(b.order_date) as first_order_date,
        max(b.order_date) as last_order_date,
        count(b.order_id) as number_of_orders,
        sum(total_amount) as total_amount
    from
        customers as a left join orders_payments as b using(customer_id)
    group by 1
)

select * from customer_orders
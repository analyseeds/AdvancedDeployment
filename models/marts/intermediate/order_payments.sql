with 

orders as  (
    select * from {{ ref('stg_orders' )}}
),

payments as (
    select * from {{ ref('stg_payments') }} where payment_status = 'success'
),

order_payments as (

    select
        a.order_id,
        a.customer_id,
        a.order_date,
        sum(coalesce(b.payment_amount, 0)) as total_amount

    from orders as a
    left join payments as b using (order_id)
	
	group by 1,2,3
	order by 1
)

select * from order_payments
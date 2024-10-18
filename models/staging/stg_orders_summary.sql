{#-
This model aggregates order data to provide summary statistics per customer.
It calculates the total number of orders, total payment amount, and average payment amount.
#}

with orders as (

    {#-
    Select order data from the staging orders model.
    #}
    select 
        customer_id,  -- Correct column name for customer_id from stg_orders
        order_id
    from {{ ref('stg_orders') }}

),

payments as (

    {#-
    Select payment data from the staging payments model.
    #}
    select 
        order_id,
        amount
    from {{ ref('stg_payments') }}

),

orders_with_payments as (

    {#-
    Join orders with payments to calculate the total payment amount per order.
    #}
    select
        o.customer_id,
        o.order_id,
        p.amount
    from orders o
    join payments p on o.order_id = p.order_id

),

aggregated_orders as (

    {#-
    Aggregate order data to compute total orders, total amount, and average payment amount per customer.
    #}
    select
        customer_id,
        count(order_id) as total_orders,
        sum(amount) as total_amount,
        avg(amount) as avg_order_value
    from orders_with_payments
    group by customer_id

)

select
    customer_id,
    total_orders,
    total_amount,
    avg_order_value
from aggregated_orders

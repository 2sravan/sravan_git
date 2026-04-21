with sales as 
(
    select 
        sales_id,
        product_sk,
        customer_sk,
        {{ multiply('unit_price', 'quantity') }} as calculated_gross_amount,
        gross_amount,
        payment_method
    from 
        {{ ref('bronze_sales') }}
),
products as 
(
    select 
        product_sk,
        product_name,
        category
    from 
        {{ ref('bronze_product') }}
),
customers as 
(
    select 
        customer_sk,
        first_name,
        last_name,
        gender,
        email
    from 
        {{ ref('bronze_customer') }}
),
joined_query as 
(
select 
    sales.sales_id,
    sales.gross_amount,
    sales.calculated_gross_amount,
    sales.payment_method,
    products.product_name,
    products.category,
    customers.first_name,
    customers.last_name,
    customers.gender
from 
    sales
join 
    products
on 
    sales.product_sk = products.product_sk
join 
    customers
on 
    sales.customer_sk = customers.customer_sk
)
select 
    category,
    gender,
    sum(gross_amount) as total_gross_amount
from 
    joined_query
group by 
    category,
    gender
order by 
    total_gross_amount desc
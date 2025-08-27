{{config(materialized = 'table')}}

with invoices as (
    select * from {{ref('stg_invoices')}}
)

select 
        organization_id,
        date(created_at) date,
        count(1) total_invoices_issued,
        sum(amount) total_amount,
        sum(payment_amount) payment_amount,
        sum(amount) - sum(payment_amount) balance_amount
from invoices 
group by organization_id, date(created_at)
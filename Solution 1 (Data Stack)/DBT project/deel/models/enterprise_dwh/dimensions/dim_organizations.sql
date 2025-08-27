{{config(materialized='table')}}

with organizations_table as (
    select * from {{ref('stg_organizations')}}
),

invoices as (
    select organization_id,amount,payment_amount from {{ref('stg_invoices')}}
),

aggregated_invoices as (
    select organization_id,
            count(1) as total_invoices_issued,
            sum(amount) as total_invoice_amount,
            sum(payment_amount) as total_payment_amount
    from invoices
    group by 1
)

select ot.*, inv.total_invoices_issued ,inv.total_invoice_amount, inv.total_payment_amount
from organizations_table ot
left join aggregated_invoices inv on ot.organization_id = inv.organization_id


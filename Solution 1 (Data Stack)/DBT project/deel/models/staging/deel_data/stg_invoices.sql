{{config(materialized='table')}}

with source as (
    select * from {{source('deel_raw_data','INVOICES')}}

)

select 

        INVOICE_ID as invoice_id,
        PARENT_INVOICE_ID as parent_invoice_id,
        TRANSACTION_ID as transaction_id,
        ORGANIZATION_ID as organization_id,
        TYPE as type,
        STATUS as status,
        CURRENCY as currency,
        PAYMENT_CURRENCY as payment_currency,
        PAYMENT_METHOD as payment_method,
        coalesce(AMOUNT,0) as amount,
        coalesce(PAYMENT_AMOUNT,0) as payment_amount,
        FX_RATE as fx_rate,
        FX_RATE_PAYMENT as fx_rate_payment,
        CREATED_AT as created_at,
        current_timestamp() last_refreshed_at


 from source
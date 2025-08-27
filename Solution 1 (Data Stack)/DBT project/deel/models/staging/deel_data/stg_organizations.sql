{{config(materialized = 'table', schema = 'staging')}}

with source as (
    select * from {{source('deel_raw_data','ORGANIZATIONS')}}
)

select
        ORGANIZATION_ID as organization_id,
        FIRST_PAYMENT_DATE as first_payment_date,
        LAST_PAYMENT_DATE as last_payment_date,
        LEGAL_ENTITY_COUNTRY_CODE as legal_entity_country_code,
        COUNT_TOTAL_CONTRACTS_ACTIVE as total_contracts_active,
        CREATED_DATE as created_date,
        current_timestamp() last_refreshed_at

 from source

-- Use the `ref` function to select from other models
{{ config(materialized='view') }}
select *
from {{ ref('account_test') }}


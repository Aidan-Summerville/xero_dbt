with calendar as (

    select *
    from {{ ref('calendar') }}

), ledger as (

    select *
    from {{ ref('general_ledger') }}

    union all

    select *
    from {{ ref('general_ledger_burn_uk') }}

    union all

    select *
    from {{ ref('general_ledger_drum_horse') }}

    union all

    select *
    from {{ ref('general_ledger_flagship_marketing') }}

    union all

    select *
    from {{ ref('general_ledger_greentarget') }}

    union all 

    select *
    from {{ ref('general_ledger_sharper_marketing') }}

    union all

    select *
    from {{ ref('general_ledger_unity_marketing') }}


), joined as (

    select 
        ledger.agency_name,
        {{ dbt_utils.surrogate_key(['calendar.date_month','ledger.account_id']) }} as profit_and_loss_id,
        calendar.date_month, 
        ledger.account_id,
        ledger.account_name,
        ledger.account_code,
        case when ledger.account_type = 'REVENUE' then 'Income'
             when ledger.account_type = 'OVERHEADS' then 'Operating Expenses'
             when ledger.account_type = 'DEPRECIATN' then 'Operating Expenses'
             when ledger.account_type = 'DIRECTCOSTS' then 'Cost of Sales'
        end as cost_class, 
        ledger.account_class, 
        coalesce(sum(ledger.net_amount *-1),0) as net_amount
    from calendar
    left join ledger
        on calendar.date_month = cast({{ dbt_utils.date_trunc('month', 'ledger.journal_date') }} as date)
    where ledger.account_type in('REVENUE', 'OVERHEADS', 'DIRECTCOSTS','DEPRECIATN')
    {{ dbt_utils.group_by(8) }}

)

select *
from joined
{{ config(materialized='table') }}
with journals as (

    select 
    journalid as journal_id
    ,createddateutc as created_date_utc
    ,journaldate as journal_date
    ,journalnumber as journal_number
    ,reference
    ,sourceid as source_id
    ,sourcetype as source_type
    from "STITCH"."FLAGSHIP_XERO"."JOURNAL"

), journal_lines as (

   

select
  journalid as journal_id
,v.value:AccountCode::string  as account_code
,v.value:AccountID::string  as account_id
,v.value:AccountName::string as account_name
,v.value:AccountType::string as account_type
,v.value:Description::string as description
,v.value:GrossAmount as gross_amount
,v.value:JournalLineID::string  as journal_line_id
,v.value:NetAmount as net_amount
,v.value:TaxAmount as tax_amount
from STITCH.FLAGSHIP_XERO.JOURNALS s
, lateral flatten (input => journallines) v

)
), accounts as (

    select *
    from "STITCH"."FLAGSHIP_XERO"."ACCOUNT"

), joined as (

    select        
        'Flagship Consulting Ltd' agency_name,
        journals.journal_id,
        journals.created_date_utc,
        journals.journal_date,
        journals.journal_number,
        journals.reference,
        journals.source_id,
        journals.source_type,

        journal_lines.journal_line_id,
        journal_lines.account_code,
        journal_lines.account_id,
        journal_lines.account_name,
        journal_lines.account_type,
        journal_lines.description,
        journal_lines.gross_amount,
        journal_lines.net_amount,
        journal_lines.tax_amount,
        journal_lines.tax_name,
        journal_lines.tax_type,

        accounts.class as account_class,

        case when journals.source_type in ('ACCPAY', 'ACCREC') then journals.source_id end as invoice_id,
        case when journals.source_type in ('CASHREC','CASHPAID') then journals.source_id end as bank_transaction_id,
        case when journals.source_type in ('TRANSFER') then journals.source_id end as bank_transfer_id,
        case when journals.source_type in ('MANJOURNAL') then journals.source_id end as manual_journal_id,
        case when journals.source_type in ('APPREPAYMENT', 'APOVERPAYMENT', 'ACCPAYPAYMENT', 'ACCRECPAYMENT', 'ARCREDITPAYMENT', 'APCREDITPAYMENT') then journals.source_id end as payment_id,
        case when journals.source_type in ('ACCPAYCREDIT','ACCRECCREDIT') then journals.source_id end as credit_note_id

    from journals
    left join journal_lines
        on journals.journal_id = journal_lines.journal_id
    left join accounts
        on accounts.account_id = journal_lines.account_id

)

select *
from joined 
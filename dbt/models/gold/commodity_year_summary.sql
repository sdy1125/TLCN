{{ config(materialized='table', properties={'format': "'PARQUET'"}) }}

select
    year,
    commodity,
    round(sum(export_quantity_tons), 2) as export_quantity_tons,
    round(sum(export_value_usd), 2) as export_value_usd,
    round(sum(export_value_usd) / nullif(sum(export_quantity_tons), 0), 2)
        as export_unit_value_usd_per_ton
from {{ source('silver', 'agri_trade') }}
group by 1, 2


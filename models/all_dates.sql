{{ config (
    materialized="table"
)}}

{{ dbt_utils.date_spine(
    datepart="day",
    start_date="DATE('2020-01-01')",
    end_date="DATE('2021-01-01')"
)
}}
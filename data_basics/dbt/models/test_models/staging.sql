{{
config (
materialized = 'incremental',
incremental_strategy = 'insert_overwrite',
partition_by={
"field"  : "created_at",
"data_type": "timestamp",
"granularity": "DAY",
}
)

SELECT * 
FROM  
{{source('raw_data', 'transactions')}
WHERE DATE(inserted_timestamp) >= '2025-04-01'}
{% if is_incremental() %}
and inserted_timestamp >= timestamp_sub(current_timestamp(), INTERVAL 4 DAY)
{% endif %}
}}

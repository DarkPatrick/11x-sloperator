create table sandbox.ug_monetization_sloperator_{table_name} on cluster ug_core
{schema}
engine = ReplicatedMergeTree('/service/clickhouse/ug_core/tables/{{shard}}/sandbox/ug_monetization_sloperator_{table_name}', '{{replica}}')
PARTITION BY ({partition})
ORDER BY ({sorting})
SETTINGS index_granularity = 8192


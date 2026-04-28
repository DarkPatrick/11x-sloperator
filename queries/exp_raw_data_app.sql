with
    {exp_id} as `exp_id`,
    toDate('{date_filter}') as `date_filter`,
    {where_sql} as `where_condition`,
    {having_sql} as `having_condition`,
    (
        select
            toUInt32(`aee`.`id`) as `id`,
            `aee`.`date_start` as `date_start`,
            `aee`.`date_end` as `date_end`
        from
            `mysql_u_guitarcom`.`ab_experiment_export` as `aee`
        where
            `aee`.`product` = 'UG'
        and
            `aee`.`id` = `exp_id`
    ) as exp_data
    

select
    `urea`.`unified_id`,
    `urea`.`experiments.variation`[indexOf(`urea`.`experiments.id`, `exp_id`)] as `variation`,
    min(toUnixTimestamp(`urea`.`datetime`)) AS `exp_start_dt`,
    argMin(`urea`.`rights`, `urea`.`datetime`) AS `rights`,
    argMin(`urea`.`user_id`, `urea`.`datetime`) AS `user_id`,
    argMin(`urea`.`payment_account_id`, `urea`.`datetime`) AS `payment_account_id`,
    argMin(`urea`.`country`, `urea`.`datetime`) AS `country`,
    argMin(`urea`.`auth`, `urea`.`datetime`) AS `auth`
    -- , [('platform', toString(argMin(`urea`.`platform`, `urea`.`datetime`))), ('value', toString(argMin(`urea`.`value`, `urea`.`datetime`)))] as `params`
from
    `default`.`ug_rt_events_app` as `urea`
where
    `urea`.`date` = `date_filter`
and
    `urea`.`datetime` between toDateTime(tupleElement(exp_data,2)) and if(tupleElement(exp_data,3) < tupleElement(exp_data,2), toDateTime(now()), toDateTime(tupleElement(exp_data,3)))
and
    `urea`.`unified_id` > 0
and (
    (where_condition)
    or `urea`.`event` = 'App Install'
)
and
    `variation` > 0
and
    `urea`.`source` = '{client}'
group by
    `unified_id`,
    `variation`
having
    (having_condition)
and
    toDate(`exp_start_dt`, 'UTC') = `date_filter`

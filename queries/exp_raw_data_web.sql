with
    {exp_id} as `exp_id`,
    toDate('{date_filter}') as `date_filter`,
    {where_sql} as `where_condition`,
    {having_sql} as `having_condition`,
    (
        select distinct
            toUInt32(`aee`.`id`) as `id`,
            `aee`.`date_start` as `date_start`,
            `aee`.`date_end` as `date_end`
        from
            `mysql_u_guitarcom`.`ab_experiment_export` as `aee`
        where
            `aee`.`product` = 'UG'
        and
            `aee`.`id` = `exp_id`
        limit 1
    ) as exp_data
    

select
    `urew`.`unified_id`,
    `urew`.`experiments.variation`[indexOf(`urew`.`experiments.id`, `exp_id`)] as `variation`,
    minIf(toUnixTimestamp(`urew`.`datetime`), `urew`.`event` != 'App Install') AS `exp_start_dt`,
    argMinIf(`urew`.`rights`, `urew`.`datetime`, `urew`.`event` != 'App Install') AS `rights`,
    argMinIf(`urew`.`user_id`, `urew`.`datetime`, `urew`.`event` != 'App Install') AS `user_id`,
    argMinIf(`urew`.`item_id`, `urew`.`datetime`, `urew`.`event` = 'App Install') AS `payment_account_id`,
    argMinIf(`urew`.`country`, `urew`.`datetime`, `urew`.`event` != 'App Install') AS `country`,
    argMinIf(`urew`.`auth`, `urew`.`datetime`, `urew`.`event` != 'App Install') AS `auth`
    -- , [('platform', toString(argMin(`urew`.`platform`, `urew`.`datetime`))), ('value', toString(argMin(`urew`.`value`, `urew`.`datetime`)))] as `params`
from
    `default`.`ug_rt_events_web` as `urew`
where
    `urew`.`date` = `date_filter`
and
    `urew`.`datetime` between toDateTime(tupleElement(exp_data,2)) and if(tupleElement(exp_data,3) < tupleElement(exp_data,2), toDateTime(now()), toDateTime(tupleElement(exp_data,3)))
and
    `urew`.`unified_id` > 0
and (
    (where_condition)
    or `urew`.`event` = 'App Install'
)
and
    `variation` > 0
and
    `urew`.`source` = '{client}'
group by
    `unified_id`,
    `variation`
having
    (having_condition)
and
    toDate(`exp_start_dt`, 'UTC') = `date_filter`

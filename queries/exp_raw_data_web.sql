with
    {exp_id} as `exp_id`,
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
    `urew`.`unified_id`,
    `urew`.`experiments.variation`[indexOf(`urew`.`experiments.id`, 7304)] as `variation`,
    min(`urew`.`datetime`) AS `exp_start_dt`,
    argMin(`urew`.`rights`, `urew`.`datetime`) AS `rights`,
    argMin(`urew`.`user_id`, `urew`.`datetime`) AS `user_id`,
    argMin(`urew`.`country`, `urew`.`datetime`) AS `country`,
    argMin(`urew`.`auth`, `urew`.`datetime`) AS `auth`,
    [('platform', toString(argMin(`urew`.`platform`, `urew`.`datetime`))), ('value', toString(argMin(`urew`.`value`, `urew`.`datetime`)))] as `params`
from
    `default`.`ug_rt_events_web` as `urew`
where
    `urew`.`date` between toDate(tupleElement(exp_data,2)) and if(tupleElement(exp_data,3) < tupleElement(exp_data,2), toDate(now()), toDate(tupleElement(exp_data,3)))
and
    `urew`.`datetime` between toDateTime(tupleElement(exp_data,2)) and if(tupleElement(exp_data,3) < tupleElement(exp_data,2), toDateTime(now()), toDateTime(tupleElement(exp_data,3)))
and
    `urew`.`unified_id` > 0
and
    (where_condition)
group by
    `unified_id`,
    `variation`
having
    (having_condition)

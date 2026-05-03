select
from
    from
    `default`.`ug_rt_events_app` as `urea`
inner join
    {exp_mobweb_users_table} as `emut`
on
    `urea`.`payment_account_id` = `emut`.`payment_account_id`
where
    `urea`.`date` = `date_filter`
and
    `urea`.`datetime` between toDateTime(tupleElement(exp_data,2)) and if(tupleElement(exp_data,3) < tupleElement(exp_data,2), toDateTime(now()), toDateTime(tupleElement(exp_data,3)))
and
    `urea`.`unified_id` > 0
and
    `urea`.`payment_account_id` > 0
and
    (where_condition)
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
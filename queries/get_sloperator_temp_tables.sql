select
    distinct `tab`.`table_name` as `table_name`
from
    `information_schema`.`tables` as `tab`
where
    `tab`.`table_schema` = 'sandbox'
and (
    `tab`.`table_name` like 'ug_monetization_sloperator_exp_users_%'
    or `tab`.`table_name` like 'ug_monetization_sloperator_exp_subscription_%'
)
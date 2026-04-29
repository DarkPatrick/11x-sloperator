select
    distinct `aex`.`id` as `id`
from
    `mysql_u_guitarcom`.`ab_experiment` as `aex`
where
    `aex`.`product` = 'UG'
and
    `aex`.`name` like '[UG Monetization]%'
and (
    `aex`.`status` = 1
    or `aex`.`date_end` >= toUnixTimestamp(today() - interval 30 day)
)

select
    *
from
    `mysql_u_guitarcom`.`ab_experiment` as `aex`
where
    `aex`.`id` = {id}
and
    `aex`.`product` = 'UG'

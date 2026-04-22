select
    `exi`.`other_answer` as `other_answer`
from
    `mysql_stats`.`answer_exit_interview` as `exi`
where
    toDate(`exi`.`date_created`) = today() - 1
and
    `exi`.`answer` = 7
and
    `exi`.`other_answer` != ''
and
    `exi`.`other_answer` != '"null"'

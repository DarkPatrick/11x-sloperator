-- в каком окне выгружаем данные
with {days_before} as `days_before`,

-- массив дат
-- нужен на тот случай, если в default.ug_subscriptions_events не будет данных за какой-то промежуток времени
dates as (
    select
        arrayJoin(
            arrayMap(
                x -> toStartOfInterval(now(), interval 1 hour) - interval x * 1 hour, 
                range(1, 1 + if(`days_before` > 0, 24 * `days_before`, toHour(now())))
            )
        ) as `dt`
)


select
    -- все поля обязательно оборачивать в backtick `` и только через алиас таблицы
    -- алиас поля тоже обязателен. после каждого должен стоять as даже если алиас совпадает с исходным именем поля
    -- алиасы полей тоже обязательно оборачиваются в backtick
    `sub`.`platform` as `platform`,
    -- toStartOfInterval(sub.`datetime`, interval 1 hour) as `dt`,
    `dat`.`dt` as `dt`,
    -- уникальная связка из subscription_id и product_code
    uniqExactIf((`sub`.`subscription_id`, `sub`.`product_code`), coalesce(`sub`.`subscription_id`, '') != '') as `subscription_cnt`
from
    -- алиасы обязательны даже если в селекте только одна таблица
    -- алиасы таблиц тоже обязательно оборачивать в backtick
    dates as `dat`
left join
    `default`.`ug_subscriptions_events` as `sub`
on
    `dat`.`dt` = toStartOfInterval(`sub`.`datetime`, interval 1 hour)
    
where
    -- фильтр даты по полю `date` обязателен
    `sub`.`date` between today() - `days_before` and today()
and
    -- чтобы исключить текущий период, за который данные неполные
    `dt` < toStartOfInterval(now(), interval 1 hour)
and
    -- фильтр на событие подписки
    `sub`.`event` = 'Subscribed'
and
    -- фильтр на платформы
    `sub`.`platform` in ('UGT_IOS', 'UGT_ANDROID', 'UG_WEB')
group by
    `dt`,
    `platform`
-- всегда используюй сортировку в финальном селекте
order by
    `dt`,
    `platform`

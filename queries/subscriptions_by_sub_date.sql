with
    toDate({date_start}) as date_start,
    toDate({date_end}) as date_end

select
    *,
    `revenue_gross` * case
        when lower(`platform`) like '%ios%' then 0.7
        when lower(`platform`) like '%and%' then 0.85
        else 1
    end as `revenue`,
    `refund_revenue_gross` * case
        when lower(`platform`) like '%ios%' then 0.7
        when lower(`platform`) like '%and%' then 0.85
        else 1
    end as `refund_revenue`,
    arraySum(arrayMap(x -> x.2 * 
        case
            when lower(`platform`) like '%ios%' and x.1 >= toDate(`subscribed_dt`) and x.1 < toDate(`subscribed_dt`) + interval 1 year then 0.7
            when lower(`platform`) like '%ios%' or lower(`platform`) like '%and%' then 0.85
            else 1
        end
        , `all_charges_arr_uniq`)
    ) as `lifetime_revenue`,
    countIf(`funnel_source` like '%Instant Offer%' and lower(`service_name`) like '%pro%') over(partition by `unified_id`) as `has_pro_instant_offer`,
    countIf(`funnel_source` like '%Instant Offer%' and lower(`service_name`) like '%book%') over(partition by `unified_id`) as `has_book_instant_offer`
from (
    select
        `use`.`subscription_id` as `subscription_id`,
        `use`.`product_code` as `product_code`,
        minIf(toUnixTimestamp(`use`.`datetime`), `use`.`event` = 'Subscribed') as `subscribed_dt`,
        minIf(toUnixTimestamp(`use`.`datetime`), `use`.`event` = 'Charged') as `charge_dt`,
        minIf(toUnixTimestamp(`use`.`datetime`), `use`.`event` = 'Canceled') as `cancel_dt`,
        minIf(toUnixTimestamp(`use`.`datetime`), `use`.`event` = 'Refunded') as `refund_dt`,
        minIf(toUnixTimestamp(`use`.`datetime`), `use`.`event` in ('Upgrade', 'Crossgrade')) as `upgrade_dt`,
        argMinIf(`use`.`platform`, `use`.`datetime`, `use`.`event` = 'Subscribed') as `platform`,
        argMinIf(
            case
                when `use`.`datetime_next_billing` < `use`.`datetime` then toUnixTimestamp(`use`.`datetime`)
                else toUnixTimestamp(`use`.`datetime_next_billing`)
            end,
            `use`.`datetime`, `use`.`event` = 'Subscribed'
        ) as `first_charge_expected_dt`,
        argMinIf(`use`.`trial`, `use`.`datetime`, `use`.`event` = 'Subscribed') as `trial`,
        argMinIf(`use`.`funnel_source`, `use`.`datetime`, `use`.`event` = 'Subscribed') as `funnel_source`,
        argMinIf(`use`.`product_id`, `use`.`datetime`, `use`.`event` = 'Subscribed') as `product_id`,
        argMinIf(`use`.`user_id`, `use`.`datetime`, `use`.`event` = 'Subscribed') as `user_id`,
        argMinIf(`use`.`unified_id`, `use`.`datetime`, `use`.`event` = 'Subscribed') as `unified_id`,
        argMinIf(`use`.`payment_account_id`, `use`.`datetime`, `use`.`event` = 'Subscribed') as `payment_account_id`,
        argMinIf(`use`.`service_name`, `use`.`datetime`, `use`.`event` = 'Subscribed') as `service_name`,
        argMinIf(`use`.`duration_count`, `use`.`datetime`, `use`.`event` = 'Subscribed') as `duration_count`,
        if (
            (`duration_count` = 0 and `service_name` = '' and `trial` = 0)
                or (product_id like 'onetime%' or product_id like '%|paid_trial')
            , 1, 0
        ) as `is_otp`,
        argMinIf(`use`.`usd_price`, `use`.`datetime`, `use`.`event` = 'Charged') as `revenue_gross`,
        argMinIf(
            case
                when `use`.`product_id` in ('com.ultimateguitar.tabs.plus.intro.1year', 'com.ultimateguitar.ugt.plus.intro.1year2', 'com.ultimateguitar.tabs.plus.1year7') then `use`.`usd_price` * 19.99/39.99
                else `use`.`usd_price`
            end, 
            `use`.`datetime`, `use`.`event` = 'Refunded'
        ) as `refund_revenue_gross`,
        argMinIf(-toFloat32OrZero(`use`.`params.str_value`[indexOf(`use`.`params.key`, 'usd_refund')]), `use`.`datetime`, `use`.`event` in ('Upgrade', 'Crossgrade')) as `upgrade_revenue`,
        groupArrayIf(
            (
                `use`.`date`,
                case
                    when `use`.`product_id` in ('com.ultimateguitar.tabs.plus.intro.1year', 'com.ultimateguitar.ugt.plus.intro.1year2', 'com.ultimateguitar.tabs.plus.1year7') then `use`.`usd_price` * 19.99/39.99
                    else `use`.`usd_price`
                end
            ), 
            `use`.`event` = 'Charged'
        ) as `all_charges_arr`,
        arrayFilter(
            (t, i) -> i = 1 or t.1 != `all_charges_arr`[i-1].1, 
            `all_charges_arr`, 
            arrayEnumerate(`all_charges_arr`)
        ) as `all_charges_arr_uniq`
    from
        `default`.`ug_subscriptions_events` as `use`
    where
        `use`.`date` >= date_start - interval 15 day
    and
        `use`.`event` in ('Subscribed', 'Charged', 'Canceled', 'Refunded', 'Crossgrade', 'Upgrade', 'Downgrade')
    group by
        `subscription_id`,
        `product_code`
    having
        toDate(`subscribed_dt`) between date_start - interval 15 day and date_end
    and
        lower(`funnel_source`) not like '%email%'
)

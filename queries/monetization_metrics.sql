-- select
--     s1.dt as dt,
--     s1.variation as variation,
--     s1.members as members,
--     s1.install_cnt as install_cnt,
--     s1.subscriber_cnt + s2.subscriber_cnt as subscriber_cnt,
--     s1.access_cnt + s2.access_cnt as access_cnt,
--     s1.access_instant_cnt + s2.access_instant_cnt as access_instant_cnt,
--     s1.access_ex_trial_cnt + s2.access_ex_trial_cnt as access_ex_trial_cnt,
--     s1.access_trial_cnt + s2.access_trial_cnt as access_trial_cnt,
--     s1.active_trial_cnt + s2.active_trial_cnt as active_trial_cnt,
--     s1.trial_subscriber_cnt + s2.trial_subscriber_cnt as trial_subscriber_cnt,
--     s1.charged_trial_cnt + s2.charged_trial_cnt as charged_trial_cnt,
--     s1.active_charged_trial_cnt + s2.active_charged_trial_cnt as active_charged_trial_cnt,
--     s1.access_otp_cnt + s2.access_otp_cnt as access_otp_cnt,
--     s1.cancel_trial_cnt + s2.cancel_trial_cnt as cancel_trial_cnt,
--     s1.trial_buyer_cnt + s2.trial_buyer_cnt as trial_buyer_cnt,
--     s1.late_charged_cnt + s2.late_charged_cnt as late_charged_cnt,
--     s1.buyer_cnt + s2.buyer_cnt as buyer_cnt,
--     s1.charge_cnt + s2.charge_cnt as charge_cnt,
--     s1.refund_14d_cnt + s2.refund_14d_cnt as refund_14d_cnt,
--     s1.revenue + s2.revenue as revenue,
--     s1.refund_revenue + s2.refund_revenue as refund_revenue,
--     s1.recurrent_charge_cnt + s2.recurrent_charge_cnt as recurrent_charge_cnt,
--     s1.recurrent_revenue + s2.recurrent_revenue as recurrent_revenue,
--     s1.trial_revenue + s2.trial_revenue as trial_revenue,
--     s1.active_trial_revenue + s2.active_trial_revenue as active_trial_revenue,
--     s1.lifetime_revenue + s2.lifetime_revenue as lifetime_revenue,
--     s1.upgrade_cnt + s2.upgrade_cnt as upgrade_cnt,
--     s1.upgrade_revenue + s2.upgrade_revenue as upgrade_revenue,
--     case
--         when members < 2 then 0
--         when s2.members < 2 then s1.arpu_var
--         when s1.members < 2 then s2.arpu_var
--         else ((s1.members - 1) * s1.arpu_var + (s2.members - 1) * s2.arpu_var + s1.members * power((s1.revenue / s1.members - revenue / members), 2) + s2.members * power((s2.revenue / s2.members - revenue / members), 2)) / (members - 1)
--     end as arpu_var,
--     case
--         when members < 2 then 0
--         when s2.members < 2 then s1.lifetime_arpu_var
--         when s1.members < 2 then s2.lifetime_arpu_var
--         else ((s1.members - 1) * s1.lifetime_arpu_var + (s2.members - 1) * s2.lifetime_arpu_var + s1.members * power((s1.lifetime_revenue / s1.members - lifetime_revenue / members), 2) + s2.members * power((s2.lifetime_revenue / s2.members - lifetime_revenue / members), 2)) / (members - 1)
--     end as lifetime_arpu_var,
--     case
--         when charge_cnt < 2 then 0
--         when s2.charge_cnt < 2 then s1.arppu_var
--         when s1.charge_cnt < 2 then s2.arppu_var
--         when s2.charge_cnt > 1 then ((s1.charge_cnt - 1) * s1.arppu_var + (s2.charge_cnt - 1) * s2.arppu_var + s1.charge_cnt * power((s1.revenue / s1.charge_cnt - revenue / charge_cnt), 2) + s2.charge_cnt * power((s2.revenue / s2.charge_cnt - revenue / charge_cnt), 2)) / (charge_cnt - 1)
--         else s1.arppu_var
--     end as arppu_var,
--     s1.cancel_14d_cnt + s2.cancel_14d_cnt as cancel_14d_cnt,
--     s1.cancel_1m_cnt + s2.cancel_1m_cnt as cancel_1m_cnt
-- from (
    select
        toDate(`eut`.`exp_start_dt`) as `dt`,
        `eut`.`variation` as `variation`,
        uniq(`eut`.`unified_id`) as `members`,
        uniq(`eut`.`payment_account_id`) as `install_cnt`,
        uniqIf(`eut`.`unified_id`, `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`is_otp` = 0) as `subscriber_cnt`,
        uniqIf(`eut`.`unified_id`, `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`is_otp` = 1) as `otp_owner_cnt`,
        uniqIf(`eut`.`unified_id`, `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800) as `access_owner_cnt`,
        uniqIf((`sta`.`subscription_id`, `sta`.`product_id`), `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and toDate(`sta`.`charge_dt`) = toDate(`sta`.`subscribed_dt`) and `sta`.`trial` = 0 and `sta`.`is_otp` = 0) as `access_instant_cnt`,
        uniqIf((`sta`.`subscription_id`, `sta`.`product_id`), `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`trial` > 0 and toDate(`sta`.`charge_dt`) = toDate(`sta`.`subscribed_dt`) and `sta`.`is_otp` = 0) as `access_ex_trial_cnt`,
        uniqIf((`sta`.`subscription_id`, `sta`.`product_id`), `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`trial` > 0 and not (toDate(`sta`.`charge_dt`) = toDate(`sta`.`subscribed_dt`)) and `sta`.`is_otp` = 0) as `access_trial_cnt`,
        uniqIf(`eut`.`unified_id`, `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`trial` > 0 and not (toDate(`sta`.`charge_dt`) = toDate(`sta`.`subscribed_dt`)) and `sta`.`is_otp` = 0) as `trial_subscriber_cnt`,
        uniqIf((`sta`.`subscription_id`, `sta`.`product_id`), `sta`.`subscribed_dt` < `eut`.`exp_start_dt` and `sta`.`trial` > 0 and not (toDate(`sta`.`charge_dt`) = toDate(`sta`.`subscribed_dt`)) and `sta`.`first_charge_expected_dt` > `eut`.`exp_start_dt` and `sta`.`is_otp` = 0 and (`sta`.`charge_dt` = 0 or `sta`.`charge_dt` > `eut`.`exp_start_dt`)) as `active_trial_cnt`,
        uniqIf((`sta`.`subscription_id`, `sta`.`product_id`), `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`trial` > 0 and not (toDate(`sta`.`charge_dt`) = toDate(`sta`.`subscribed_dt`)) and `sta`.`cancel_dt` < `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` > toUnixTimestamp(now()) and `sta`.`is_otp` = 0) as `pending_trial_cnt`,
        uniqIf((`sta`.`subscription_id`, `sta`.`product_id`), `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`is_otp` = 1) as `access_otp_cnt`,
        `access_instant_cnt` + `access_ex_trial_cnt` + `access_trial_cnt` as `subscriptions_cnt`,
        any(`spu`.`subscriptions_per_user_var`) as `subscriptions_per_user_var`,
        `subscriptions_cnt` + `access_otp_cnt` as `access_cnt`,

        uniqIf((`sta`.`subscription_id`, `sta`.`product_id`), `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`trial` > 0 and not (toDate(`sta`.`charge_dt`) = toDate(`sta`.`subscribed_dt`)) and `sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt`+ 86400 and `sta`.`is_otp` = 0) as `charged_trial_cnt`,
        uniqIf((`sta`.`subscription_id`, `sta`.`product_id`), `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`trial` > 0 and not (toDate(`sta`.`charge_dt`) = toDate(`sta`.`subscribed_dt`)) and (`sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt`+ 86400 or `sta`.`has_pro_instant_offer` > 0 and lower(`sta`.`service_name`) like '%pro%' or `sta`.`has_book_instant_offer` > 0 and lower(`sta`.`service_name`) like '%book%') and `sta`.`is_otp` = 0) as `any_charged_trial_cnt`,
        uniqIf((`sta`.`subscription_id`, `sta`.`product_id`), `sta`.`subscribed_dt` < `eut`.`exp_start_dt` and not (toDate(`sta`.`charge_dt`) = toDate(`sta`.`subscribed_dt`)) and `sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` + 86400 and `sta`.`is_otp` = 0) as `active_charged_trial_cnt`,
        uniqIf((`sta`.`subscription_id`, `sta`.`product_id`), `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`trial` > 0 and `sta`.`charge_dt` = 0 and `sta`.`cancel_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` + 86400 and `sta`.`is_otp` = 0) as `cancel_trial_cnt`,
        uniqIf(`eut`.`unified_id`, `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`trial` > 0 and not (toDate(`sta`.`charge_dt`) = toDate(`sta`.`subscribed_dt`)) and `sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` + 86400 and `sta`.`is_otp` = 0) as `trial_buyer_cnt`,
        uniqIf((`sta`.`subscription_id`, `sta`.`product_id`), `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`charge_dt` > `sta`.`first_charge_expected_dt` + 86400 and `sta`.`is_otp` = 0) as `late_charged_cnt`,
        uniqIf(`eut`.`unified_id`, `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` + 86400 and `sta`.`is_otp` = 0) as `subscribe_buyer_cnt`,
        uniqIf(`eut`.`unified_id`, `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` + 86400) as `buyer_cnt`,
        uniqIf((`sta`.`subscription_id`, `sta`.`product_id`), `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` + 86400 and `sta`.`is_otp` = 0) as `subscription_charge_cnt`,
        `subscription_charge_cnt` + `access_otp_cnt` as `charge_cnt`,
        uniqIf((`sta`.`subscription_id`, `sta`.`product_id`), `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` + 86400 and `sta`.`refund_dt` between `sta`.`charge_dt` and `sta`.`charge_dt` + 1209600) as `refund_14d_cnt`,
        uniqIf((`sta`.`subscription_id`, `sta`.`product_id`), `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` + 86400 and `sta`.`is_otp` = 0 and `sta`.`charge_dt` > toUnixTimestamp(now()) - 1209600) as `pending_14d_charge_cnt`,
        coalesce(sumIf(`sta`.`revenue`, `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` + 86400), 0)
            - coalesce(sumIf(`sta`.`refund_revenue`, `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` + 86400 and `sta`.`refund_dt` between `sta`.`charge_dt` and `sta`.`charge_dt` + 1209600), 0) as `revenue`,
        coalesce(sumIf(`sta`.`refund_revenue`, `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` + 86400 and `sta`.`refund_dt` between `sta`.`charge_dt` and `sta`.`charge_dt` + 1209600), 0) as `refund_revenue`,
        uniqIf((`sta`.`subscription_id`, `sta`.`product_id`), `sta`.`subscribed_dt` < `eut`.`exp_start_dt` and `sta`.`charge_dt` >= `sta`.`subscribed_dt`) as `recurrent_charge_cnt`,
        coalesce(sumIf(`sta`.`revenue`, `sta`.`subscribed_dt` < `eut`.`exp_start_dt` and `sta`.`charge_dt` >= `sta`.`subscribed_dt`), 0) as `recurrent_revenue`,
        coalesce(sumIf(`sta`.`revenue`, `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`trial` > 0 and not (toDate(`sta`.`charge_dt`) = toDate(`sta`.`subscribed_dt`)) and `sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` + 86400 and `sta`.`is_otp` = 0), 0) as `trial_revenue`,
        coalesce(sumIf(`sta`.`revenue`, `sta`.`subscribed_dt` < `eut`.`exp_start_dt` and `sta`.`trial` > 0 and not (toDate(`sta`.`charge_dt`) = toDate(`sta`.`subscribed_dt`)) and `sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` + 86400 and `sta`.`is_otp` = 0), 0) as `active_trial_revenue`,
        coalesce(sumIf(`sta`.`lifetime_revenue`, `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` + 86400), 0)
            - coalesce(sumIf(`sta`.`refund_revenue`, `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` + 86400 and `sta`.`refund_dt` between `sta`.`charge_dt` and `sta`.`charge_dt` + 1209600), 0) as `lifetime_revenue`,
        uniqIf((`sta`.`subscription_id`, `sta`.`product_id`), `sta`.`upgrade_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800) as `upgrade_cnt`,
        sumIf(`sta`.`upgrade_revenue`, `sta`.`upgrade_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800) as `upgrade_revenue`,
        varSamp(if(`sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` + 86400 and `sta`.`is_otp` = 0, `sta`.`revenue`, 0) - if(`sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` + 86400 and `sta`.`refund_dt` between `sta`.`charge_dt` and `sta`.`charge_dt` + 1209600, `sta`.`refund_revenue`, 0)) as `arpu_var`,
        varSamp(if(`sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` + 86400 and `sta`.`is_otp` = 0, `sta`.`lifetime_revenue`, 0) - if(`sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` + 86400 and `sta`.`refund_dt` between `sta`.`charge_dt` and `sta`.`charge_dt` + 1209600, `sta`.`refund_revenue`, 0)) as `lifetime_arpu_var`,
        varSampIf(if(`sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` + 86400 and `sta`.`is_otp` = 0, `sta`.`revenue`, 0) - if(`sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` + 86400 and `sta`.`refund_dt` between `sta`.`charge_dt` and `sta`.`charge_dt` + 1209600, `sta`.`refund_revenue`, 0), `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` + 86400 and `sta`.`is_otp` = 0) as `arppu_var`,
        uniqIf((`sta`.`subscription_id`, `sta`.`product_id`), `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` + 86400 and `sta`.`cancel_dt` between `sta`.`charge_dt` and `sta`.`charge_dt` + 1209600) as `cancel_14d_cnt`,
        uniqIf((`sta`.`subscription_id`, `sta`.`product_id`), `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`charge_dt` between `sta`.`subscribed_dt` and `sta`.`first_charge_expected_dt` + 86400 and `sta`.`cancel_dt` between `sta`.`charge_dt` and `sta`.`charge_dt` + 2592000) as `cancel_1m_cnt`
    from (
        select distinct *
        from {exp_users_table}
        where
            `client` = {client_sql}
        and
            `segment` = {segment_sql}
    ) as `eut`
    left join (
        select distinct *
        from {subscription_table}
    )as `sta`
    on
        `eut`.`unified_id` = `sta`.`unified_id`
    left join (
        select
            `dt`,
            `variation`,
            varSamp(`subscriptions_per_user_cnt`) as `subscriptions_per_user_var`
        from (
            select
                toDate(`eut`.`exp_start_dt`) as `dt`,
                `eut`.`variation` as `variation`,
                `eut`.`unified_id` as `unified_id`,
                uniqIf((`sta`.`subscription_id`, `sta`.`product_id`), `sta`.`subscribed_dt` between `eut`.`exp_start_dt` and `eut`.`exp_start_dt` + 604800 and `sta`.`is_otp` = 0) as `subscriptions_per_user_cnt`
            from (
                select distinct *
                from {exp_users_table}
                where
                    `client` = {client_sql}
                and
                    `segment` = {segment_sql}
            ) as `eut`
            left join (
                select distinct *
                from {subscription_table}
            ) as `sta`
            on
                `eut`.`unified_id` = `sta`.`unified_id`
            group by
                `dt`,
                `variation`,
                `unified_id`
        )
        group by
            `dt`,
            `variation`
    ) as `spu`
    on
        toDate(`eut`.`exp_start_dt`) = `spu`.`dt`
    and
        `eut`.`variation` = `spu`.`variation`
    group by
        toDate(`eut`.`exp_start_dt`),
        `eut`.`variation`
    order by
        `dt`,
        `variation`
    -- ) as s1
-- left join (
--     select
--         toDate(`eut`.exp_start_dt) as dt,
--         `eut`.variation as variation,
--         uniq(payment_account_id) as members,
--         uniqIf(payment_account_id, subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800) as subscriber_cnt,
--         access_instant_cnt + access_ex_trial_cnt + access_trial_cnt as access_cnt,
--         uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and toDate(charge_dt) = toDate(subscribed_dt) and trial = 0 and duration_count > 0) as access_instant_cnt,
--         uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and trial > 0 and toDate(charge_dt) = toDate(subscribed_dt) and duration_count > 0) as access_ex_trial_cnt,
--         uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and duration_count > 0) as access_trial_cnt,
--         uniqIf((subscription_id, product_id), subscribed_dt < exp_start_dt and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and toDateTime(first_charge_expected_dt) + interval 1 day > exp_start_dt and duration_count > 0) as active_trial_cnt,
--         uniqIf(payment_account_id, subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and duration_count > 0) as trial_subscriber_cnt,
--         uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0) as charged_trial_cnt,
--         uniqIf((subscription_id, product_id), subscribed_dt < exp_start_dt and not (toDate(charge_dt) = toDate(subscribed_dt)) and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0) as active_charged_trial_cnt,
--         uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and trial = 0 and service_name = '' and duration_count = 0) as access_otp_cnt,
--         uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and trial > 0 and toDate(charge_dt) = toDateTime(0) and toDateTime(cancel_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0) as cancel_trial_cnt,
--         uniqIf(payment_account_id, subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0) as trial_buyer_cnt,
--         uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and toDateTime(charge_dt) > toDateTime(first_charge_expected_dt) + interval 1 day) as late_charged_cnt,
--         uniqIf(payment_account_id, subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) as buyer_cnt,
--         uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) as charge_cnt,
--         uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day) as refund_14d_cnt,
--         sumIf(`sta`revenue, subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) 
--             - sumIf(`sta`refund_revenue, subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day) as revenue,
--         sumIf(`sta`refund_revenue, subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day) as refund_revenue,
--         uniqIf((subscription_id, product_id), subscribed_dt < exp_start_dt and charge_dt >= subscribed_dt) as recurrent_charge_cnt,
--         sumIf(`sta`revenue, subscribed_dt < exp_start_dt and charge_dt >= subscribed_dt) as recurrent_revenue,
--         sumIf(`sta`revenue, subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0) as trial_revenue,
--         sumIf(`sta`revenue, subscribed_dt < exp_start_dt and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0) as active_trial_revenue,
--         sumIf(`sta`lifetime_revenue, subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) 
--             - sumIf(`sta`refund_revenue, subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day) as lifetime_revenue,
--         uniqIf((subscription_id, product_id), upgrade_dt between exp_start_dt and `eut`.exp_start_dt + 604800) as upgrade_cnt,
--         sumIf(`sta`upgrade_revenue, upgrade_dt between exp_start_dt and `eut`.exp_start_dt + 604800) as upgrade_revenue,
--         varSamp(if(subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day, `sta`revenue, 0) - if(subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(`sta`refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day, `sta`refund_revenue, 0)) as arpu_var,
--         varSamp(if(subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day, `sta`lifetime_revenue, 0) - if(subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(`sta`refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day, `sta`refund_revenue, 0)) as lifetime_arpu_var,
--         varSampIf(if(subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day, `sta`revenue, 0) - if(subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(`sta`refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day, `sta`refund_revenue, 0), subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) as arppu_var,
--         uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(cancel_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day) as cancel_14d_cnt,
--         uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and `eut`.exp_start_dt + 604800 and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(cancel_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 1 month) as cancel_1m_cnt
--     from
--         {exp_users_table} as `eut`
--     inner join
--         {subscription_table} as `sta`
--     using(payment_account_id)
--     where
--         `eut`.payment_account_id > 0
--     and
--         `eut`.unified_id != `sta`unified_id
--     group by
--         dt,
--         variation
-- ) as s2
-- on
--     s1.dt = s2.dt
-- and
--     s1.variation = s2.variation

# Конфигурация SQL-запросов
SQL_QUERIES = {
    'common_kpi': {
        'query': "select payments.get_kpi(:start_date, :end_date);",
        'params': {'start_date': ':start_week', 'end_date': ':end_week'}
    },
    'tf_sr_payouts': {
        'query': """
            with b as (
                select * 
                from 
                    payments.payment_requests_cashed prc
                where 
                    status in ('FINISHED') 
                    and purpose in ('PAYOUT', 'PARTNER_REWARD') 
                    and "sender_type" in ('WALLET', 'STOCK') 
                    and updated_at between :start_week and :end_week
                    and lower(prc.risk_control_payload::text) not like '%unknown partner%' 
                    and lower(prc.risk_control_payload::text) not like '%unknown payout%'
            ),
            c as (
                SELECT distinct on(id)
                    (data->>'updated_at')::timestamp as submitted_time,*
                FROM 
                    b, 
                    jsonb_array_elements(status_change_history::jsonb) data
                WHERE 
                    data->>'status' in ('SUBMITTED', 'WAITING_STOCK')
            )
            select 
                count(*) as total_payouts,
                count(*) filter (where submitted_time - created_at > '00:01:00' and "currency" <> 'btc') as long_payouts,
                count(*) filter (where submitted_time - created_at > '00:04:00' and "currency" = 'btc')  as long_payouts_btc
            from c;
        """,
        'params': {'start_week': ':start_week', 'end_week': ':end_week_exclusive'}
    },
    'tf_sr_partner_liability': {
        'query': """
            select 
                count(*) as total_payouts, 
                count(*) filter (where updated_at - created_at >'00:01:00') as long_payouts
            from (
                select * from payments.payment_requests_cashed prc
                where status = 'FINISHED' and purpose in ('PAYOUT') 
                and "sender_type" in ('PARTNER_LIABILITY') 
                and updated_at between :start_week and :end_week
                and lower(prc.risk_control_payload::text) not like '%unknown partner%' 
                and lower(prc.risk_control_payload::text) not like '%unknown payout%'
            ) subq1;
        """,
        'params': {'start_week': ':start_week', 'end_week': ':end_week_exclusive'}
    },
    'tf_sr_submitted_to_finished': {
        'query': """
            select avg(finished_time-submitted_time) as avg_duration from (
                select distinct on(id) (jb ->> 'updated_at')::timestamp as submitted_time, prc.updated_at as finished_time
                from payments.payment_requests prc, jsonb_array_elements(status_change_history::jsonb) jb
                where status ='FINISHED' 
                    and purpose in ('PAYOUT', 'PARTNER_REWARD') 
                    and "sender_type" in ('WALLET', 'STOCK') 
                    and created_at between :start_week and :end_week
                    and jb ->> 'status' in ('SUBMITTED', 'WAITING_STOCK')
                    and lower(prc.risk_control_payload::text) not like '%unknown partner%' 
                    and lower(prc.risk_control_payload::text) not like '%unknown payout%'
            ) sub1;
        """,
        'params': {'start_week': ':start_week', 'end_week': ':end_week_exclusive'}
    },
    'tf_sr_median_payouts': {
        'query': """
            with tmp_median as (
                select 
                    prc.id, 
                    prc.currency, 
                    (jb ->> 'updated_at')::timestamp as updated_at, 
                    prc.created_at,
                    jb ->> 'status' as status
                from 
                    payments.payment_requests_cashed prc, 
                    jsonb_array_elements(prc.status_change_history) jb
                where 
                    prc.created_at between :start_week and :end_week
                    and prc.purpose in ('PAYOUT', 'PARTNER_REWARD')
                    and prc."sender_type" in ('WALLET') 
                    and (prc.status in ('FINISHED') or status_change_history::text like '%WAITING_STOCK%')
                    and lower(prc.risk_control_payload::text) not like '%unknown partner%' 
                    and lower(prc.risk_control_payload::text) not like '%unknown payout%'
                order by updated_at desc)
            select PERCENTILE_CONT(0.5) within GROUP(order by updated_at::timestamp - created_at) as median_duration
            from tmp_median where status = 'SUBMITTED';
        """,
        'params': {'start_week': ':start_week', 'end_week': ':end_week_exclusive'}
    },
    'statuses_payout_reward': {
        'query': """
            drop table if exists tmp_statuses;
            create temp table tmp_statuses as 
            select 
                prc.id, 
                prc.currency, 
                (jb ->> 'updated_at')::timestamp as updated_at, 
                jb ->> 'status' as status
            from 
                payments.payment_requests_cashed prc, 
                jsonb_array_elements(prc.status_change_history) jb
            where 
                prc.created_at between :start_week and :end_week
                and prc.purpose in ('PAYOUT', 'PARTNER_REWARD')
                and prc."sender_type" in ('WALLET', 'STOCK') 
                and (prc.status in ('FINISHED') or status_change_history::text like '%WAITING_STOCK%')
                and lower(prc.risk_control_payload::text) not like '%unknown partner%' 
                and lower(prc.risk_control_payload::text) not like '%unknown payout%'
            order by updated_at desc;

            select 
                avg(to_SS) as _to_SETTING_SENDER, 
                avg(to_WRC) as _to_WAITING_RISK_CONTROL, 
                avg(to_CBRC) as _to_CHECKING_BY_RISK_CONTROL,
                avg(to_N2) as _in_NOT_ENOUGH_BALANCE,
                avg(to_N) as _to_NEW,
                avg(to_BR1) as _to_BUILDING_RAW,
                avg(to_WAITING_BUILD_RAW) as _to_WAITING_BUILD_RAW,
                avg(to_BR2) as _to_BUILT_RAW,
                avg(to_SR) as _to_SENDING_RAW,
                avg(to_S) as _to_SUBMITTED,
                avg(to_F) as _to_FINISHED
            from (
            with maxes_updates as (
                select id as pr_id, currency, 
                max(updated_at) filter (where status::text = 'INIT') as updated_at_to_INIT,
                coalesce(max(updated_at) filter (where status::text = 'NOT_ENOUGH_BALANCE'), NULL) as updated_at_to_NOT_ENOUGH_BALANCE,
                max(updated_at) filter (where status::text = 'SETTING_SENDER') as updated_at_to_SETTING_SENDER, 
                max(updated_at) filter (where status::text = 'WAITING_RISK_CONTROL') as updated_at_to_WAITING_RISK_CONTROL, 
                max(updated_at) filter (where status::text = 'CHECKING_BY_RISK_CONTROL') as updated_at_to_CHECKING_BY_RISK_CONTROL, 
                max(updated_at) filter (where status::text = 'NEW') as updated_at_to_NEW,
                max(updated_at) filter (where status::text = 'BUILDING_RAW') as updated_at_to_BUILDING_RAW, 
                max(updated_at) filter (where status::text = 'WAITING_BUILD_RAW') as updated_at_to_WAITING_BUILD_RAW, 
                max(updated_at) filter (where status::text = 'BUILT_RAW') as updated_at_to_BUILT_RAW, 
                max(updated_at) filter (where status::text = 'SENDING_RAW') as updated_at_to_SENDING_RAW, 
                max(updated_at) filter (where status::text = 'SUBMITTED') as updated_at_to_SUBMITTED,
                max(updated_at) filter (where status::text = 'FINISHED') as updated_at_to_FINISHED
                from tmp_statuses 
                group by 1,2
            )
            select pr_id, currency,
            coalesce(updated_at_to_NEW-updated_at_to_NOT_ENOUGH_BALANCE, interval '0 seconds') as to_N2, 
            coalesce(updated_at_to_SETTING_SENDER-updated_at_to_INIT, interval '0 seconds') as to_SS, 
            coalesce(updated_at_to_WAITING_RISK_CONTROL-updated_at_to_SETTING_SENDER, interval '0 seconds') as to_WRC, 
            coalesce(updated_at_to_CHECKING_BY_RISK_CONTROL-updated_at_to_WAITING_RISK_CONTROL, interval '0 seconds') as to_CBRC,
            coalesce(updated_at_to_NEW-updated_at_to_CHECKING_BY_RISK_CONTROL, interval '0 seconds') as to_N,
            coalesce(updated_at_to_BUILDING_RAW-updated_at_to_NEW, interval '0 seconds') as to_BR1,
            coalesce(updated_at_to_BUILT_RAW-updated_at_to_WAITING_BUILD_RAW, interval '0 seconds') as to_WAITING_BUILD_RAW,
            coalesce(updated_at_to_BUILT_RAW-updated_at_to_BUILDING_RAW, interval '0 seconds') as to_BR2,
            coalesce(updated_at_to_SENDING_RAW-updated_at_to_BUILT_RAW, interval '0 seconds') as to_SR,
            coalesce(updated_at_to_SUBMITTED-updated_at_to_SENDING_RAW, interval '0 seconds') as to_S,
            coalesce(updated_at_to_FINISHED-updated_at_to_SUBMITTED, interval '0 seconds') as to_F 
            from maxes_updates) subq1;
        """,
        'params': {'start_week': ':start_week', 'end_week': ':end_week_exclusive'}
    },
    'sr_payouts_slow': {
        'query': """
            drop table if exists tmp_processing_all;
            create temp table tmp_processing_all as 
            select 
                prc.currency, 
                (jb ->> 'updated_at')::timestamp - prc.created_at as time_processing_SUBMITTED,
                prc.updated_at - prc.created_at as time_processing_FINISHED,
                case 
                    when prc.currency in (select currency from payments.currencies_cashed where token_type in ('sol', 'tontoken') or currency in ('sol', 'ton'))
                        then '00:00:20'::interval
                    when prc.currency = 'btc'
                        then '00:01:45'::interval
                    when prc.currency = 'xmr'
                        then '00:01:15'::interval
                    else '00:00:08'::interval
                end as metric,
                prc.requester_id
            from 
                payments.payment_requests_cashed prc, 
                jsonb_array_elements(prc.status_change_history) jb
            where 
                prc.created_at between :start_week and :end_week
                and prc.purpose in ('PAYOUT', 'PARTNER_REWARD')
                and prc."sender_type" in ('WALLET', 'STOCK') 
                and (prc.status in ('FINISHED') or status_change_history::text like '%WAITING_STOCK%')
                and (jb ->> 'status')::text = 'SUBMITTED'
                and lower(prc.risk_control_payload::text) not like '%unknown partner%' 
                and lower(prc.risk_control_payload::text) not like '%unknown payout%';

            -- SR payouts / (total slow payouts)
            select (round(fail_count::numeric * 100 / cnt_total::numeric, 2))::text || '%' as long_count
            from (
                select count(*) as cnt_total, count(*) filter (where time_processing_SUBMITTED > metric) as fail_count 
                from tmp_processing_all
            ) subq2;
        """,
        'params': {'start_week': ':start_week', 'end_week': ':end_week_exclusive'}
    },
    'currency_stats': {
        'query': """
            drop table if exists tmp_processing_all;
            create temp table tmp_processing_all as 
            select 
                prc.currency, 
                (jb ->> 'updated_at')::timestamp - prc.created_at as time_processing_SUBMITTED,
                prc.updated_at - prc.created_at as time_processing_FINISHED,
                case 
                    when prc.currency in (select currency from payments.currencies_cashed where token_type in ('sol', 'tontoken') or currency in ('sol', 'ton'))
                        then '00:00:20'::interval
                    when prc.currency = 'btc'
                        then '00:01:45'::interval
                    when prc.currency = 'xmr'
                        then '00:01:15'::interval
                    else '00:00:08'::interval
                end as metric,
                prc.requester_id
            from 
                payments.payment_requests_cashed prc, 
                jsonb_array_elements(prc.status_change_history) jb
            where 
                prc.created_at between :start_week and :end_week
                and prc.purpose in ('PAYOUT', 'PARTNER_REWARD')
                and prc."sender_type" in ('WALLET', 'STOCK') 
                and (prc.status in ('FINISHED') or status_change_history::text like '%WAITING_STOCK%')
                and (jb ->> 'status')::text = 'SUBMITTED'
                and lower(prc.risk_control_payload::text) not like '%unknown partner%' 
                and lower(prc.risk_control_payload::text) not like '%unknown payout%';

            select 
                network, 
                currency, 
                cnt_total, 
                avg_time_processing_SUBMITTED, 
                median_time_processing_SUBMITTED, 
                avg_time_processing_FINISHED, 
                median_time_processing_FINISHED, 
                metric, 
                round(fail_count::numeric/cnt_total::numeric, 4) * 100 as tf_sr_currency
            from (
                with all_currency as (
                select 
                    cc.network,
                    tpa.currency, 
                    count(*) as cnt_total, 
                    (avg(time_processing_SUBMITTED))::interval as avg_time_processing_SUBMITTED, 
                    PERCENTILE_CONT(0.5) within GROUP(order by time_processing_SUBMITTED) as median_time_processing_SUBMITTED,
                    avg(time_processing_FINISHED) as avg_time_processing_FINISHED, 
                    PERCENTILE_CONT(0.5) within GROUP(order by time_processing_FINISHED) as median_time_processing_FINISHED,
                    count(*) filter (where time_processing_SUBMITTED > metric) as fail_count,
                    metric
                from tmp_processing_all tpa
                inner join payments.currencies cc on cc.currency = tpa.currency
                group by 
                    cc.network, 
                    tpa.currency, 
                    metric
                order by cnt_total desc)
                    select * from all_currency
            ) subq1;
        """,
        'params': {'start_week': ':start_week', 'end_week': ':end_week_exclusive'}
    },
    'statuses_partner_liability': {
        'query': """
            drop table if exists tmp_statuses;
            create temp table tmp_statuses as 
            select 
                prc.id, 
                prc.currency, 
                (jb ->> 'updated_at')::timestamp as updated_at, 
                jb ->> 'status' as status
            from 
                payments.payment_requests prc, 
                jsonb_array_elements(prc.status_change_history) jb
            where 
                prc.created_at between :start_week and :end_week
                and prc.purpose in ('PAYOUT')
                and prc."sender_type" in ('PARTNER_LIABILITY') 
                and prc.status = 'FINISHED' 
                and lower(prc.risk_control_payload::text) not like '%unknown partner%' 
                and lower(prc.risk_control_payload::text) not like '%unknown payout%'
            order by updated_at desc;

            select 
                avg(to_SS) as _to_SETTING_SENDER, 
                avg(to_WRC) as _to_WAITING_RISK_CONTROL, 
                avg(to_CBRC) as _to_CHECKING_BY_RISK_CONTROL,
                avg(to_N) as _to_NEW,
                avg(to_P) as _to_PROCESSING,
                avg(to_F) as _to_FINISHED
            from (
            with maxes_updates as (
                select id as pr_id, currency, 
                max(updated_at) filter (where status::text = 'INIT') as updated_at_to_INIT,
                max(updated_at) filter (where status::text = 'SETTING_SENDER') as updated_at_to_SETTING_SENDER, 
                max(updated_at) filter (where status::text = 'WAITING_RISK_CONTROL') as updated_at_to_WAITING_RISK_CONTROL, 
                max(updated_at) filter (where status::text = 'CHECKING_BY_RISK_CONTROL') as updated_at_to_CHECKING_BY_RISK_CONTROL, 
                max(updated_at) filter (where status::text = 'NEW') as updated_at_to_NEW,
                max(updated_at) filter (where status::text = 'PROCESSING') as updated_at_to_PROCESSING, 
                max(updated_at) filter (where status::text = 'FINISHED') as updated_at_to_FINISHED
                from tmp_statuses group by 1,2
            )
            select pr_id, currency,
            coalesce(updated_at_to_SETTING_SENDER-updated_at_to_INIT, interval '0 seconds') as to_SS, 
            coalesce(updated_at_to_WAITING_RISK_CONTROL-updated_at_to_SETTING_SENDER, interval '0 seconds') as to_WRC, 
            coalesce(updated_at_to_CHECKING_BY_RISK_CONTROL-updated_at_to_WAITING_RISK_CONTROL, interval '0 seconds') as to_CBRC,
            coalesce(updated_at_to_NEW-updated_at_to_CHECKING_BY_RISK_CONTROL, interval '0 seconds') as to_N,
            coalesce(updated_at_to_PROCESSING-updated_at_to_NEW, interval '0 seconds') as to_P,
            coalesce(updated_at_to_FINISHED-updated_at_to_PROCESSING, interval '0 seconds') as to_F 
            from maxes_updates) subq1;
        """,
        'params': {'start_week': ':start_week', 'end_week': ':end_week_exclusive'}
    }
}

# Запросы для параллельного выполнения
PARALLEL_QUERIES = [
    'common_kpi',
    'tf_sr_payouts',
    'tf_sr_partner_liability',
    'tf_sr_submitted_to_finished',
    'tf_sr_median_payouts',
    'statuses_payout_reward',
    'sr_payouts_slow',
    'currency_stats',
    'statuses_partner_liability'
]
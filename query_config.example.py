# SQL Queries Configuration Template
# Copy this file to query_config.py and customize with your actual queries

SQL_QUERIES = {
    'common_kpi': {
        'query': "SELECT your_schema.your_function(:start_date, :end_date);",
        'params': {'start_date': ':start_week', 'end_date': ':end_week'}
    },
    'tf_sr_payouts': {
        'query': """
            WITH filtered_data AS (
                SELECT * 
                FROM your_schema.your_table
                WHERE 
                    status IN ('FINISHED') 
                    AND purpose IN ('PAYOUT', 'PARTNER_REWARD') 
                    AND sender_type IN ('WALLET', 'STOCK') 
                    AND updated_at BETWEEN :start_week AND :end_week
            )
            SELECT 
                COUNT(*) AS total_payouts,
                COUNT(*) FILTER (WHERE processing_time > '00:01:00') AS long_payouts
            FROM filtered_data;
        """,
        'params': {'start_week': ':start_week', 'end_week': ':end_week_exclusive'}
    },
    'tf_sr_partner_liability': {
        'query': """
            SELECT 
                COUNT(*) AS total_payouts, 
                COUNT(*) FILTER (WHERE updated_at - created_at > '00:01:00') AS long_payouts
            FROM your_schema.your_table
            WHERE 
                status = 'FINISHED' 
                AND purpose IN ('PAYOUT') 
                AND sender_type IN ('PARTNER_LIABILITY') 
                AND updated_at BETWEEN :start_week AND :end_week;
        """,
        'params': {'start_week': ':start_week', 'end_week': ':end_week_exclusive'}
    },
    'tf_sr_submitted_to_finished': {
        'query': """
            SELECT AVG(finished_time - submitted_time) AS avg_duration 
            FROM (
                SELECT 
                    submitted_time, 
                    finished_time
                FROM your_schema.your_table
                WHERE 
                    status = 'FINISHED' 
                    AND created_at BETWEEN :start_week AND :end_week
            ) subq;
        """,
        'params': {'start_week': ':start_week', 'end_week': ':end_week_exclusive'}
    },
    'tf_sr_median_payouts': {
        'query': """
            SELECT 
                PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY processing_duration) AS median_duration
            FROM (
                SELECT 
                    id,
                    processing_duration
                FROM your_schema.your_table
                WHERE 
                    created_at BETWEEN :start_week AND :end_week
                    AND status = 'FINISHED'
            ) subq;
        """,
        'params': {'start_week': ':start_week', 'end_week': ':end_week_exclusive'}
    },
    'statuses_payout_reward': {
        'query': """
            SELECT 
                AVG(time_to_status_1) AS _to_status_1,
                AVG(time_to_status_2) AS _to_status_2,
                AVG(time_to_status_3) AS _to_status_3
            FROM (
                SELECT 
                    id,
                    status_1_time - init_time AS time_to_status_1,
                    status_2_time - status_1_time AS time_to_status_2,
                    status_3_time - status_2_time AS time_to_status_3
                FROM your_schema.your_status_table
                WHERE created_at BETWEEN :start_week AND :end_week
            ) subq;
        """,
        'params': {'start_week': ':start_week', 'end_week': ':end_week_exclusive'}
    },
    'sr_payouts_slow': {
        'query': """
            SELECT 
                ROUND(COUNT(*) FILTER (WHERE processing_time > threshold)::numeric * 100.0 / COUNT(*)::numeric, 2)::text || '%' AS long_count
            FROM your_schema.your_table
            WHERE 
                created_at BETWEEN :start_week AND :end_week
                AND status = 'FINISHED';
        """,
        'params': {'start_week': ':start_week', 'end_week': ':end_week_exclusive'}
    },
    'currency_stats': {
        'query': """
            SELECT 
                currency,
                COUNT(*) AS cnt_total,
                AVG(processing_time) AS avg_time,
                PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY processing_time) AS median_time
            FROM your_schema.your_table
            WHERE created_at BETWEEN :start_week AND :end_week
            GROUP BY currency
            ORDER BY cnt_total DESC;
        """,
        'params': {'start_week': ':start_week', 'end_week': ':end_week_exclusive'}
    },
    'statuses_partner_liability': {
        'query': """
            SELECT 
                AVG(time_to_status_1) AS _to_status_1,
                AVG(time_to_status_2) AS _to_status_2,
                AVG(time_to_status_3) AS _to_status_3
            FROM (
                SELECT 
                    id,
                    status_1_time - init_time AS time_to_status_1,
                    status_2_time - status_1_time AS time_to_status_2,
                    status_3_time - status_2_time AS time_to_status_3
                FROM your_schema.your_table
                WHERE 
                    created_at BETWEEN :start_week AND :end_week
                    AND sender_type = 'PARTNER_LIABILITY'
            ) subq;
        """,
        'params': {'start_week': ':start_week', 'end_week': ':end_week_exclusive'}
    }
}

# Queries for parallel execution
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


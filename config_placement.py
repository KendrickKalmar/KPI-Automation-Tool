PLACEMENT_CONFIG = {
    'common_kpi': {
        'spreadsheet_id': 'depo_kpi',
        'sheet_name': 'Services',
        'data_type': 'common_kpi_table',
        'placement': {
            'custody_deposit': {
                'value1': 'B',
                'value2': 'D',
                'value3': 'F',
            },
            'custody_conversion': {
                'value1': 'J',
                'value2': 'L',
                'value3': 'N',
                'value4': 'P',
            },
            'BALANCING': {
                'value1': 'AD',
                'value2': 'AF',
            },
            'PAYOUT': {
                'value1': 'AH',
                'value2': 'AJ',
            },
            'ROUTING': {
                'value1': 'AL',
                'value2': 'AN',
            },
            'reward': {
                'value1': 'R',
                'value2': 'T',
                'value3': 'V',
                'value4': 'X',
            },
        },
    },
    'custody_deposits_avg': {
        'spreadsheet_id': 'depo_kpi',
        'sheet_name': 'Services',
        'data_type': 'single_value',
        'placement': {
            'avg_duration_sending_weekly': 'H',
        },
    },
    'tf_sr_payouts': {
        'spreadsheet_id': 'sr_plus',
        'sheet_name': 'TF_SR',
        'data_type': 'single_value',
        'placement': {
            'total_payouts': 'B',
            'long_payouts': 'C',
        }
    },
    'tf_sr_partner_liability': {
        'spreadsheet_id': 'sr_plus',
        'sheet_name': 'TF_SR',
        'data_type': 'single_value',
        'placement': {
            'total_payouts': 'D',
            'long_payouts': 'E',
        }
    },
    'tf_sr_submitted_to_finished': {
        'spreadsheet_id': 'sr_plus',
        'sheet_name': 'TF_SR',
        'data_type': 'single_value',
        'placement': {
            'avg_duration': 'I',
        }
    },
    'tf_sr_median_payouts': {
        'spreadsheet_id': 'sr_plus',
        'sheet_name': 'TF_SR',
        'data_type': 'single_value',
        'placement': {
            'median_duration': 'L',
        }
    },
    'statuses_payout_reward': {
        'spreadsheet_id': 'sr_plus',
        'sheet_name': 'status',
        'data_type': 'statuses_shift',
        'placement': {
            'status_values': 'E5:E15'
        }
    },
    'statuses_partner_liability': {
        'spreadsheet_id': 'sr_plus',
        'sheet_name': 'status',
        'data_type': 'statuses_shift',
        'placement': {
            'status_values': 'C20:C25'
        }
    },
    'sr_payouts_slow': {
        'spreadsheet_id': 'sr_plus',
        'sheet_name': 'status',
        'data_type': 'single_value',
        'placement': {
            'long_count': 'E16',
        }
    },
    'currency_stats': {
        'spreadsheet_id': 'sr_plus',
        'sheet_name': 'currency',
        'data_type': 'table',
        'placement': {
            'start_cell': 'A1',
        }
    },
    'tf_sr_links_increment': {
        'spreadsheet_id': 'sr_plus',
        'sheet_name': 'status',
        'data_type': 'single_value',
        'placement': {
            'noop': 'A1'
        }
    },
}

QUERY_TO_CONFIG_MAPPING = {
    'common_kpi': 'common_kpi',
    'custody_deposits': 'custody_deposits_avg',
    'tf_sr_payouts': 'tf_sr_payouts',
    'tf_sr_partner_liability': 'tf_sr_partner_liability',
    'tf_sr_submitted_to_finished': 'tf_sr_submitted_to_finished',
    'tf_sr_median_payouts': 'tf_sr_median_payouts',
    'statuses_payout_reward': 'statuses_payout_reward',
    'statuses_partner_liability': 'statuses_partner_liability',
    'sr_payouts_slow': 'sr_payouts_slow',
    'currency_stats': 'currency_stats'
}

PARALLEL_QUERIES = [
    'common_kpi',
    'tf_sr_payouts',
    'tf_sr_partner_liability',
    'tf_sr_submitted_to_finished',
    'statuses_payout_reward',
    'sr_payouts_slow',
    'currency_stats',
    'statuses_partner_liability'
]
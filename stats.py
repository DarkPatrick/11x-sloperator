import logging
import pandas as pd
from typing import List, Optional, Any

from clickhouse_worker import execute_sql, get_experiment, create_experiment_users_table, create_experiments_subscription_table, drop_table, get_monetization_metrics


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def calculate_exp_info(exp_id) -> tuple[pd.DataFrame, str]:
    exp_info: dict = get_experiment(exp_id)
    exp_users_table: str = create_experiment_users_table(exp_info)
    subscription_table: str = create_experiments_subscription_table(exp_info)
    logger.info("exp_users_table:\n%s, subscription_table: %s", exp_users_table, subscription_table)
    df: pd.DataFrame = get_monetization_metrics(exp_info, exp_users_table, subscription_table)
    # drop_table(exp_users_table)
    # drop_table(subscription_table)
    return df, f"exp_users_table={exp_users_table}, subscription_table={subscription_table}"

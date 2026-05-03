# TODO: add calculation by source UGT_ANDROI / UGT_IOS / UG_WEB / ...
import logging
import pandas as pd
import numpy as np
import scipy.stats as stats
import scipy.special as special
from statsmodels.stats.power import TTestIndPower
import math
import yaml
from typing import List, Optional, Any

from clickhouse_worker import execute_sql, execute_sql_modify, get_experiment, create_experiment_users_table, create_experiments_subscription_table, drop_table, get_monetization_metrics, create_exp_results_table, update_exp_results_table, drop_exp_partitions, create_exp_stats_table



logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



def fill_missing_variations_by_date(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["dt"] = pd.to_datetime(df["dt"])

    all_dates = pd.date_range(df["dt"].min(), df["dt"].max(), freq="D")
    all_variations = df["variation"].dropna().unique()

    full_index = pd.MultiIndex.from_product(
        [all_dates, all_variations],
        names=["dt", "variation"]
    )

    df = (
        df.set_index(["dt", "variation"])
        .reindex(full_index)
        .reset_index()
    )

    value_cols = [
        col for col in df.columns
        if col not in ["dt", "variation"]
    ]

    df[value_cols] = df[value_cols].fillna(0)

    return df


def calc_cumulative_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    df = fill_missing_variations_by_date(df)
    df = df.copy()
    df["dt"] = pd.to_datetime(df["dt"])

    var_config = {
        "arpu_var": {
            "count_col": "members",
            "revenue_col": "revenue",
        },
        "lifetime_arpu_var": {
            "count_col": "members",
            "revenue_col": "lifetime_revenue",
        },
        "arppu_var": {
            "count_col": "buyer_cnt",
            "revenue_col": "revenue",
        },
    }

    var_cols = set(var_config.keys())

    regular_cols = [
        col for col in df.columns
        if col not in ["dt", "variation"] and col not in var_cols
    ]

    result_parts = []

    for variation, group in df.sort_values(["variation", "dt"]).groupby("variation"):
        group = group.copy()

        group[regular_cols] = group[regular_cols].cumsum()

        original_group = df[df["variation"] == variation].sort_values("dt").copy()

        for var_col, cfg in var_config.items():
            count_col = cfg["count_col"]
            revenue_col = cfg["revenue_col"]

            cumulative_vars = []

            prev_count = 0
            prev_revenue = 0
            prev_var = 0

            for _, row in original_group.iterrows():
                count = row[count_col]
                revenue = row[revenue_col]
                current_var = row[var_col]

                if pd.isna(current_var):
                    current_var = 0

                total_count = prev_count + count
                total_revenue = prev_revenue + revenue

                if total_count <= 1:
                    cumulative_var = 0
                elif prev_count == 0:
                    cumulative_var = current_var
                else:
                    prev_mean = prev_revenue / prev_count if prev_count else 0
                    current_mean = revenue / count if count else 0
                    total_mean = total_revenue / total_count if total_count else 0

                    cumulative_var = (
                        (prev_count - 1) * prev_var
                        + (count - 1) * current_var
                        + prev_count * (prev_mean - total_mean) ** 2
                        + count * (current_mean - total_mean) ** 2
                    ) / (total_count - 1)

                cumulative_vars.append(cumulative_var)

                prev_count = total_count
                prev_revenue = total_revenue
                prev_var = cumulative_var

            group[var_col] = cumulative_vars

        result_parts.append(group)

    result = pd.concat(result_parts, ignore_index=True)
    result = result.sort_values(["variation", "dt"]).reset_index(drop=True)

    result["dt"] = result["dt"].dt.strftime("%Y-%m-%d")

    return result


def normalize_metric_config(metric_items: list[dict]) -> dict:
    config = {}

    for item in metric_items:
        config.update(item)

    return config


def calc_stats(mean_0, mean_1, var_0, var_1, len_0, len_1, alpha=None, required_power=None, pvalue=None, calc_mean=False):
        if math.isnan(mean_0) or math.isnan(mean_1) or math.isnan(len_0) or math.isnan(len_1):
            return {"pvalue": 1,  
                "cohen_d": 0, 
                "ci": [np.array([0, 0])],
                }
        if alpha is None:
            alpha = 0.05
        if required_power is None:
            required_power = 0.8

        std = np.sqrt(var_0 / len_0 + var_1 / len_1)
        mean_abs = abs(mean_1 - mean_0)
        mean = mean_1 - mean_0
        sd = np.sqrt((var_0 * len_0 + var_1 * len_1) / (len_0 + len_1 - 2))

        if pvalue is None:
            pvalue = stats.norm.cdf(x=0, loc=mean_abs, scale=std) * 2
        elif not calc_mean:
            std_corrected = np.abs(special.nrdtrisd(0, pvalue / 2, mean_abs))
            sd *= 1 + (std_corrected - std) / std
            std = std_corrected
        else:
            mean_abs = special.nrdtrimn(pvalue / 2, std, 0)
            mean = mean_abs
            if mean_0 > mean_1:
                mean *= -1

        cohen_d = mean_abs / sd

        return {"pvalue": pvalue, 
                "cohen_d": cohen_d, 
                "ci": [np.array([stats.norm.ppf(alpha / 2, mean, std), 
                    stats.norm.ppf(1 - alpha / 2, mean, std)])],
                }


def safe_divide(numerator, denominator):
    if pd.isna(denominator) or denominator == 0:
        return np.nan

    return numerator / denominator


def calc_metrics_stats_by_variation_pairs(
    cumulative_df: pd.DataFrame,
    metrics_yaml_path: str,
    control_variation=1,
) -> pd.DataFrame:
    df = cumulative_df.copy()
    df["dt"] = pd.to_datetime(df["dt"])

    with open(metrics_yaml_path, "r") as file:
        metrics_config = yaml.safe_load(file)

    all_variations = sorted(df["variation"].unique())

    test_variations = [
        variation
        for variation in all_variations
        if variation != control_variation
    ]

    result_rows = []

    for metric_name, metric_items in metrics_config.items():
        metric_config = normalize_metric_config(metric_items)

        numerator_col = metric_config.get("numerator")
        denominator_col = metric_config.get("denominator")
        variance_col = metric_config.get("variance")
        distribution = metric_config.get("distribution")
        is_percentage = metric_config.get("percentage", False)

        required_cols = {"dt", "variation", numerator_col, denominator_col}

        if variance_col:
            required_cols.add(variance_col)

        if not required_cols.issubset(df.columns):
            continue

        for current_dt in sorted(df["dt"].unique()):
            date_df = df[df["dt"] == current_dt]

            control_rows = date_df[date_df["variation"] == control_variation]

            if control_rows.empty:
                continue

            control_row = control_rows.iloc[0]

            numerator_0 = control_row[numerator_col]
            denominator_0 = control_row[denominator_col]

            mean_0 = safe_divide(numerator_0, denominator_0)
            len_0 = denominator_0

            if pd.isna(mean_0) or pd.isna(len_0) or len_0 <= 0:
                continue

            if variance_col:
                var_0 = control_row[variance_col]
                if pd.isna(var_0):
                    var_0 = 0
            elif distribution == "bernoulli":
                var_0 = mean_0 * (1 - mean_0)
            else:
                continue

            for test_variation in test_variations:
                test_rows = date_df[date_df["variation"] == test_variation]

                if test_rows.empty:
                    continue

                test_row = test_rows.iloc[0]

                numerator_1 = test_row[numerator_col]
                denominator_1 = test_row[denominator_col]

                mean_1 = safe_divide(numerator_1, denominator_1)
                len_1 = denominator_1

                if pd.isna(mean_1) or pd.isna(len_1) or len_1 <= 0:
                    continue

                if variance_col:
                    var_1 = test_row[variance_col]
                    if pd.isna(var_1):
                        var_1 = 0
                elif distribution == "bernoulli":
                    var_1 = mean_1 * (1 - mean_1)
                else:
                    continue

                stats = calc_stats(
                    mean_0=mean_0,
                    mean_1=mean_1,
                    var_0=var_0,
                    var_1=var_1,
                    len_0=len_0,
                    len_1=len_1,
                )

                mean_diff = mean_1 - mean_0
                ci = stats["ci"]

                koeff = 1
                if is_percentage:
                    koeff = 100
                    # mean_0 *= 100
                    # mean_1 *= 100
                    # mean_diff *= 100
                    # ci = ci * 100

                result_rows.append({
                    "dt": current_dt,
                    "metric": metric_name,
                    "variation_pair": f"{control_variation} vs {test_variation}",
                    "control_variation": control_variation,
                    "test_variation": test_variation,

                    "mean_0": mean_0 * koeff,
                    "mean_1": mean_1 * koeff,
                    "mean_diff": mean_diff * koeff,
                    "lift": mean_diff / mean_0 * 100 if mean_0 != 0 else 0,

                    "ci_low": ci[0][0] * koeff,
                    "ci_high": ci[0][1] * koeff,
                    "pvalue": stats["pvalue"],

                    "numerator": numerator_col,
                    "denominator": denominator_col,
                    "variance": variance_col,
                    "distribution": distribution,
                    "percentage": is_percentage,
                })

    result = pd.DataFrame(result_rows)

    if not result.empty:
        result["dt"] = result["dt"].dt.strftime("%Y-%m-%d")

    return result


def calculate_exp_info(exp_id) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame], dict[str, pd.DataFrame], str]:
    exp_info: dict = get_experiment(exp_id)
    # if exp_id has no field exp_info["clients_list"] or its and empty array or empty string then reassign it with ["UGT_IOS", "UGT_ANDROID", "UG_WEB"]
    if not exp_info.get("clients_list"):
        exp_info["clients_list"] = ["UGT_IOS", "UGT_ANDROID", "UG_WEB"]
    if exp_info.get("experiment_event_start") in [None, "", "xxx"]:
        raise ValueError(f"Experiment {exp_id} has invalid experiment_event_start: {exp_info.get('experiment_event_start')}")
    df_tot = {}
    stats_df_tot = {}
    df_cum_agg_tot = {}
    for client in exp_info["clients_list"]:
        for segment_name, segment in exp_info["segments"].items():
            logger.info("Calculating experiment info for exp_id=%s, client=%s", exp_id, client)
            # log start calulation with exp_info for debugging
            logger.info("Calculating experiment info for exp_id=%s with exp_info:\n%s", exp_id, exp_info)
            logger.info("loading users...")
            exp_users_table: str = create_experiment_users_table(exp_info, client, segment)
            logger.info("loading subscriptions...")
            subscription_table: str = create_experiments_subscription_table(exp_info, client, segment)
            logger.info("exp_users_table:\n%s, subscription_table: %s", exp_users_table, subscription_table)
            logger.info("loading exp monetization metrics...")
            df: pd.DataFrame = get_monetization_metrics(exp_info, exp_users_table, subscription_table)
            df_tot[(client, segment_name)] = df

            logger.info("deleteing temp tables...")
            drop_table(exp_users_table)
            drop_table(subscription_table)
            logger.info("calculating cumulative aggregates...")
            df_cum_agg = calc_cumulative_aggregates(df)
            logger.info("calculating cumulative statistics...")
            stats_df = calc_metrics_stats_by_variation_pairs(
                cumulative_df=df_cum_agg,
                metrics_yaml_path="metrics.yaml",
                control_variation=1,
            )
            df_cum_agg = df_cum_agg.melt(id_vars=["dt", "variation"], var_name="metric", value_name="value")
            df_cum_agg["exp_id"] = exp_id
            df_cum_agg["client"] = client
            df_cum_agg["segment"] = segment_name
            df_cum_agg_tot[(client, segment_name)] = df_cum_agg
            stats_df["exp_id"] = exp_id
            stats_df["client"] = client
            stats_df["segment"] = segment_name
            stats_df_tot[(client, segment_name)] = stats_df
            is_exists = execute_sql("exists sandbox.ug_monetization_sloperator_ug_exp_results")
            if int(is_exists.iloc[0].values[0]) == 0:
                create_exp_results_table(stats_df)
            else:
                drop_exp_partitions(exp_id, client_name=client, segment=segment_name, table_name="ug_exp_results")
                update_exp_results_table(stats_df, table="ug_exp_results")

            is_exists = execute_sql("exists sandbox.ug_monetization_sloperator_ug_exp_stats")
            if int(is_exists.iloc[0].values[0]) == 0:
                create_exp_stats_table(df_cum_agg)
            else:
                drop_exp_partitions(exp_id, client_name=client, segment=segment_name, table_name="ug_exp_stats")
                update_exp_results_table(df_cum_agg, table="ug_exp_stats")
    return df_tot, df_cum_agg_tot, stats_df_tot, f"exp_users_table={exp_users_table}, subscription_table={subscription_table}"


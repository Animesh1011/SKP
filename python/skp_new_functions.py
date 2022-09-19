from typing import (
    Iterator
)
import json
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from common.functional import compose_functions
from pyspark.sql import DataFrame, Column
import jobs.reqresp.functions as rrf


def skp_rebalance(
    df: DataFrame,
    *args: str
) -> DataFrame:
    return compose_functions(
        lambda x: x
    )(df)


def skp_profile(
    df: DataFrame,
    *args: str
) -> DataFrame:
    return compose_functions(
        lambda x: rrf.explode_col(x, 'details'),
        lambda x: rrf.jsonify(x, ['eligiblePortfolioManagementTriggers'])
    )(df)
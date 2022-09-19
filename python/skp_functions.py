from typing import (
    Optional,
    List,
    Dict,
    Set
)
import json
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType
from common.functional import compose_functions
from common.df_utils import rename_cols
import jobs.reqresp.functions as rrf
import jobs.reqresp.reqresp_utils as rru

def dly_details(
    df: DataFrame,
) -> DataFrame:
    return compose_functions(
        lambda x: rrf.explode_col(x,'details'),
        lambda x: rrf.jsonify(x,['eligiblePortfolioManagementTriggers'])   
    )(df)

def dly_rebalance(
    df: DataFrame,
) -> DataFrame:
    return compose_functions(
        lambda x: rrf.match_http_method(x,'POST'),
        lambda x: rrf.match_http_method(x,'GET')
    )(df)

from typing import (
    Iterator
)
import json
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from common.functional import compose_functions
from pyspark.sql import DataFrame, Column
import jobs.reqresp.functions as rrf


def profileID_coalesce(df: DataFrame) -> DataFrame:
    col = "profileId"
    return df.withColumn(col, F.coalesce("profileId", col)) if "profileId" in df.columns else df

def explode_col_iro(
    df: DataFrame,
    *args: str
) -> DataFrame:
    """
    Combination of common function explode_col and local function clientUid_coalesce
    """
    return compose_functions(
        profileId_coalesce,
        lambda x: rrf.explode_col(x, *args)
    )(df)

def secondary_process_iro(
    df: DataFrame,
    *args: str
) -> DataFrame:
    """
    Combination of common function secondary_process and local function clientUid_coalesce
    """
    return compose_functions(
        profileId_coalesce,
        lambda x: rrf.secondary_process(x, *args),
    )(df)
def rebalancerequest_req(
    df: DataFrame,
    *args: str
) -> DataFrame:
    str_cols: Iterator[Column] = (F.col(x).cast('string') for x in args)
    return compose_functions(
        lambda x: x.withColumn('rebalancerequest_req_nncolumn', F.coalesce(*str_cols))
    )(df)


def goals_conf_req(
    df: DataFrame,
    *args: str
) -> DataFrame:
    str_cols: Iterator[Column] = (F.col(x).cast('string') for x in args)
    return compose_functions(
        lambda x: rrf.explode_col(x, 'goals'),
        lambda x: rrf.extract_nested(x, 'configuration', False),
        lambda x: rrf.extract_nested(x, 'withdrawalStatement', True),
        lambda x: rrf.extract_nested(x, 'withdrawalStatement_amount', True),
        lambda x: x.withColumn('goalId_nncolumn', F.coalesce(*str_cols))
    )(df)

def goals_resp(
    df: DataFrame,
    *args: str
) -> DataFrame:
    str_cols: Iterator[Column] = (F.col(x).cast('string') for x in args)
    return compose_functions(
        lambda x: rrf.explode_col(x, 'goals'),
        lambda x: x.withColumn('goalId', F.coalesce(*str_cols))
    )(df)

def accounts_resp(
    df: DataFrame,
    *args: str
) -> DataFrame:
    str_cols: Iterator[Column] = (F.col(x).cast('string') for x in args)
    return compose_functions(
        lambda x: rrf.explode_col(x, 'accounts'),
        lambda x: rrf.extract_nested(x, 'cashPosition', True),
        lambda x: rrf.extract_nested(x, 'CashInPosition', True),
        lambda x: x.withColumn('accountId', F.coalesce(*str_cols))
    )(df)

def positions_resp(
    df: DataFrame,
    *args: str
) -> DataFrame:
    str_cols: Iterator[Column] = (F.col(x).cast('string') for x in args)
    return compose_functions(
        lambda x: rrf.explode_col(x, 'positions'),
        lambda x: rrf.extract_nested(x, 'identification','washSaleTransactions', True),
        lambda x: x.withColumn('nncolumn_washSaleAccountPositionId', F.coalesce(*str_cols))
    )(df)

def configuration_resp(
    df: DataFrame,
    *args: str
) -> DataFrame:
    str_cols: Iterator[Column] = (F.col(x).cast('string') for x in args)
    return compose_functions(
        lambda x: rrf.explode_col(x, 'positions'),
        lambda x: rrf.extract_nested(x, 'identification','washSaleTransactions', True),
        lambda x: x.withColumn('rebalanceType', F.coalesce(*str_cols))
    )(df)

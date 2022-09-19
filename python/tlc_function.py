from typing import (
    Iterator
)
import json
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from common.functional import compose_functions
from pyspark.sql import DataFrame, Column
import jobs.reqresp.functions as rrf


def clientUid_coalesce(df: DataFrame) -> DataFrame:
    """
    Rename clientUid as profileId
    """
    col = "profileId"
    return df.withColumn(col, F.coalesce("clientUid", col)) if "clientUid" in df.columns else df

def extract_opportunityDate(df: DataFrame) -> DataFrame:
    """
    Transform date struct into formatted date column
    """
    return df.withColumn(
        'opportunityDate',
        F.to_date(
            F.concat_ws(
                '-',
                df.opportunityDate.year, 
                df.opportunityDate.month,
                df.opportunityDate.day
            )
        )
    )

def explode_col_tlc(
    df: DataFrame,
    *args: str
) -> DataFrame:
    """
    Combination of common function explode_col and local function clientUid_coalesce
    """
    return compose_functions(
        clientUid_coalesce,
        lambda x: rrf.explode_col(x, *args)
    )(df)

def secondary_process_tlc(
    df: DataFrame,
    *args: str
) -> DataFrame:
    """
    Combination of common function secondary_process and local function clientUid_coalesce
    """
    return compose_functions(
        clientUid_coalesce,
        lambda x: rrf.secondary_process(x, *args),
    )(df)

def harvest_opp_sell_trans(
    df: DataFrame,
    # *args: str
) -> DataFrame:
    return compose_functions(
        clientUid_coalesce,
        lambda x: rrf.explode_col(x, 'harvestOpportunities'),
        lambda x: extract_opportunityDate(x),
        lambda x: rrf.jsonify(x, ['opportunityCodes']),
        lambda x: rrf.extract_nested(x, 'sellTransaction', True),
        lambda x: x.withColumn("harvestOpportunityId", F.monotonically_increasing_id())
    )(df)

def sell_alt_share_classes(
    df: DataFrame,
    *args: str
) -> DataFrame:
    str_cols: Iterator[Column] = (F.col(x).cast('string') for x in args)
    return compose_functions(
        lambda x: rrf.secondary_process(x, 'sellTransaction_alternateShareClasses', 'harvestOpportunityId', 'sellTransaction_taxLots', 'buyTransactions'),
        lambda x: x.withColumn('nncolumn_alternateShareClasses', F.coalesce(*str_cols))
    )(df)

def buy_alt_share_classes(
    df: DataFrame,
    *args: str
) -> DataFrame:
    str_cols: Iterator[Column] = (F.col(x).cast('string') for x in args)
    return compose_functions(
        lambda x: rrf.secondary_process(x, 'alternateShareClasses', 'harvestOpportunityId'),
        lambda x: x.withColumn('nncolumn_alternateShareClasses', F.coalesce(*str_cols))
    )(df)

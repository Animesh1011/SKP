from functools import reduce
from typing import (
    List,
    Sequence,
    Iterable,
    Optional,
    Dict
)
from pyspark.sql import DataFrame, Column
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType
from common.df_utils import multi_col
from jobs.reqresp import BASE_COLUMNS
from common.functional import compose_functions

def millis_to_timestamp(column: str) -> Column:
    return (F.col(column)/1000).cast(TimestampType())

def is_cached(df: DataFrame) -> bool:
    return df.storageLevel.useMemory

def explode_col(df: DataFrame, column: str) -> DataFrame:
    return df.withColumn(column, F.explode_outer(F.col(column))).select(f"{column}.*", "*")

def prune_df(df: DataFrame, *args: str) -> DataFrame:
    cols: List[str] = [*BASE_COLUMNS, *args]
    return df.select(*cols).dropDuplicates()
def secondary_process(df: DataFrame, exp_col: str, *args: str) -> DataFrame:
    return explode_col(prune_df(df, exp_col, *args), exp_col)

def extract_nested(df: DataFrame, column: str, pref: bool = False) -> DataFrame:
    def _prefixed_cols(schema, column):
        return [f"{column}.{field} as {column}_{field}" for field in schema[column].dataType.fieldNames()]
    if pref:
        return df.selectExpr("*", *_prefixed_cols(df.schema, column)).drop(column)
    else:
        return df.select("*", f"{column}.*").drop(column)

def multi_nest_extract(df: DataFrame, columns: Sequence[str]) -> DataFrame:
    return reduce(lambda x, y: extract_nested(x, y, pref=True), columns, df)

@multi_col
def jsonify(df: DataFrame, column: Iterable[str]) -> DataFrame:
    return df.withColumn(column, F.to_json(F.col(column)))

def output_select(df: DataFrame, columns: Sequence[str], na_col: Optional[str] = None) -> DataFrame:
    def null_check(df: DataFrame) -> DataFrame:
        return df.filter(F.col(na_col).isNotNull()) if na_col else df
    return null_check(df).select(*columns).dropDuplicates()

def match_column(df: DataFrame, column: str, string: str) -> DataFrame:
    return df.filter(F.col(column).rlike(string))

def match_http_method(df: DataFrame, httpMethod_value: str) -> DataFrame:
    return df.filter(F.col('httpMethod') == httpMethod_value)

def endpoint_match(
    df: DataFrame,
    endpointType: str,
    column: str,
    string: str
) -> DataFrame:
    return compose_functions(
        lambda x: match_http_method(x, endpointType),
        lambda x: match_column(x, column, string)
    )(df)

def posexplode_col(df: DataFrame, column: str, position_name: str) -> DataFrame:
    return df.select(F.posexplode_outer(F.col(column)), '*')\
             .select("col.*", "*")\
             .withColumnRenamed("pos", position_name)\
             .drop('col')

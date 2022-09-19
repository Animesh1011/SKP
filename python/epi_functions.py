from typing import (
    Iterator
)
import json
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from common.functional import compose_functions
from pyspark.sql import DataFrame, Column
import jobs.reqresp.functions as rrf

#Transformation functions
def body_coalesce(df: DataFrame) -> DataFrame:
    col = "profileId"
    return df.withColumn(col, F.coalesce("body_profileId", col)) if "body_profileId" in df.columns else df

def profile_id_body_coalesce(
    df: DataFrame,
    *args: str
) -> DataFrame:
    str_cols: Iterator[Column] = (F.col(x).cast('string') for x in args)
    return compose_functions(
        lambda x: x,
        body_coalesce,
        lambda x: x.withColumn('profile_portfolio_nncol', F.coalesce(*str_cols))
    )(df)

def extract_investor_goals(
    df: DataFrame,
    *args: str
) -> DataFrame:
    str_cols: Iterator[Column] = (F.col(x).cast('string') for x in args)
    return compose_functions(
       
 lambda x: rrf.secondary_process(x, 
            'goals', 
            'accounts', 
            'shortTermTaxRate',
            'nextRebalanceDate',
            'taxBracket',
            'version',
            'lastUpdatedDateTime',
            'lastSavedDateTime',
            'lastUpdatedUserId'
        ),
        lambda x: rrf.extract_nested(x,'spendingFund',pref=True),
        lambda x: rrf.extract_nested(x,'tilts'),
        lambda x: rrf.multi_nest_extract(x,('bond', 'fund', 'equity')),
        lambda x: x.withColumn('investor_goals_nncol', F.coalesce(*str_cols))
    )(df)

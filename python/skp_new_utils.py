from typing import (
    Dict,
    List,
    Optional,
    FrozenSet,
    Union,
    Callable
)
from functools import reduce
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from jobs.reqresp.pipeline import ProcessSet, ProcessPipeline
from jobs.reqresp.router import Router
from common.serde_utils import DecimalDecoder
from common.arg_parsers import parse_file
import jobs.reqresp.reqresp_utils as rru
import jobs.reqresp.functions as rrf
import jobs.reqresp.skp.skp_configs as skpc
import jobs.reqresp.skp.skp_mapping as skpm

PsetCallable = Callable[[datetime, str, str], ProcessSet]

Endpoint = Dict[str, Union[str, Callable, List[Callable]]]
Dataset = Dict[str, Union[str, List[Endpoint]]]

def resp_post_skp_rebalance_process_set(date: datetime, consumer: str) -> ProcessSet:
    pset: ProcessSet = ProcessSet()
    resp_post_skp_rebalance_router: Router = Router(skpm.RESP_SKP_REBALANCE_FUNC_MAP, date)
    pset.add(
        resp_post_skp_rebalance_router,
        f'{consumer}_resp_post_skp_rebalance_process_set',
        skpc.SKP_POST_REBALANCE_COLUMNS,
        nncolumn='lastUpdated',
        fargs=resp_post_skp_rebalance_router.args
    )
    return pset

def resp_get_skp_rebalance_process_set(date: datetime, consumer: str) -> ProcessSet:
    pset: ProcessSet = ProcessSet()
    resp_get_skp_rebalance_router: Router = Router(skpm.RESP_SKP_REBALANCE_FUNC_MAP, date)
    pset.add(
        resp_get_skp_rebalance_router,
        f'{consumer}_resp_get_skp_rebalance_process_set',
        skpc.SKP_GET_REBALANCE_COLUMNS,
        nncolumn='lastUpdated',
        fargs=resp_get_skp_rebalance_router.args
    )
    return pset

def resp_get_skp_profile_process_set(date: datetime, consumer: str) -> ProcessSet:
    pset: ProcessSet = ProcessSet()
    resp_get_skp_profile_router: Router = Router(skpm.RESP_GET_SKP_PROFILE_FUNC_MAP, date)
    pset.add(
        resp_get_skp_profile_router,
        f'{consumer}_resp_get_skp_profile_process_set',
        skpc.SKP_PROFILE_COLUMNS,
        nncolumn='skipClient',
        fargs=resp_get_skp_profile_router.args
    )
    return pset

#Dictionary to input arguments
decoder_fields: Dict[str, List[str]] = {
    'boolfields': skpc.BOOLEAN_FIELDS,
    'dtfields': skpc.DATETIME_FIELDS
}

def process(
    dataset: Dataset, 
    *, 
    spark: SparkSession, 
    db: str, 
    bucket: str, 
    date: str,
    write_options: Optional[str],
    cache_options: Optional[bool],
    discovery_layer = True
) -> None:
    option_write: Optional[FrozenSet[str]] = parse_file(write_options)
    consumer: str = dataset['Consumer']
    base_prefix: str = f'{skpc.BASE_PREFIX}/{consumer}'
    date_dt: datetime = datetime.strptime(date, "%Y-%m-%d")
    #replaced with get_staging_data
    preprocessed_df: Optional[DataFrame] = rru.get_staging_data(spark, bucket, f"{dataset['StagingPrefix']}/{date}", use_discovery=discovery_layer)
    if preprocessed_df:
        rru.create_base_table(
            preprocessed_df,
            spark,
            db,
            bucket,
            base_prefix,
            f'{consumer}_base',
            date,
            extend_base_columns = True #httpMethod & other columns -> base table
        )
        for endpoint in dataset['Endpoints']:
            #match the httpMethod type first and then with the uri column
            endpoint_match_df: DataFrame = rrf.endpoint_match(preprocessed_df, endpoint['httpMethod'], 'uri', endpoint['Pattern'])
            if not endpoint_match_df.rdd.isEmpty():
                prepared_df: DataFrame = rru.body_preprocess(
                    spark,
                    endpoint_match_df,
                    endpoint['Schema'](date_dt),
                    decoder=DecimalDecoder,
                    **decoder_fields
                )
                checkpoint_df: DataFrame = prepared_df.repartition(1000, 'correlationId').checkpoint(eager=True)
                pipeline: ProcessPipeline = ProcessPipeline(
                    *[process_set(date_dt, consumer) for process_set in endpoint['ProcessSets']],
                    spark=spark,
                    db=db,
                    bucket=bucket,
                    base_prefix=base_prefix,
                    date=date,
                    coalesce=20,
                    option_write=option_write,
                    option_cache=cache_options
                )
                print(f"Running {str(pipeline)} for {date}")
                pipeline.run(checkpoint_df)
            else:
                print(f'''No data found for endpoint with matching uri {endpoint['Pattern']} for date {date}''')
    else:
        print(f"No data found for {dataset['StagingPrefix']} for {date}")
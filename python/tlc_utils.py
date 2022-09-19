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
import jobs.reqresp.tlc.tlc_configs as tlcc
import jobs.reqresp.tlc.tlc_mappings as tlcm

PsetCallable = Callable[[datetime, str, str], ProcessSet]

Endpoint = Dict[str, Union[str, Callable, List[Callable]]]
Dataset = Dict[str, Union[str, List[Endpoint]]]

def resp_tlc_process_set(date: datetime, consumer: str) -> ProcessSet:
    pset: ProcessSet = ProcessSet()
    resp_harvest_opp_sell_trans_router: Router = Router(tlcm.RESP_HARVEST_OPP_SELL_TRANS_FUNC_MAP, date)
    pset.add(
        resp_harvest_opp_sell_trans_router,
        f'{consumer}_resp_harvest_opp_sell_trans',
        tlcc.HARVEST_OPP_SELL_TRANS,
        nncolumn='sellTransaction_accountPositionId',
        fargs=resp_harvest_opp_sell_trans_router.args
    )
    resp_sell_trans_alt_share_classes_router: Router = Router(tlcm.RESP_SELL_TRANS_ALT_SHARE_CLASSES_FUNC_MAP, date)
    pset.add(
        resp_sell_trans_alt_share_classes_router,
        f'{consumer}_resp_sell_trans_alt_share_classes',
        tlcc.SELL_TRANS_ALT_SHARE_CLASSES,
        nncolumn='nncolumn_alternateShareClasses',
        fargs=resp_sell_trans_alt_share_classes_router.args
    )
    resp_tax_lots_router: Router = Router(tlcm.RESP_TAX_LOTS_FUNC_MAP, date)
    pset.add(
        resp_tax_lots_router,
        f'{consumer}_resp_tax_lots',
        tlcc.TAX_LOTS,
        nncolumn='id',
        fargs=resp_tax_lots_router.args
    )    
    resp_buy_trans_router: Router = Router(tlcm.RESP_BUY_TRANS_FUNC_MAP, date)
    pset.add(
        resp_buy_trans_router,
        f'{consumer}_resp_buy_trans',
        tlcc.BUY_TRANS,
        nncolumn='accountPositionId',
        fargs=resp_buy_trans_router.args
    )
    resp_buy_trans_alt_share_classes_router: Router = Router(tlcm.RESP_BUY_TRANS_ALT_SHARE_CLASSES_FUNC_MAP, date)
    pset.add(
        resp_buy_trans_alt_share_classes_router,
        f'{consumer}_resp_buy_trans_alt_share_classes',
        tlcc.BUY_TRANS_ALT_SHARE_CLASSES,
        nncolumn='nncolumn_alternateShareClasses',
        fargs=resp_buy_trans_alt_share_classes_router.args
    )
    return pset

def req_tlc_process_set(date: datetime, consumer: str) -> ProcessSet:
    pset: ProcessSet = ProcessSet()
    req_financial_securities_router: Router = Router(tlcm.REQ_FINANCIAL_SECURITIES_FUNC_MAP, date)
    pset.add(
        req_financial_securities_router,
        f'{consumer}_req_financial_securities',
        tlcc.FINANCIAL_SECURITIES,
        nncolumn='accountPositionId',
        fargs=req_financial_securities_router.args
    )
    req_unrealized_tax_lots_router: Router = Router(tlcm.REQ_UNREALIZED_TAX_LOTS_FUNC_MAP, date)
    pset.add(
        req_unrealized_tax_lots_router,
        f'{consumer}_req_unrealized_tax_lots',
        tlcc.UNREALIZED_TAX_LOTS,
        nncolumn='id',
        fargs=req_unrealized_tax_lots_router.args
    )
    req_historical_trans_router: Router = Router(tlcm.REQ_HISTORICAL_TRANS_FUNC_MAP, date)
    pset.add(
        req_historical_trans_router,
        f'{consumer}_req_historical_trans',
        tlcc.HISTORICAL_TRANS,
        nncolumn='accountPositionId',
        fargs=req_historical_trans_router.args
    )
    req_realized_tax_lots_router: Router = Router(tlcm.REQ_REALIZED_TAX_LOTS_FUNC_MAP, date)
    pset.add(
        req_realized_tax_lots_router,
        f'{consumer}_req_realized_tax_lots',
        tlcc.REALIZED_TAX_LOTS,
        nncolumn='id',
        fargs=req_realized_tax_lots_router.args
    )
    return pset

#Dictionary to input arguments
decoder_fields: Dict[str, List[str]] = {
    'decfields': tlcc.decimal_fields,
    'intfields': tlcc.integer_fields,
    'boolfields': tlcc.boolean_fields,
    'dtfields': tlcc.datetime_fields
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
    base_prefix: str = f'{tlcc.BASE_PREFIX}/{consumer}'
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
            extend_base_columns = False #httpMethod & other columns -> base table
        )
        for endpoint in dataset['Endpoints']:
            #match the httpMethod type first and then with the uri column
            match_df: DataFrame = rrf.match_column(preprocessed_df, 'uri', endpoint['Pattern'])
            if not match_df.rdd.isEmpty():
                prepared_df: DataFrame = rru.body_preprocess(
                    spark,
                    match_df,
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

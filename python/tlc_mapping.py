from typing import List
from datetime import datetime
from common.schema_utils import create_schema
from jobs.reqresp.router import RouterMap, SchemaMap
import jobs.reqresp.functions as rrf
import jobs.reqresp.tlc.tlc_schemas as tlcs
import jobs.reqresp.tlc.tlc_functions as tlcf
import jobs.reqresp.tlc.tlc_configs as tlcc

############################## REQUEST ##############################

REQ_TLC_SCHEMA_MAP: SchemaMap = {
    (datetime(2020, 7, 1), datetime(9999, 12, 31)): create_schema(
        tlcs.REQ_TLC_VER1,
        inf_dt=True,
        scl=2
    )
}

REQ_FINANCIAL_SECURITIES_FUNC_MAP: RouterMap = {
    (datetime(2020, 7, 1), datetime(9999, 12, 31)) : {
        "function": tlcf.explode_col_tlc,
        "args": ('financialSecurities',)
    }
}

REQ_UNREALIZED_TAX_LOTS_FUNC_MAP: RouterMap = {
    (datetime(2020, 7, 1), datetime(9999, 12, 31)) : {
        "function": tlcf.secondary_process_tlc,
        "args": (
            'unrealizedTaxLots',
            'historicalTransactions',
            'realizedTaxLots',
            'accountPositionId',
        )
    }
}

REQ_HISTORICAL_TRANS_FUNC_MAP: RouterMap = {
    (datetime(2020, 7, 1), datetime(9999, 12, 31)) : {
        "function": tlcf.secondary_process_tlc,
        "args": ('historicalTransactions', 'realizedTaxLots',)
    }
}

REQ_REALIZED_TAX_LOTS_FUNC_MAP: RouterMap = {
    (datetime(2020, 7, 1), datetime(9999, 12, 31)) : {
        "function": tlcf.secondary_process_tlc,
        "args": ('realizedTaxLots',)
    }
}

############################## RESPONSE ##############################

RESP_TLC_SCHEMA_MAP: SchemaMap = {
    (datetime(2020, 7, 1), datetime(9999, 12, 31)): create_schema(
        tlcs.RESP_TLC_VER1,
        inf_dt=True,
        scl=2
    )
}

RESP_HARVEST_OPP_SELL_TRANS_FUNC_MAP: RouterMap = {
    (datetime(2020, 7, 1), datetime(9999, 12, 31)) : {
        "function": tlcf.harvest_opp_sell_trans
    }
}

RESP_SELL_TRANS_ALT_SHARE_CLASSES_FUNC_MAP: RouterMap = {
    (datetime(2020, 7, 1), datetime(9999, 12, 31)) : {
        "function": tlcf.sell_alt_share_classes,
        "args": tuple(x for x in tlcc.SELL_TRANS_ALT_SHARE_CLASSES if x not in tlcc.BASE_COLUMNS)
    }
}

RESP_TAX_LOTS_FUNC_MAP: RouterMap = {
    (datetime(2020, 7, 1), datetime(9999, 12, 31)) : {
        "function": tlcf.secondary_process_tlc,
        "args": ('sellTransaction_taxLots', 'harvestOpportunityId', 'buyTransactions')
    }
}

RESP_BUY_TRANS_FUNC_MAP: RouterMap = {
    (datetime(2020, 7, 1), datetime(9999, 12, 31)) : {
        "function": tlcf.secondary_process_tlc,
        "args": ('buyTransactions', "harvestOpportunityId")
    }
}

RESP_BUY_TRANS_ALT_SHARE_CLASSES_FUNC_MAP: RouterMap = {
    (datetime(2020, 7, 1), datetime(9999, 12, 31)) : {
        "function": tlcf.buy_alt_share_classes,
        "args": (
            tuple(x for x in tlcc.BUY_TRANS_ALT_SHARE_CLASSES if x not in tlcc.BASE_COLUMNS)
        )
    }
}

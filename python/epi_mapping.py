""" 
Cannot Backfill for the dates prior to 2021-09-01
"""
from datetime import datetime
from common.schema_utils import create_schema
from jobs.reqresp.router import RouterMap, SchemaMap
import jobs.reqresp.functions as rrf
import jobs.reqresp.epi.epi_schemas as epis
import jobs.reqresp.epi.epi_functions as epif
import jobs.reqresp.epi.epi_configs as epic

RESP_POST_SCHEMA_MAP: SchemaMap = {
    (datetime(2021, 9, 1), datetime(9999, 12, 30)): create_schema(
        epis.RESP_CREATE_PROFILE_PORTFOLIO_VER1,
        inf_dt=True,
        scl=15
    )
}


RESP_GET_SCHEMA_MAP: SchemaMap = {
    (datetime(2021, 9, 1), datetime(9999, 12, 30)): create_schema(
        epis.RESP_RETRIEVE_PROFILE_PORTFOLIO_VER1,
        inf_dt=True,
        scl=15
    )
}

RESP_PUT_SCHEMA_MAP: SchemaMap = {
    (datetime(2021, 9, 1), datetime(9999, 12, 30)): create_schema(
        epis.RESP_UPDATE_PROFILE_PORTFOLIO_VER1,
        inf_dt=True,
        scl=15
    )
}


REQ_POST_SCHEMA_MAP: SchemaMap = {
    (datetime(2021, 9, 1), datetime(9999, 12, 30)): create_schema(
        epis.REQ_CREATE_PROFILE_PORTFOLIO_VER1,
        inf_dt=True,
        scl=15
    )
}


REQ_PUT_SCHEMA_MAP: SchemaMap = {
    (datetime(2021, 9, 1), datetime(9999, 12, 30)): create_schema(
        epis.REQ_UPDATE_PROFILE_PORTFOLIO_VER1,
        inf_dt=True,
        scl=15
    )
}


RESP_CREATE_PORTFOLIO_FUNC_MAP: RouterMap = {
    (datetime(2021, 9, 1), datetime(9999, 12, 30)): {
        "function": epif.profile_id_body_coalesce,
        "args": tuple(x for x in epic.PROFILE_PORTFOLIO_COLUMNS if x not in epic.BASE_COLUMNS)
    }
}
RESP_RETRIEVE_PORTFOLIO_FUNC_MAP: RouterMap = {
    (datetime(2021, 9, 1), datetime(9999, 12, 30)): {
        "function": epif.profile_id_body_coalesce,
        "args": tuple(x for x in epic.PROFILE_PORTFOLIO_COLUMNS if x not in epic.BASE_COLUMNS)
    }
}

RESP_UPDATE_PORTFOLIO_FUNC_MAP: RouterMap = {
    (datetime(2021, 9, 1), datetime(9999, 12, 30)): {
        "function": epif.profile_id_body_coalesce,
        "args": tuple(x for x in epic.PROFILE_PORTFOLIO_COLUMNS if x not in epic.BASE_COLUMNS)
    }
}

REQ_CREATE_PORTFOLIO_FUNC_MAP: RouterMap = {
    (datetime(2021, 9, 1), datetime(9999, 12, 30)): {
        "function": epif.profile_id_body_coalesce,
        "args": tuple(x for x in epic.PROFILE_PORTFOLIO_COLUMNS if x not in epic.BASE_COLUMNS)
    }
}

REQ_UPDATE_PORTFOLIO_FUNC_MAP: RouterMap = {
    (datetime(2021, 9, 1), datetime(9999, 12, 30)): {
        "function": epif.profile_id_body_coalesce,
        "args": tuple(x for x in epic.PROFILE_PORTFOLIO_COLUMNS if x not in epic.BASE_COLUMNS)
    }
}
RESP_CREATE_INVESTOR_GOALS_FUNC_MAP: RouterMap = {
    (datetime(2021, 9, 1), datetime(9999, 12, 30)): {
        "function": epif.extract_investor_goals,
        "args" : tuple(x for x in epic.GOAL_TILTS_PORTFOLIO_COLUMNS if x not in epic.BASE_COLUMNS)
    }
}

RESP_RETRIEVE_INVESTOR_GOALS_FUNC_MAP: RouterMap = {
    (datetime(2021, 9, 1), datetime(9999, 12, 30)): {
        "function": epif.extract_investor_goals,
        "args" : tuple(x for x in epic.GOAL_TILTS_PORTFOLIO_COLUMNS if x not in epic.BASE_COLUMNS)
    }
}

RESP_UPDATE_INVESTOR_GOALS_FUNC_MAP: RouterMap = {
    (datetime(2021, 9, 1), datetime(9999, 12, 30)): { 
        "function": epif.extract_investor_goals,
        "args" : tuple(x for x in epic.GOAL_TILTS_PORTFOLIO_COLUMNS if x not in epic.BASE_COLUMNS)
    }
}

REQ_CREATE_INVESTOR_GOALS_FUNC_MAP: RouterMap = {
    (datetime(2021, 9, 1), datetime(9999, 12, 30)): {
        "function": epif.extract_investor_goals,
        "args" : tuple(x for x in epic.GOAL_TILTS_PORTFOLIO_COLUMNS if x not in epic.BASE_COLUMNS)
    }
}

REQ_UPDATE_INVESTOR_GOALS_FUNC_MAP: RouterMap = {
    (datetime(2021, 9, 1), datetime(9999, 12, 30)): {
        "function": epif.extract_investor_goals,
        "args" : tuple(x for x in epic.GOAL_TILTS_PORTFOLIO_COLUMNS if x not in epic.BASE_COLUMNS)
    }
}



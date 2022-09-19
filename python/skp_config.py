from typing import List
from jobs.reqresp import BASE_COLUMNS

#BASE_COLUMNS = ["profileId", "correlationId", "sessionId", "consumerId", "req_resp_ts"]

DB = "ead_skpmaster"

BASE_PREFIX = "master/skp"

BOOLEAN_FIELDS = [
    'holdRebalance',
    'skipClient'
]

DATETIME_FIELDS: List[str] = [
    "expirationDate",
    "lastUpdated"
]

# Table name example: da_resp_dly_fee_invoices

REQ_DLY_REBALANCE: List[str] = BASE_COLUMNS + [
    'holdRebalance',
    'lastUpdatedBy'
]

RESP_DLY_FEES: List[str] = BASE_COLUMNS + [
    'holdRebalance',
    'lastUpdatedBy',
    'expirationDate',
    'lastUpdated'
]


RESP_DLY_DETAILS: List[str] = BASE_COLUMNS + [
    'skipClient',
    'details_applicationcode',
    'details_message',
    'eligiblePortfolioManagementTriggers'
]

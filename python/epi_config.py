from typing import List
from jobs.reqresp import BASE_COLUMNS

DB: str = 'ead_epimaster'

BASE_PREFIX: str = 'master/epi'

DECIMAL_FIELDS: List[str] = [
    "shortTermTaxRate",
    "targetAmount",
    "ceilingAmount",
    "floorAmount"
]

INTEGER_FIELDS: List[str] = [
    "version",
    "activeEquityTarget"
]

DATETIME_FIELDS: List[str] = [
    "nextRebalanceDate",
    "lastUpdatedDateTime",
    "lastSavedDateTime"
]

#consumer - PAS, httpMethod - get/put/post
# Table name example: pas_resp_retrieve_portfolio
PROFILE_PORTFOLIO_COLUMNS:List[str] = BASE_COLUMNS + [
    "shortTermTaxRate",
    "nextRebalanceDate",
    "taxBracket",
    "version",
    "lastUpdatedDateTime",
    "lastSavedDateTime",
    "lastUpdatedUserId"
]

# Table name example: pas_resp_retrieve_investor_goals
GOAL_TILTS_PORTFOLIO_COLUMNS:List[str] = BASE_COLUMNS + [
    "goalId",
    "spendingFund_targetAmount",
    "spendingFund_ceilingAmount",
    "spendingFund_floorAmount",
    "spendingFund_accountId",
    "bond_tilt",
    "bond_lastUpdatedDateTime",
    "bond_lastUpdatedUserId",
    "fund_tilt",
    "fund_lastUpdatedDateTime",
    "fund_lastUpdatedUserId",
    "equity_tilt",
    "equity_activeEquityTarget",
    "equity_lastUpdatedDateTime",
    "equity_lastUpdatedUserId"
]

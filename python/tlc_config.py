from typing import List
from jobs.reqresp import BASE_COLUMNS

DB: str = "ead_tlcmaster"
BASE_PREFIX: str = "master/tlc"

decimal_fields: List[str] = [
    "averageCostPerShare",
    "gainOrLossAmount",
    "quantity"
]

boolean_fields: List[str] = [
    "covered",
    "hasPartialCost",
]

integer_fields: List[str] = []

datetime_fields: List[str] = [
    "tradeDateTimeString",
    "tradeDateTime",
    "tradeDate",
    "washSaleExpirationDate",
    "opportunityDate",
]

############################### REQUEST ###############################

FINANCIAL_SECURITIES: List[str] = BASE_COLUMNS + [
    "accountPositionId",
    "costBasisMethod",
    "securityId",
    "securityIdType",
]

UNREALIZED_TAX_LOTS: List[str] = BASE_COLUMNS + [
    "id",
    "averageCostPerShare",
    "covered",
    "gainOrLossAmount",
    "hasPartialCost",
    "lotType",
    "quantity",
    "securityId",
    "term",
    "tradeDateTime",
    "tradeType",
    "accountPositionId",
]

HISTORICAL_TRANS: List[str] = BASE_COLUMNS + [
    "accountPositionId",
    "securityIdType",
    "securityId",
    "tradeDate",
    "tradeStatus",
    "tradeType",
]

REALIZED_TAX_LOTS: List[str] = BASE_COLUMNS + [
    "accountPositionId",
    "gainOrLossAmount",
    "id",
    "lotType",
    "securityId",
    "securityIdType",
    "tradeDateTime",
    "tradeType",
]


############################### RESPONSE ###############################

HARVEST_OPP_SELL_TRANS: List[str] = BASE_COLUMNS + [
    "harvestOpportunityId",
    "opportunityDate",
    "aggregateMinimumHarvestableMarketValue",
    "opportunityCodes",
    "sellTransaction_accountPositionId",
    "sellTransaction_costBasisMethod",
    "sellTransaction_securityId",
    "sellTransaction_tradeType",
    "sellTransaction_washSaleExpirationDate",
    "sellTransaction_washSaleImminent",
]

SELL_TRANS_ALT_SHARE_CLASSES: List[str] = BASE_COLUMNS + [
    "securityId",
    "tradeType",
    "washSaleAccountPositionId",
    "washSaleExpirationDate",
    "washSaleImminent",
    "harvestOpportunityId",
]

TAX_LOTS: List[str] = BASE_COLUMNS + [
    "id",
    "averageCostPerShare",
    "covered",
    "gainOrLossAmount",
    "hasPartialCost",
    "lotType",
    "quantity",
    "securityId",
    "term",
    "tradeDateTimeString",
    "tradeType",
    "harvestOpportunityId",
]

BUY_TRANS: List[str] = BASE_COLUMNS + [
    "accountPositionId",
    "securityId",
    "tradeType",
    "washSaleExpirationDate",
    "washSaleImminent",
    "harvestOpportunityId",
]

BUY_TRANS_ALT_SHARE_CLASSES: List[str] = BASE_COLUMNS + [
    "securityId",
    "tradeType",
    "washSaleAccountPositionId",
    "washSaleExpirationDate",
    "washSaleImminent",
    "harvestOpportunityId",
]

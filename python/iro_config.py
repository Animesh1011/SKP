from typing import List
from jobs.reqresp import BASE_COLUMNS

DB: str = "ead_iromaster"
BASE_PREFIX: str = "master/iro"

DECIMAL_FIELDS: List[str] = [
    "oneTimeWithdrawalAmount",
    "value",
    "goalValue",
    "currentValue",
    "recommendedValue",
    "buyOrSellPercentage",
    "costBasis",
    "estimatedGainLoss",
    "exchangeableValue",
    "harvestableLossAmountToSell",
    "realizedGainOrLoss",
    "recommendedPositionUnrealizedGainOrLossAmount",
    "recommendedSharesToSell",
    "balance",
    "totalGainLoss",
    "totalRealizedGainOrLoss",
    "yearToDateContributionAmount",
    "feeAmount",
    "unrealizedGainOrLoss" ,
    "averageCostPerShare",
    "gainOrLossAmount",
    "recommendedQuantityToSell",
    "spendingFundCeilingAmount",
    "spendingFundFloorAmount",
    "spendingFundTargetAmount",
    "availableBalance",
    "availableBalanceAsOfDate",
    "employeeContributionAmount",
    "employerContributionAmount",
    "investedBalance",
    "currentSourceValue",
    "exchangeableSourceBalance",
    "recommendedSourceValue",
    "afterTax",
    "taxDeferred",
    "taxFree",
    "recommendedValue",
    "totalExchangeableBalance",
    "recommendedLotUnrealizedGainOrLossAmount",
    "recommendedQuantityToSell",







]

BOOLEAN_FIELDS: List[str] = [
   "includeRecommendationsForHypoManaged",
   "includeRecommendationsForPreManaged",
   "shouldSellAllExistingRetailPositions",
   "includePreviousDayBalance",
   "bondAggregatedRange",
   "containsNewRecommendation",
   "withdrawalNotifyAdvisor",
   "withdrawalNotifyAdvisor",
   "cashPositionHasViolation",
   "shouldRecommendFutureContributionAllocation",
   "minimumMarketOrderAdjustment",
   "newPosition",
   "settlementFund",
   "shareClassUpgraded",
   "shouldHarvestLosses",
   "covered",
   "hasPartialCost",
   "allocationBySourceAllowed",
   "exchangeBySourceAllowed",
   "recommendationsManuallyUpdated"


]  
DATETIME_FIELDS: List[str] = [
    "withdrawalDate",
    "withdrawalDueDate",
    "balanceAsOfDate",
    "rebalanceDateTime",
    "recordIdentifierDateTime",
    "washSaleExpirationDate",
    "tradeDateTime",
    "investedBalanceAsOfDate",
    "recommendationsUpdatedDateTime"

]
############################### REQUEST ###############################

REBALANCEREQUEST_REQ: List[str] = BASE_COLUMNS + [
  "includePreviousDayBalance",
  "processTriggeredBy",
  "processType",
  "rebalanceRequestedUserId",
  "runType"
]
GOALS_CONF_REQ: List[str] = BASE_COLUMNS + [
    "goalId",
    "includeRecommendationsForHypoManaged",
    "includeRecommendationsForPreManaged",
    "oneTimeWithdrawalAccountId",
    "oneTimeWithdrawalAmount",
    "oneTimeWithdrawalGoalId",
    "rebalanceType",
    "shouldSellAllExistingRetailPositions",
    "withdrawalStatement_accountID",
    "withdrawalStatement_purpose",
    "withdrawalStatement_statementId",
    "withdrawalStatement_sweepPositionId",
    "withdrawalStatement_withdrawalDate",
    "withdrawalStatement_withdrawalDueDate",
    "withdrawalStatement_withdrawalType",
    "withdrawalStatement_amount_currencyCode",
    "withdrawalStatement_amount_value"
]

############################### RESPONSE ###############################

GOALS_RESP: List[str] = BASE_COLUMNS + [
    "goalId",
    "bondAggregatedRange",
    "containsNewRecommendation",
    "goalValue",
    "rebalanceType",
    "withdrawalNotifyAdvisor"
]

ACCOUNTS_RESP: List[str] = BASE_COLUMNS + [
    "goalId",
    "accountsId",
    "accountManagementType",
    "accountType",
    "balance",
    "totalGainLoss",
    "totalRealizedGainOrLoss",
    "yearToDateContributionAmount",
    "cashPosition_buyOrSellPercentage",
    "cashPosition_currentValue",
    "cashPosition_recommendedValue",
    "CashInPosition_currentValue",
    "CashInPosition_recommendedValue"
]

POSITIONS_RESP: List[str] = BASE_COLUMNS + [
    "goalId",
    "accountsId",
    "buyOrSellPercentage",
    "costBasis",
    "costBasisMethod",
    "costBasisMethodUsedForProcessing",
    "currentValue",
    "estimatedGainLoss",
    "exchangeableValue",
    "harvestableLossAmountToSell",
    "linkedAccountId",
    "minimumMarketOrderAdjustment",
    "newPosition",
    "positionGroupId",
    "positionGroupIdType",
    "positionId",
    "positionInstruction",
    "realizedGainOrLoss",
    "recommendedPercentage",
    "recommendedPositionUnrealizedGainOrLossAmount",
    "recommendedSharesToSell",
    "recommendedValue",
    "securityId",
    "securityIdType",
    "securityName",
    "settlementFund",
    "shareClass",
    "shareClassUpgraded",
    "shouldHarvestLosses",
    "totalRealizedGainOrLoss",
    "unrealizedGainOrLoss",
    "identification_cusip",
    "identification_securityIdentification",
    "identification_sedol",
    
  "washSaleTransactions_washSaleAccountPositionId",
    "washSaleTransactions_washSaleExpirationDate"
]

CONFIGURATION_RESP: List[str] = BASE_COLUMNS + [
    "goalId",
    "accountsId",
    "oneTimeWithdrawalAmount",
    "rebalanceType",
    "spendingFundCeilingAmount",
    "spendingFundFloorAmount",
    "spendingFundTargetAmount"
]


#!/bin/bash

#==============================================================================
# DESCRIPTION: Bash script to run BES Normalization Adhoc Backfill.
#
# PARAMETER 1: Raw bucket - Bucket to read raw data from
# PARAMETER 2: Output bucket - Bucket to write to
#==============================================================================

source /home/hadoop/app/scripts/masking/mask.bash ${1} ${2} ${3}

START="2022-01-01"
END="2022-07-18"
DEPLOY=$(date +%F)
INTERVAL=15
BASE_PATH="/home/hadoop/app"

DATESTR="$(python3 ${BASE_PATH}/scripts/backfill_gen.py \
--start-date ${START} --deploy-date ${DEPLOY} --end-date ${END} \
--interval ${INTERVAL})"
echo ${DATESTR}
RESULT=0
OVERRIDE=${DEPLOY}
while [[ $DATESTR ]]
do
    for file in /home/hadoop/app/configurations/mask_configs/skp/*.json; do
        mask $file ${DATESTR} False
    done
    if [ $? -ne 0 ]
    then
        RESULT=1
    fi
	OVERRIDE=$(date -d"$OVERRIDE + 1 day" +"%Y-%m-%d")
    DATESTR="$(python3 ${BASE_PATH}/scripts/backfill_gen.py \
        --start-date ${START} --deploy-date ${DEPLOY} --end-date ${END} \
        --interval ${INTERVAL} --override-date ${OVERRIDE})"
    echo ${DATESTR}
done

if [[ "${RESULT}" -eq "0" ]] 
then
    echo "Job succeeded"
    exit 0
else
    echo "Job Failed"
    exit 1
fi

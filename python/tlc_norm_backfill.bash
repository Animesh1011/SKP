#!/bin/bash

#==============================================================================
# DESCRIPTION: Bash script to run TLC Request Response Normalization.
#
# PARAMETER 1: cleansed Bucket
#==============================================================================

echo "Starting TLC Normalization Backfill"

START="2022-01-26"
END="2022-08-08"
DEPLOY=$(date +%Y-%m-%d)
INTERVAL=4

BASE_PATH="/home/hadoop/app"
 
DATESTR="$(python3 ${BASE_PATH}/scripts/backfill_gen.py \
        --start-date ${START} --deploy-date ${DEPLOY} --end-date ${END} \
        --interval ${INTERVAL})"
 
echo "Generated Datestr = ${DATESTR}"
RESULT=0
OVERRIDE=${DEPLOY}

while [[ $DATESTR ]]
do

    # Running spark-submit for normalization
    spark-submit --deploy-mode client --driver-cores 5 --driver-memory 10g \
            --executor-memory 10G \
            --py-files hdfs:///home/hadoop/app/dist/jobs.zip \
            /home/hadoop/app/dist/main.py \
            --job reqresp.tlc.tlc_main \
            --job-args bucket=${1}
            dates=${DATESTR} default_date="False"
            
    norm_result=$?
    [ $norm_result -eq 0 ]; RESULT=$?;
	if [[ $RESULT -ne 0 ]]
	then
		RESULT=1
	fi
    OVERRIDE=$(date -d"$OVERRIDE + 1 day" +"%Y-%m-%d")
    DATESTR="$(python3 ${BASE_PATH}/scripts/backfill_gen.py \
        --start-date ${START} --deploy-date ${DEPLOY} --end-date ${END} \
        --interval ${INTERVAL} --override-date ${OVERRIDE})"
    echo ${DATESTR}

done

# Check that the result of both spark-submits is 0
# if not exit the script with exit code 1
if [ $RESULT -eq 0 ]
then
    echo "Job succeeded"
    exit 0
else
    echo "Job Failed"
    exit 1
fi
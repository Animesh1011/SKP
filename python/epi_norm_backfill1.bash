#!/bin/bash

#==========================================================================
# DESCRIPTION: Bash script to run EPI Request Response Compaction Backfill.
#
# PARAMETER 1: raw Bucket
# PARAMETER 2: cleansed Bucket
#==========================================================================

echo "Starting EPI Compaction Backfill on epi-controller1"

START="2021-11-30"
END="2022-01-04"
DEPLOY=$(date +%Y-%m-%d)
INTERVAL=12

BASE_PATH="/home/hadoop/app"
 
DATESTR="$(python3 ${BASE_PATH}/scripts/backfill_gen.py \
        --start-date ${START} --deploy-date ${DEPLOY} --end-date ${END} \
        --interval ${INTERVAL})"
 
echo "Generated Datestr = ${DATESTR}"
RESULT=0

OVERRIDE=${DEPLOY}

while [[ $DATESTR ]]
do
    #Running spark-submit for compaction
    preprocessing(){
        spark-submit --deploy-mode client --driver-memory 9g --driver-cores 5 --executor-cores 2 \
                    --py-files hdfs:///home/hadoop/app/dist/jobs.zip \
                    /home/hadoop/app/dist/main.py \
                    --job reqresp.feereport_preprocess \
                    --job-args raw_bucket=${1} output_bucket=s3://${2}/master/staging/${3} base_key=${4} dates=${5} default_date="False"
    }

    echo "Run preprocessing EPI Response PAS"
    preprocessing ${1} ${2} "epi_pas_response" "Master/EPI-Response/VGI-US-PERSONAL_ADVISOR_SERVICES" ${DATESTR}

    comp_resp_result=$?

    echo "Run preprocessing EPI Request PAS"
    preprocessing ${1} ${2} "epi_pas_request" "Master/EPI-Request/VGI-US-PERSONAL_ADVISOR_SERVICES" ${DATESTR}

    comp_req_result=$?

    [ $comp_resp_result -eq 0 ] && [ $comp_req_result -eq 0 ]; RESULT=$?;
	if [[ $RESULT -ne 0 ]]
	then
		RESULT=1
	fi
    OVERRIDE=$(date -d"$OVERRIDE + 1 day" +"%Y-%m-%d")
    DATESTR="$(python3 ${BASE_PATH}/scripts/backfill_gen.py \
        --start-date ${START} --deploy-date ${DEPLOY} --end-date ${END} \
        --interval ${INTERVAL} --override-date ${OVERRIDE})"
    echo ${DATESTR}

    hdfs dfs -rm -r "/home/hadoop/app/spark-checkpoint/"
    echo "deleted spark-checkpoint directory"
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

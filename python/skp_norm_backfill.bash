#!/bin/bash

#==============================================================================
# DESCRIPTION: Bash script to run SKP Request Response Compaction and Normalization Backfill.
#
# PARAMETER 1: raw Bucket
# PARAMETER 2: cleansed Bucket
#==============================================================================

echo "Starting SKP Normalization Backfill"

RERUN_DATE="[\"2022-08-29\"]"

NORM_DATES="[\"2022-08-29\",\"2022-08-28\",\"2022-08-27\"]"

if [[ $RERUN_DATE ]] && [[ $NORM_DATES ]];
then
    #Running spark-submit for compaction
    preprocessing(){
        spark-submit --deploy-mode client --driver-memory 9g --driver-cores 5 --executor-cores 2 \
                    --py-files hdfs:///home/hadoop/app/dist/jobs.zip \
                    /home/hadoop/app/dist/main.py \
                    --job reqresp.feereport_preprocess \
                    --job-args raw_bucket=${1} output_bucket=s3://${2}/master/staging/${3} base_key=${4} dates=${5} default_date="False"
    }
    echo "Run preprocessing SKP Response PAS"
    preprocessing ${1} ${2} "skp_pas_response" "Master/SKP-Response/VGI-US-PERSONAL_ADVISOR_SERVICES" ${RERUN_DATE}

    comp_req_result=$?

    # Running spark-submit for normalization
    spark-submit --deploy-mode client --driver-cores 5 --driver-memory 10g \
            --executor-memory 10G \
            --py-files hdfs:///home/hadoop/app/dist/jobs.zip \
            /home/hadoop/app/dist/main.py \
            --job reqresp.skp.skp_main \
            --job-args bucket=${2} \
            dates=${NORM_DATES} default_date="False"

    norm_result=$?

    [ $comp_req_result -eq 0 ] && [ $norm_result -eq 0 ]; RESULT=$?;
	if [[ $RESULT -ne 0 ]]
	then
		RESULT=1
	fi
else
    echo "No Dates to Backfill!"
    RESULT=0
fi


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
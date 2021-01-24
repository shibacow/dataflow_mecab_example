#!/bin/bash
export PROJECT=xxxxxxx
export BUCKET=xxxxxxxxxx

export REGION=us-central1
export RUNNER=DataflowRunner
export DT=`date "+%s"`
#export MODE=COST_OPTIMIZED
export MODE=SPEED_OPTIMIZED
export INPUT=$PROJECT:user_bq_dataset.user_bq_table
export MACHINE_TYPE=n1-highmem-16
#export MACHINE_TYPE=n1-standard-2
python3 mecab_count.py \
	--region $REGION \
	--input  $INPUT \
	--output gs://$BUCKET/results_$DT/outputs \
	--runner $RUNNER \
	--project $PROJECT \
	--flexrs_goal $MODE \
	--dicpath gs://$BUCKET/dic/mecab-ipadic-neologd \
	--machine_type  $MACHINE_TYPE \
	--temp_location gs://$BUCKET/tmp/ \
	--setup_file ./setup.py  



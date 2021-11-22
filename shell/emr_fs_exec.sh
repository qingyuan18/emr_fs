#!/usr/bin/env bash

##替換具體變量########
##寫入s3 script#####
##執行emr steps#####
aws emr add-steps \
--cluster-id $emr_cluster_id \
--steps Type=CUSTOM_JAR,Name="Run a script from S3 with script-runner.jar",ActionOnFailure=CONTINUE,Jar=s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar, \
Args=[$s3_script_location/$emr_fs_script]
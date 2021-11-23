#!/usr/bin/env bash

##替換具體變量########
##寫入s3 script#####
##執行emr steps#####
aws emr add-steps \
--cluster-id $emr_cluster_id \
--steps Type=CUSTOM_JAR,Name="Run a script from S3 with script-runner.jar",ActionOnFailure=CONTINUE,Jar=s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar, \
Args=[$s3_script_location/$emr_fs_script]


######get table properties ##########################
hive -e "show create table default.test"|cut -d '(' -f2|cut -d ')' -f1|read normalFeaturesKeys
tableDesc=$(hive -e "show create table default.test")
tableProps=${tableDesc#*TBLPROPERTIES}
( 'bucketing_version'='2', 'feature_unique_id'='userid', 'last_modified_by'='hadoop', 'last_modified_time'='1637572572', 'transient_lastDdlTime'='1637572572')

#####get each properties ########################
array=(`echo $tableProps | tr ',' ' '` )
for each in ${array[@]}
do
    result=$(echo $each | grep "feature_unique_id")
    if [[ "$result" != "" ]]
    then
      feature_unique_id=${each#*=}
    else

    fi
done


###add feature store  metadata######
hive -e "alter table test1 set tblproperties('feature_unique_id'='userid')"
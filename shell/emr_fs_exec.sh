#!/usr/bin/env bash

##替換具體變量########
##寫入s3 script#####
##執行emr steps#####
aws emr add-steps \
--cluster-id $emr_cluster_id \
--steps Type=CUSTOM_JAR,Name="Run a script from S3 with script-runner.jar",ActionOnFailure=CONTINUE,Jar=s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar, \
Args=[$s3_script_location/$emr_fs_script]
if $1 = "inspect" ; then
   inspect $@
fi
inspect() {
  featureStoreName=$2
  featureGroupName=$3
  ######get table properties ##########################
  hive -e "show create table ${featureStoreName}.${featureGroupName}"|cut -d '(' -f2|cut -d ')' -f1|read normalFeaturesKeys
  tableDesc=$(hive -e "show create table ${featureStoreName}.${featureGroupName}")
  tableProps=${tableDesc#*TBLPROPERTIES}

  #####get each properties ########################
  feature_meta_string=""
  array=(`echo $tableProps | tr ',' ' '` )
  for each in ${array[@]}
  do
      result=$(echo $each | grep "feature_unique_id")
      if [[ "$result" != "" ]];then
        feature_meta_string=${feature_meta_string}" "${result}
      fi
  done
  return feature_meta_string
}

###add feature store  metadata######
hive -e "alter table test1 set tblproperties('feature_unique_id'='userid')"
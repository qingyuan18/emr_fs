#!/usr/bin/env bash

if $1 = "inspect" ; then
   inspect $@
fi
inspect() {
  featureStoreName=$2
  featureGroupName=$3
  ######get table properties ##########################
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
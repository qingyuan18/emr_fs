#!/usr/bin/env bash
printUsage() {
    echo "****************************************************************************"
    echo "EMR feature store management util"
	  echo "Choose task option for you want"
    echo "i)deploy feature store on EMR cluster"
    echo "l)list feature store group "
	  echo "p)inspect feature store group"
	  echo "q)versioning(time travel) query "
    echo "l)ingest to offline feature store"
    echo "s)sychronize offline to online feature store"
    echo "q)quit"
    echo "****************************************************************************"
	echo "please input your option"
}


parseArgs() {
    if [ "$#" -eq 0 ]; then
        printUsage
        exit 0
    fi
    optString="emr-cluster-id:,feature-store-location:,feature-store-group-name:,version-start-dt:,version-end-dt:,ingest-s3-data:,sychronize-online-featureStore"
    # IMPORTANT!! -o option can not be omitted, even there are no any short options!
    # otherwise, parsing will go wrong!
    OPTS=$(getopt -o "" -l "$optString" -- "$@")
    exitCode=$?
    if [ $exitCode -ne 0 ]; then
        echo ""
        printUsage
        exit 1
    fi
    eval set -- "$OPTS"
    while true; do
        case "$1" in
            --emr-cluster-id)
                EMR_CLUSTER_ID="${2}"
                shift 2
                ;;
            --feature-store-group-name)
                FEATURE_STORE_GROUP_NAME="$2"
                shift 2
                ;;
            --feature-store-location)
                FEATURE_STORE_LOCATION="$2"
                shift 2
                ;;
            --version-start-dt)
                VERSION_START_DT="$2"
                shift 2
                ;;
            --version-end-dt)
                VERSION_END_DT="$2"
                shift 2
                ;;
            --ingest-s3-data)
                INGEST_S3_DATA="$2"
                shift 2
                ;;
			--sychronize-online-featureStore)
                ONLINE_FEATURE_STORE_NAME="$2"
				shift 2
				;;
        esac
    done
}

#parseArgs $@

while true; do
   printUsage
   read option
   case $option in
     "q")
	   exit 1
	   ;;
     "i")
       read -p "please input emr-cluster-id:"  EMR_CLUSTER_ID
	     read -p "please input feature-store-group-name:"  FEATURE_STORE_GROUP_NAME
	     read -p "please input feature store location(s3):" FEATURE_STORE_LOCATION
	##   ##检查emr集群connectivity，IAM权限及Hudi
	##   #checkEMRHudi($EMR_CLUSTER_ID)
	##   #createFeatureStore($EMR_CLUSTER_ID,$FEATURE_STORE_GROUP_NAME)
	   ;;
	  *)
        echo ""
        echo "Invalid option $1." >&2
        printUsage
        ;;
   esac
done
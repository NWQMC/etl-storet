#!/bin/bash
WORK_DIR=/pdc/wqp_data

# display usage message
function usage() {
	cat <<EndUsageText

Usage: `basename $0` EXPORT_TYPE TARGET_ENV

	This script pulls completes the storet data ETL by moving the export log to a reference file and clearing the WQP service cache.
		
EXPORT_TYPE
	One of these must be specified. If more than one is set, the last one parsed will win.
			
	Monthly  download the monthly export
	Weekly   download the weekly export
			
TARGET_ENV
	Tells the script which environment to clear the WQP cache for.
	One of these must be specified. If more than one is set, the last one parsed will win.

	ci       continuous integration
	dev      development
	qa       QA
	prod     production
		
EndUsageText
}

# output of this script is parsed and looks for this text to raise errors
function stop_bad() {
	echo "Script generated an error, quitting."
	exit 1
}

# parse arguments
for arg in "$@"
do
	case $arg in
		Monthly)
			EXPORT_TYPE=$arg
			;;
		Weekly)
			EXPORT_TYPE=$arg
			;;
		ci)
			TARGET_ENV=$arg
			CLEAR_CACHE_URL="Does CI have any related deployed code environment?"
			;;
		dev)
			CLEAR_CACHE_URL="http://cida-eros-wqpdev.er.usgs.gov:8080/ogcproxy/rest/clearcache/wqp_sites"
			;;
		qa)
			CLEAR_CACHE_URL="http://cida-eros-wqpqa.er.usgs.gov:8080/ogcproxy/rest/clearcache/wqp_sites"
			;;
		prod)
			CLEAR_CACHE_URL="http://www.waterqualitydata.us/ogcservices/rest/clearcache/wqp_sites"
			;;
	esac
done

# if any required variables are null or empty, display usage and quit
([ ! -n "${EXPORT_TYPE}" ] || [ ! -n "${CLEAR_CACHE_URL}" ]) && usage && stop_bad

EXPORT_REF="stormodb_shire_storetw_${EXPORT_TYPE}_expdp.ref"
EXPORT_LOG="stormodb_shire_storetw_${EXPORT_TYPE}_expdp.log"

starting_dir=`pwd`

cd ${WORK_DIR}

mv ${EXPORT_LOG} ${EXPORT_REF}
curl ${CLEAR_CACHE_URL} > /dev/null 2>&1

cd ${starting_dir}
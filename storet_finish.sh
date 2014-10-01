#!/bin/bash
WORK_DIR=/pdc/wqp_data

# display usage message
function usage() {
	cat <<EndUsageText

Usage: `basename $0` EXPORT_TYPE

	This script pulls completes the storet data ETL by moving the export log to a reference file and clearing the WQP service cache.
		
EXPORT_TYPE
	One of these must be specified. If more than one is set, the last one parsed will win.
			
	Monthly  download the monthly export
	Weekly   download the weekly export
			
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
	esac
done

# if any required variables are null or empty, display usage and quit
[ ! -n "${EXPORT_TYPE}" ] && usage && stop_bad

EXPORT_REF="stormodb_shire_storetw_${EXPORT_TYPE}_expdp.ref"
EXPORT_LOG="stormodb_shire_storetw_${EXPORT_TYPE}_expdp.log"

starting_dir=`pwd`

cd ${WORK_DIR}

mv ${EXPORT_LOG} ${EXPORT_REF}

cd ${starting_dir}
#!/bin/bash
WORK_DIR=$1

# display usage message
function usage() {
	cat <<EndUsageText

Usage: `basename $0` working_directory

	This script pulls completes the storet data ETL by moving the export log to a reference file and clearing the WQP service cache.
		
EndUsageText
}

# output of this script is parsed and looks for this text to raise errors
function stop_bad() {
	echo "Script generated an error, quitting."
	exit 1
}

EXPORT_REF="owpubdw_vmwaters1_storetw_Monthly_expdp.ref"
EXPORT_LOG="owpubdw_vmwaters1_storetw_Monthly_expdp.log"

starting_dir=`pwd`

cd ${WORK_DIR}

mv ${EXPORT_LOG} ${EXPORT_REF}

cd ${starting_dir}

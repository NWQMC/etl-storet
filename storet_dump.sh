#!/bin/bash
DATE_SUFFIX=`date +%Y%m%d_%H%M`
HTTP_BASE=http://www.epa.gov/storet/download/storetw
WORK_DIR=/pdc/temp

# display usage message
function usage() {
	cat <<-EndUsageText
		Usage: `basename $0` OPTIONS
		
		This script retrieves weekly or monthly storet data exports from the EPA.
		It pulls down the appropriate log file and checks the export was successfully completed.
		It checks the previous 
		
		OPTIONS:
		-m download the monthly exports
		
		-w download the weekly exports
		
	EndUsageText
}

# output of this script is parsed and looks for this text to raise errors
function stop_bad() {
	echo "Script generated an error, quitting."
	exit 1
}

# output of this script is parsed and looks for this text to not run other steps
function stop_ok() {
	echo "No new export to process."
	exit 0
}

# set so if any command in a piped sequence returns a non-zero error code, the script fails
set -o pipefail

# if not a single parameter, display usage and quit
[ "$#" -ne 1 ] && usage && stop_bad

# check options for -m [monthly] or -w [weekly], first one it finds is the one it uses
while getopts ":mw" opt
do
	case $opt in
		m)
			EXPORT_LOG="stormodb_shire_storetw_Monthly_expdp.log"
			EXPORT_REF="stormodb_shire_storetw_Monthly_expdp.ref"
			DUMP_FILE_GREP="stormodb_shire_storetw_Monthly_...cdmp"
			break
			;;
		w)
			EXPORT_LOG="stormodb_shire_storetw_Weekly_expdp.log"
			EXPORT_REF="stormodb_shire_storetw_Weekly_expdp.ref"
			DUMP_FILE_GREP="stormodb_shire_storetw_Weekly_...cdmp"
			break
			;;
	esac
done

# if any required variables are null or empty, display usage and quit
([ ! -n "${EXPORT_LOG}" ] || [ ! -n "${EXPORT_REF}" ] || [ ! -n "${DUMP_FILE_GREP}" ]) && usage && stop_bad

starting_dir=`pwd`

cd ${WORK_DIR}

# quietly pull the export log, using timestamping to only pull if remote file is newer than local
wget -Nq ${HTTP_BASE}/${EXPORT_LOG}

# check export log for the phrase 'successfully completed'; if not found, quit
grep -q "successfully completed" ${EXPORT_LOG} || (echo "${EXPORT_LOG} does not contain 'successfully completed'." && stop_bad)

# if a reference file exists and the log isn't newer than the reference file, we don't need to do more
([ -f ${EXPORT_REF} ] && [ ! ${EXPORT_LOG} -nt ${EXPORT_REF} ]) && stop_ok

# remove any dump files found locally but not in the export log
comm -13 <(grep -o ${DUMP_FILE_GREP} ${EXPORT_LOG}) <(ls | grep ${DUMP_FILE_GREP}) | xargs rm -f

# download any dump files newer on remote than they are on local
grep -o ${DUMP_FILE_GREP} ${EXPORT_LOG} | sed -e 's/^/http:\/\/www.epa.gov\/storet\/download\/storetw\//' | xargs -n 1 -P 12 wget -Nq

cd ${starting_dir}
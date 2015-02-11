#!/bin/bash
DATE_SUFFIX=`date +%Y%m%d_%H%M`
HTTP_BASE=http://www.epa.gov/storet/download/storetw
WORK_DIR=/pdc/wqp_data

# display usage message
function usage() {
	cat <<EndUsageText

Usage: `basename $0` EXPORT_TYPE

	This script pulls storet data exports from the EPA.
		
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

# output of this script is parsed and looks for this text to not run other steps
function stop_ok() {
	echo "No new export to process."
	exit 0
}

# set so if any command in a piped sequence returns a non-zero error code, the script fails
set -o pipefail

# if not exactly one parameter, display usage and quit
[ "$#" -ne 1 ] && usage && stop_bad

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

EXPORT_REF="owpubdw_vmwaters1_storetw_${EXPORT_TYPE}_expdp.ref"
EXPORT_LOG="owpubdw_vmwaters1_storetw_${EXPORT_TYPE}_expdp.log"
DUMP_FILE_GREP="owpubdw_vmwaters1_storetw_${EXPORT_TYPE}_...cdmp"

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
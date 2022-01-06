#!/bin/bash

# =======================================================================
# USAGE MESSAGES
# =======================================================================

USAGE="
USAGE: anonymize standard [DATE] [REQUESTOR] [-options ...]

Launch imaging services anonymization pipeline using standard rules.

Example 1 (no options):   anonymize standard 01_01_20 Chang

Example 2 (with options): anonymize standard 01_01_20 Dan -as (-as for accession/no removing secondaries)

OPTIONS:
  
  -a                anonymize using an accession column in the csv
  -s                anonymize with shifting the dates
  -n                anonymize without removing any secondaries
  -d                used by caidm workstation script to mount files
  -l                anonymize using lighter rules
  -r                no anonymization will be performed
"

  usage() {
    echo "$USAGE"
  }

  if [[ $# -eq 0  ]] || [[ "$1" == "-h"  ]] || [[ "$1" == "--help"  ]]; then
    usage
  exit
  fi

# =======================================================================
# PIPELINE
# =======================================================================

# --- Extract ENV variables
ANON_SCRIPT_PATH=${ANON_SCRIPT_PATH:-"$ANON_PATH/scripts/"}
export ANON_SCRIPT_PATH

# --- add to pythonpath
PYTHONPATH=${PYTHONPATH:-"$ANON_SCRIPT_PATH"}
export PYTHONPATH

# --- extract date / requestor
DATE=$1
REQUESTOR=$2

python $ANON_PATH/anonymize_standard.py $DATE $REQUESTOR ${@:3}

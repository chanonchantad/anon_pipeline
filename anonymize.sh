#!/bin/bash

# =======================================================================
# USAGE MESSAGES
# =======================================================================

USAGE="
USAGE: anonymize [COMMAND] [-options ...]

anonymization command-line interface

COMMANDS:

  standard            Launch regular anonymization pipeline using standard rules.
  accession           Launch accession anonymization pipeline using manual accession input.
  folder              Launch anonymization on a folder
"

usage() {
        echo "$USAGE"

}

if [[ $# -eq 0  ]] || [[ "$1" == "-h"  ]] || [[ "$1" == "--help"  ]]; then
    usage
    exit
fi

# --- Extract command
COMMAND=$1
shift

# =======================================================================
# RUN
# =======================================================================
case $COMMAND in
        standard)
            "$ANON_PATH/anonymize_standard.sh" "$@"
            exit
            ;;
        accession)
            "$ANON_PATH/anonymize_acc.sh" "$@"
            exit
            ;;
esac

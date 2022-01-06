#!/bin/bash

date=$1
name=$2

# --- anonymization workstation
scp -rp /data/apps/DICOMPipeline/anon_pipeline/mirc/anon caidm@128.195.184.221:~/Desktop/mirc_anon_${date}_${name}


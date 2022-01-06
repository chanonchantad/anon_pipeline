# DICOM anonymization pipeline.

## SETUP:

**Add these lines to .bashrc or .zshrc:**

alias anonymize='<PATH_TO>/anonymize.sh'\
export ANON_PATH='<PATH_TO>/anon_pipeline'

**Configure config.yml in the 'config' folder**

PACS_DL_PATH: <PATH WHERE DICOM FILES WILL GET DOWNLOADED>\
CSV_PATH: <PATH WHERE ALL THE CSV FILES ARE STORED>\
EXPORT_PATH: <PATH WHERE FILES END UP AFTER FINISHING>

## USAGE:

anonymize [COMMAND] [-options ...]

anonymization command-line interface

COMMANDS:

  standard  - Launch regular anonymization pipeline using standard rules.\
  accession - Launch accession anonymization pipeline using manual accession input.

## STEPS:

anonymization_standard.py contains the entire pipeline and calls out these different steps within scripts.

1) **cleandirs.py** - cleans all of the working directory where the dicom files are processed

2) **download.py / download_acc.py** - will query the PACS database based on the csv defined in query.csv and generate matches, missing and exclude.

3) **find_discrepancy.py** - creates discrepancy.csv to show which rows in query.csv were not found in PACS. IGNORED if using query by accession flag.

4) **pacs_tools.py** - has dependencies to **pacs.py** and **pacs_client.py**. begins the dicom download process from pacs.

5) **countv2.sh** - keeps track of the dicoms downloading from pacs and assumes download is done once a certain amount of time passes.

6) **sorter_anonymizer.py** - sorts the folders into accession-named folders within ~/mirc/flat and then quarantines all dicom files based on the rules defined in this file in ~/mirc/sorted. Sends files to ~/mirc/anon 

7) **anonymize_dicoms.py** - has dependencies to hash.py and files within "rules". anonymizes all dicoms within ~/mirc/anon based on the rules provided in the .csv file.

8) **post_process.py** - secondary processing to scrub pixel_array and remove burnt in PHI. Uses the rules specified in da-pixel.yml

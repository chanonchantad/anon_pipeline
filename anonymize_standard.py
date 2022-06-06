import sys, os, yaml
import subprocess

# --- import custom libraries
import cleandirs
import download, download_acc # for querying pacs
import find_discrepancy       # for finding missing requests
import sorter_anonymizer      # sort and quarantine dicoms
import anonymize_dicoms       # anonymize dicoms
import find_pid_modality    # sort dicoms into secondary modalities and pid
import post_process

# --- import pacs libraries
import pacs_tools
import pandas as pd
# ----------------------------------------------
# GLOBAL VARIABLES
# ----------------------------------------------
# --- path globals
ANON_ROOT_PATH = os.environ['ANON_PATH']

# --- read yaml file
paths = yaml.load(open(ANON_ROOT_PATH + '/config/config.yml', 'r'), Loader=yaml.Loader)
CSV_PATH = paths['CSV_PATH']
ACC_CSV_PATH = paths['ACC_CSV_PATH']
PACS_DL_PATH = paths['PACS_DL_PATH']

# --- requestor related globals
DATE = sys.argv[1]
REQUESTOR = sys.argv[2]

# ----------------------------------------------
# PARSE FLAGS PROVIDED IN COMMAND LINE
# ----------------------------------------------

flag_synonyms = {
    '-accession' : 'a',
    '-mount' : 'm',
    '-light' : 'l',
    '-raw'   : 'r',
    '-nosort': 'n',
    '-privonly' : 'p',
    '-shift' : 's',
    '-custom': 'c',
    '-killdl': 'k'
}

flag_rules = {
    'a' : 'ACCESSION',
    'm' : 'MOUNT',
    'l' : 'LIGHT',
    'r' : 'RAW',
    'n': 'NOSORT',
    'p': 'PRIVONLY',
    's': 'SHIFT',
    'c': 'CUSTOM',
    'k': 'KILL_DOWNLOAD'
}

flag_vars = {
    'ACCESSION' : False,
    'MOUNT' : False,
    'LIGHT' : False,
    'RAW' : False,
    'NOSORT' : False,
    'PRIVONLY': False,
    'SHIFT': False,
    'CUSTOM': False,
    'KILL_DOWNLOAD': False
}

# --- check if any flags are given
if len(sys.argv) > 3:
    
    # --- parse and normalize synonyms flags
    flags = sys.argv[3:]
    flags = [flag_synonyms[f] if f in flag_synonyms.keys() else f for f in flags]
    
    # --- set globals based on flags provided
    for flag in flags:
        
        # --- remove '-' from the command
        flag = flag.replace('-', '')

        if len(flag) > 1:

            for f in flag:

                if flag_rules[f] in flag_vars.keys():

                    flag_vars[flag_rules[f]] = True
        else:
            if flag_rules[flag] in flag_vars.keys():

                flag_vars[flag_rules[flag]] = True

# ----------------------------------------------
# PIPELINE PROCESS
# ----------------------------------------------

for mode, boolean in flag_vars.items():
    if boolean:
        print('RUNNING: ' + mode)

# --- clean directories
cleandirs.clean(ANON_ROOT_PATH + '/mirc')
cleandirs.clean(ANON_ROOT_PATH + '/flat')

# --- perform pacs query and generate matches, exclude, missing csv files
requestor_path = CSV_PATH + DATE + '/' + REQUESTOR

if not flag_vars['KILL_DOWNLOAD']:

    # --- choose which download.py file to use based on flags
    if flag_vars['ACCESSION']:
        download_acc.main(root=requestor_path)
    else:
        download.main(root=requestor_path)

    # --- copy files to workstation if MOUNT is activated. Mounted path specific
    if flag_vars['MOUNT']:
        subprocess.run('rm -rf /data/dicom/mirc_csvs/*', shell=True)
        subprocess.run('cp -r ' + requestor_path + '/csvs/* /data/dicom/mirc_csvs/', shell=True)
        subprocess.run('chmod 666 /data/dicom/mirc_csvs/*', shell=True)

    # --- create discrepancy.csv to show rows not found from standard query. IGNORE for accession based query
    if not flag_vars['ACCESSION']:
        find_discrepancy.find_discrepancy(REQUESTOR, DATE, CSV_PATH)

    # --- allow user to check and confirm csvs generated
    user_input = input("Please review generated matches csv file. Continue to download? Y/N: ")
    if user_input != 'Y':
        sys.exit("Terminating current anonymization pipeline request.")

    if flag_vars['MOUNT']:
        subprocess.run('cp -r /data/dicom/mirc_csvs/* ' + requestor_path + '/csvs/', shell=True)
        subprocess.run('rm -rf /data/dicom/mirc_csvs/*', shell=True)

    # --- begin download from pacs. continue pipeline when download is finished (tracked by countv2.sh)
    pacs_tools.main(requestor_path)
    subprocess.run([ANON_ROOT_PATH + '/scripts/countv2.sh', PACS_DL_PATH, 'downloaded'])

# --- determine if secondaries and foreign files will be removed with "no sort" flag
if not flag_vars['NOSORT']:

    # --- recursively move files from PACS download area to PROCESS AREA
    subprocess.run('mv ' + PACS_DL_PATH + '* ' + ANON_ROOT_PATH + '/mirc/flat/', shell=True)

    # --- sort and quarantine dicom files
    sorter_anonymizer.sort(ANON_ROOT_PATH + '/mirc')
else:
    # --- recursively move files from PACS download area to PROCESS AREA
    subprocess.run('mv ' + PACS_DL_PATH + '* ' + ANON_ROOT_PATH + '/mirc/anon/', shell=True)

if not flag_vars['NOSORT']:

    # --- find request pids (for sorting folders) and modalities containing secondaries (for post processing)
    pid_dict, scrub_dict = find_pid_modality.create_pid_2nd_scrub_dicts(DATE, REQUESTOR, CSV_PATH, ANON_ROOT_PATH)
    
    try:
        # --- post process secondaries if request contains modalities with secondary images
        if not flag_vars['RAW']:
            
            rules = post_process.prepare_yaml(ANON_ROOT_PATH + '/rules/da-pixel.yml')

            # post_process.anonymize_all(scrub_dict, rules)
            post_process.anonymize(ANON_ROOT_PATH + '/mirc/anon', rules)
    except:
        print('POST PROCESS ERROR. Secondary scrub not performed.')

    if not flag_vars['ACCESSION']:

        # --- move files into pid folder
        if bool(pid_dict):
            find_pid_modality.convert_acc_to_pid_folders(pid_dict, ANON_ROOT_PATH)

# --- anonymize dicom files using rules based on flags
if not flag_vars['RAW']:

    # --- use custom smaller set of rules for a lighter scrub
    if flag_vars['CUSTOM']:

        if flag_vars['PRIVONLY']:
            
            if flag_vars['SHIFT']:
                shifted_dates_dict = anonymize_dicoms.anonymize(ANON_ROOT_PATH + '/mirc/anon', ANON_ROOT_PATH + '/rules/custom_rules.csv', remove_non_standard=False, shift=True)
            else:
                anonymize_dicoms.anonymize(ANON_ROOT_PATH + '/mirc/anon', ANON_ROOT_PATH + '/rules/custom_rules.csv', remove_non_standard=False, shift=False)

        elif flag_vars['SHIFT']:
            shifted_dates_dict = anonymize_dicoms.anonymize(ANON_ROOT_PATH + '/mirc/anon', ANON_ROOT_PATH + '/rules/custom_rules.csv', remove_non_standard=True, shift=True)
        else:
            anonymize_dicoms.anonymize(ANON_ROOT_PATH + '/mirc/anon', ANON_ROOT_PATH + '/rules/custom_rules.csv', remove_non_standard=True, shift=False)

    # --- remove only private tags
    elif flag_vars['PRIVONLY']:
        print('Keep private tags mode.')
        if flag_vars['SHIFT']:
            shifted_dates_dict = anonymize_dicoms.anonymize(ANON_ROOT_PATH + '/mirc/anon', ANON_ROOT_PATH + '/rules/standard_rules.csv', remove_non_standard=False, shift=True)

        else:
            anonymize_dicoms.anonymize(ANON_ROOT_PATH + '/mirc/anon', ANON_ROOT_PATH + '/rules/standard_rules.csv', remove_non_standard=False, shift=False)

    else:

        # --- determine if date shift functionality will be used
        if flag_vars['SHIFT']:
            shifted_dates_dict = anonymize_dicoms.anonymize(ANON_ROOT_PATH + '/mirc/anon', ANON_ROOT_PATH + '/rules/standard_rules.csv', remove_non_standard=True, shift=True)

        # --- otherwise run normal anonymization
        else:
            anonymize_dicoms.anonymize(ANON_ROOT_PATH + '/mirc/anon', ANON_ROOT_PATH + '/rules/standard_rules.csv', remove_non_standard=True, shift=False)

# --- create spreadsheet mapping PIDs to shifted dates if shifted functionality was used
if flag_vars['SHIFT']:

    # --- read queries and matches
    df_q = pd.read_csv(requestor_path + '/csvs/query_' + DATE + '.csv', index_col=[0])
    df_m = pd.read_csv(requestor_path + '/csvs/matches_' + DATE + '.csv')
    
    # --- create shifted study dates column
    shifted_dates_list = []
    for d in df_m['study_date']:
        
        # --- convert to str
        d = str(d)

        # --- append 
        if d in shifted_dates_dict.keys():
            shifted_dates_list.append(shifted_dates_dict[d])
        else:
            shifted_dates_list.append('None')

    df_q_ = pd.DataFrame(data={'mrn' : df_q['MRN'], 'pid' : df_q['Patient Study ID']})
    df_m_ = pd.DataFrame(data={'mrn' : df_m['mrn'], 'accession' : df_m['accession'], 'study_date' : df_m['study_date'],'shifted_date': shifted_dates_list, 'study_description' : df_m['study_description']})
    df_f = df_m_.merge(df_q_, on='mrn')
    df_f = df_f.drop_duplicates()

    df_f.to_csv(ANON_ROOT_PATH + '/mirc/anon/legend.csv')

# ---------------------------------------------------------
# TRANSFER FINISHED ANONYMIZED DATA TO CAIDM WORKSTATION
# ---------------------------------------------------------

if flag_vars['MOUNT']:
    print('Transferring files...')
    if flag_vars['RAW']:
        subprocess.run('cp -r ' + ANON_ROOT_PATH + '/mirc/anon /data/dicom/mirc_dicoms/mirc_raw_' + DATE + '_' + REQUESTOR, shell=True)
    elif flag_vars['PRIVONLY']:
        subprocess.run('cp -r ' + ANON_ROOT_PATH + '/mirc/anon /data/dicom/mirc_dicoms/mirc_privonly_' + DATE + '_' + REQUESTOR, shell=True)
    elif flag_vars['ACCESSION']:
        subprocess.run('cp -r ' + ANON_ROOT_PATH + '/mirc/anon /data/dicom/mirc_dicoms/mirc_accession_' + DATE + '_' + REQUESTOR, shell=True)
    else:
        subprocess.run('cp -r ' + ANON_ROOT_PATH + '/mirc/anon /data/dicom/mirc_dicoms/mirc_anon_' + DATE + '_' + REQUESTOR, shell=True)
    print('Successfully transferred files.')

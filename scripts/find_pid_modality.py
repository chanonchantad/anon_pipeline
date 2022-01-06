import pandas as pd
import pickle
import sys
import os, glob
from pprint import pprint

# ----------------------------------------------------------------
# main functions
# ----------------------------------------------------------------
def convert_acc_to_pid_folders(acc_to_pid_dict, anon_root):

    # --- get list of accession folders
    acc_folders = glob.glob(os.path.normpath(anon_root) + '/mirc/anon/*')
    
    for acc_path in acc_folders:

        # --- grab the accession from the folder path
        accession = os.path.basename(os.path.normpath(acc_path))

        # --- convert accession to pid
        if accession in acc_to_pid_dict.keys():
            pid = acc_to_pid_dict[accession]

            new_folder_path = os.path.split(acc_path)[0] + '/' + str(pid)
        
            new_acc_path = new_folder_path + '/' + str(accession)

            # --- check if folder exists
            if not os.path.exists(new_folder_path):
                os.makedirs(new_folder_path, exist_ok=True)
            
            try:
                os.rename(acc_path, new_acc_path)
                print("Moved " + str(accession) + " to " + str(pid) + ".")
            except:
                pass
def create_pid_2nd_scrub_dicts(date, requestor, csv_root, anon_root):

        # --- find the matches and query csv files
        request_path = csv_root + str(date) + '/' + str(requestor) + '/csvs/'

        query_path, matches_path = get_query_matches_csv(date, requestor, request_path)

        # --- generate acc to pid dict and output pickle
        acc_to_pid = create_acc_to_pid_dict(query_path, matches_path)

        # --- generate 2nd scrub (PET/NM dict containing paths) and output pickle
        scrub_2nd_dict = create_2nd_scrub_acc_dict(matches_path, anon_root)
        
        return acc_to_pid, scrub_2nd_dict
# -----------------------------------------------------------------
# helper functions
# -----------------------------------------------------------------
def get_query_matches_csv(date, requestor, request_path):
    
    matches_path = request_path + 'matches_' + str(date) + '.csv'
    query_path = request_path + 'query_' + str(date) + '.csv'

    return query_path, matches_path

def create_acc_to_pid_dict(query_path, matches_path):
   
    if os.path.exists(query_path) and os.path.exists(matches_path):
        # --- load both files with pandas
        df_query = pd.read_csv(query_path, dtype={'MRN' : int})
        df_matches = pd.read_csv(matches_path, dtype={'mrn' : int})
    
        # --- create mrn to pid dictionary
        query_mrn_list = list(df_query['MRN'])
        query_pid_list = list(df_query['Patient Study ID'])
        mrn_to_pid = {str(k).replace(" ", ""):v for k,v in zip(query_mrn_list, query_pid_list)}

        # --- create acc to mrn dictionary
        matches_acc_list = list(df_matches['accession'])
        matches_mrn_list = list(df_matches['mrn'])
        acc_to_mrn = {str(k).replace(" ", "") : str(v).replace(" ", "") for k,v in zip(matches_acc_list, matches_mrn_list)}

        # --- create acc to pid dictionary
        acc_to_pid = {k1:mrn_to_pid[v1] for (k1,v1) in acc_to_mrn.items()}

        return acc_to_pid

    else:

        return {}

def create_2nd_scrub_acc_dict(matches_path, anon_root):
    
    legend = {
      #  'OTUS' : 'US',
        'CTOTPT' : 'PT'
    }
    if os.path.exists(matches_path):

        # --- load matches file with pandas
        df_matches = pd.read_csv(matches_path, dtype={'accession' : str})

        # --- create lists of accessions and modality
        accessions = list(df_matches['accession'])
        modalities = list(df_matches['modality'])

        # --- create modality : list of acc  dictionary
        mod_to_acc_dict = {}
        for acc, mod in zip(accessions, modalities):
            
            current_tag = str(mod).upper()
            if current_tag in legend.keys():
                current_tag = legend[current_tag]

            # --- check for PETS and NM
            for tag in ['PT', 'NM', 'US']:
                
                if tag == current_tag:
                    
                    # --- update dict
                    if tag not in mod_to_acc_dict.keys():
                        mod_to_acc_dict[tag] = [os.path.normpath(anon_root) + '/mirc/anon/' + acc]
                    else:
                        mod_to_acc_dict[tag].append(os.path.normpath(anon_root) + '/mirc/anon/' + acc)
        pprint(mod_to_acc_dict)
        return mod_to_acc_dict
    
    else:

        return {}

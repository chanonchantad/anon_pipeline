import pydicom
import datetime
import random
from pydicom.uid import UID
from pydicom.tag import Tag
import pandas as pd
import sys, os
import numpy as np
import time, datetime
from hash import hash
#################################################################
# This file is used to remove or hash tags of interest from 
# dicom files. The tags are fed through a csv file with the columns
# 'Remove' and 'Hash'.
#
# USAGE: python anonymize_dicoms.py <folder path containing dicoms> <csv file path>
#################################################################

MIN_SHIFT_DAYS = -1000
MAX_SHIFT_DAYS = 1000
SALT_PATH = '/data/apps/DICOMPipeline/anon_pipeline/rules/secret'
#################################################################
# Main functions
#################################################################
def anonymize(root_folder_path, csv_file_path, remove_non_standard=True, shift=False):
    """
    Anonymizes all .dcm files within the root folder given.
    
    Parameters:
    root_folder_path - the path to the folder containing all 
                       dicoms to be anonymized.
    """

    # --- create dictionary for shift dates
    shifted_dates_dict = {}

    # --- inform the user of process
    print("Beginning tag removal anonymization process.")

    if shift:
        print("Utilizing date shifting pipeline.")
    
    # --- process tags from csv file to get tags to be removed/hashed
    remove_tag, shift_tag, hashuid_tag, hashptid_tag = process_tags(csv_file_path)
    
    # --- parse the set into tag format
    remove_tag_set = set(remove_tag)
    shift_tag_set = set(shift_tag)
    hashuid_tag_set = set(hashuid_tag)
    hashptid_tag_set = set(hashptid_tag)

    # --- traverse through the root folder, find all dcm files and anonymizes them
    # salt_path = os.path.dirname(os.path.dirname(root_folder_path)) + '/rules/secret'

    counter = 0
    for root, directories, dcm_files in os.walk(root_folder_path): 
        for dcm_file in dcm_files:
            if dcm_file.endswith('.dcm'):
                
                # --- create the full path for the dicom file
                dicom_path = os.path.join(root,dcm_file)
                
                # --- read dicom with pydicom
                data = pydicom.dcmread(dicom_path)
                
                # --- read salt from text file
                file_object = open(SALT_PATH, 'r')
                salt = file_object.readline()

                # --- anonymize and hash tags of the dicom
                anonymize_dicom(data, salt, remove_tag_set, shift_tag_set, hashuid_tag_set, hashptid_tag_set, remove_non_standard, shift, shifted_dates_dict)
                
                # --- save dicom file
                data.save_as(dicom_path)
                
                # --- keep track of how many dicom files have been anonymized
                counter += 1
                print(str(counter) + " dicom files anonymized.", end='\r')
                
    # --- inform the user of success
    print()
    print("Successfully anonymized " + str(counter) + " dicom files.")
    
    if shift:
        return shifted_dates_dict

def anonymize_dicom(dicom, salt, remove_tag_set, shift_tag_set, hashuid_tag_set, hashptid_tag_set, remove_non_standard, shift, shifted_dates_dict):
    """
    Helper function to remove tags given by the list tags_to_anonymize.
    Also removes private tags.
    
    Parameters:
    dicom - the dicom file thats being anonymized
    tags_to_anonymize - a list of tags to anonymize
    """
    # --- use pydicom's built in function to remove private tags
    try:
        if remove_non_standard:
            dicom.remove_private_tags()
    except:
        pass
    
    # --- Change age
    dicom.PatientAge = '119Y'
    
    # --- prefix for hashing UIDs
    prefix = '1.2.840.10008.'
    
    # --- extract patientid to use for shifting dates
    pid = dicom.PatientID
    
    # --- custom
    #if 'InstitutionalDepartmentName' in dicom:
    #    del dicom.InstitutionalDepartmentName
    #if 'RequestAttributesSequence' in dicom:
    #    del dicom.RequestAttributesSequence
    #if 'ReferencedImageSequence' in dicom:
    #    del dicom.ReferencedImageSequence

    # --- iterrate through dicom and remove tags
    for tag in dicom:

        # --- get current tag
        current_tag = tag.tag
        
        # --- check if it is a tag in hashptid
        if str(current_tag) in hashptid_tag_set:
            dicom[current_tag].value = hash(dicom[current_tag].value)
            
        # --- check if it is a tag in hashuid
        elif str(current_tag) in hashuid_tag_set:
            suffix = str(int(hash(str(dicom[current_tag].value) + salt), 16))
            dicom[current_tag].value = prefix + suffix

        # --- check if it is a tag in date shift
        elif str(current_tag) in shift_tag_set:

            if shift:
                current_date = dicom[current_tag].value
                
                if current_date not in shifted_dates_dict.keys():

                    try:
                        shifted_date = shift_date(dicom, current_tag, salt, pid)
                        dicom[current_tag].value = shifted_date
                        shifted_dates_dict[current_date] = shifted_date
                    except:
                        pass
                else:
                    dicom[current_tag].value = shifted_dates_dict[current_date]

        # --- check if it is a tag in remove
        elif str(current_tag) in remove_tag_set:
            dicom[current_tag].value = ''
            
def anonymize_private_tags_only(root_folder_path):
    """
    Removes only private tags from all dicoms

    Parameters:
    root_folder_path - the path to the folder containing all 
                       dicoms to be anonymized.
    """
    # --- inform the user of process
    print("Beginning private tag removal anonymization process.")
    
    counter = 0
    for root, directories, dcm_files in os.walk(root_folder_path): 
        for dcm_file in dcm_files:
            if dcm_file.endswith('.dcm'):
                
                # --- create the full path for the dicom file
                dicom_path = os.path.join(root,dcm_file)
                
                # --- read dicom with pydicom
                data = pydicom.dcmread(dicom_path)
                
                # --- remove private tags
                data.remove_private_tags()

                # --- save dicom file
                data.save_as(dicom_path)
                
                # --- keep track of how many dicom files have been anonymized
                counter += 1
                print(str(counter) + " dicom files private tags removed.", end='\r')
                
    # --- inform the user of success
    print()
    print("Successfully anonymized " + str(counter) + " dicom files.")
               
#################################################################
# Helper functions
#################################################################

def process_tags(tag_csv_file):
    """
    Processes an excel file and returns several lists for tags to be removed or hashed.
    
    Returns:
    remove - a list of tags to be removed by pydicom
    to_hash - a list of tags to be hashed by pydicom
    """
    
    df = pd.read_csv(tag_csv_file, dtype={'remove_tag': str, 'shift_tag': str, 'hashuid_tag': str, 'hashptid_tag' : str})

    remove_tags = set(df['remove_tag']) 
    
    shift_tags = set(filter(lambda x : not(pd.isnull(x)), df['shift_tag']))
    
    hashuid_tags = set(filter(lambda x : not(pd.isnull(x)), df['hashuid_tag']))

    hashptid_tags = set(filter(lambda x : not(pd.isnull(x)), df['hashptid_tag']))

    return remove_tags, shift_tags, hashuid_tags, hashptid_tags

def shift_date(dicom, current_tag, salt, pid):
    
    # --- extract date value
    date = dicom[current_tag].value
    
    # --- get number of days to shift
    random.seed(str(pid) + salt)
    days_to_shift = random.randint(MIN_SHIFT_DAYS, MAX_SHIFT_DAYS)

    # --- convert date to datetime and add
    date = datetime.datetime.strptime(date, '%Y%m%d')
    shifted_date = date + datetime.timedelta(days=days_to_shift)
    shifted_date = shifted_date.strftime('%Y%m%d')

    return shifted_date

if __name__ == '__main__':
    
    if len(sys.argv) > 2:
        root_folder_path = sys.argv[1]
        csv_file_path = sys.argv[2]

        if len(sys.argv) > 3:

            flag = sys.argv[3]

            if flag == '-s':
                anonymize(root_folder_path, csv_file_path, remove_non_standard=True, shift=True)
        else:
            anonymize(root_folder_path, csv_file_path)
    
    else:
        print("Incorrect usage.")
        print("Usage: python anonymize_dicoms.py <root_folder> <csv_file>")
    

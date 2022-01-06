# =========================================================================================
# This file has two main parts: Sorting the dicoms into accessions and then quarantine
# files based on the rules provided.
#
# Written by Dr. Peter Chang
# https://github.com/peterchang77/
#
# Modified by Chanon Chantaduly
# https://github.com/chanonchantad/
# =========================================================================================

import glob, os, shutil
import sys
import pydicom

# =========================================================================================
# Removes MRNs and sorts dicoms into accessions 
# =========================================================================================

def sort_dcms(root):
    """
    Method to sort DICOMs into the following structure:

      .../accession/seriesUID/instanceUID.dcm

    """
    sort_root = root + '/flat'
    dcms = glob.glob('%s/**/*.dcm' % sort_root, recursive=True)
    for n, dcm in enumerate(dcms):
        try:
            path = create_path(dcm)
            move_file(src=dcm, dst=path, root=root)
            print('%07i/%07i: Sorting DICOMs' % (n + 1, len(dcms)), end='\r')
        except:
            pass

    print('%07i: DICOMs finished sorting' % n)

    return dcms

def create_path(dcm):

    d = pydicom.read_file(dcm)
    
    return '%s/%s/%s.dcm' % (
        d.AccessionNumber,
        d.SeriesInstanceUID,
        d.SOPInstanceUID)

def move_file(src, dst, root):
    
    move_root = root + '/sorted'

    # Make directories
    os.makedirs('%s/%s' % (move_root, dst.split('/')[0]), exist_ok=True)
    os.makedirs('%s/%s/%s' % (move_root, dst.split('/')[0], dst.split('/')[1]), exist_ok=True)

    # Move 
    shutil.move(src=src, dst='%s/%s' % (move_root, dst))

def summarize(root='/data/dicom/mirc_sorted'):
    """
    DEPRECATED FUNCTION
    """
    series = glob.glob('%s/*/*/' % root)
    for s in series:
        dcms = glob.glob('%s*.dcm' % s)
        print('%s: %03i files' % (s, len(dcms)))

# ========================================================================
# Checks to see which dicom files to keep/quarantine
# ========================================================================

def check_small_series(dcm, path, N=10):
    """
    Method to check if DICOM has less than N number of images in series

    """
    dcms = glob.glob('%s/*.dcm' % os.path.dirname(path))

    return len(dcms) <= N

def check_no_pixel_array(dcm, path):
    """
    Method to check if DICOM as no pixels

    """
    return hasattr(dcm, 'pixel_array') == False

def check_secondary_capture(dcm, path):
    """
    Method to check if DICOM is a secondary capture

    """
    status = False
    if hasattr(dcm, 'SOPClassUID'):
        status = dcm.SOPClassUID == '1.2.840.10008.5.1.4.1.1.7'
    elif hasattr(dcm, 'MediaStorageSOPClassUID'):
        status = dcm.MediaStorageSOPClassUID == '1.2.840.10008.5.1.4.1.1.7'
    elif hasattr(dcm, 'ImageType'):
        content = str(dcm.ImageType)
        status = content.find('SECONDARY') > -1
    return status

def check_burned_annotation(dcm, path):
    """
    Method to check if DICOM has 'BurnedAnnotation' flag
    """
    status = False
    if hasattr(dcm, 'BurnedAnnotation'):
        status = dcm.BurnedAnnotation == 'YES'

    return status

def check_rgb(dcm, path):
    """
    Method to check if DICOM is RGB file

    """
    status = False
    if hasattr(dcm, 'pixel_array'):
        status = dcm.pixel_array.ndim == 3 

    return status

def check_desc(dcm, path):
    """
    Method to remove based on description
    """
    
    quar_set = {'DOSE REPORT'}

    status = False
    if hasattr(dcm, 'SeriesDescription'):
        
        desc = dcm.SeriesDescription.upper()
        
        if desc in quar_set:
            status = True

    return status
# ===============================================================
# DEFINE RULES
# ===============================================================
# 
# 1. For any rule to ignore, set RULES['use'] dictionary to False
# 2. For modality specific rules, define corresponding RULES dict 
# 
# ===============================================================

RULES = {}

RULES['use'] = {
    'small_series': False,
    'no_pixel_array': True,
    'secondary_capture': True,
    'burned_annotation': True,
    'rgb': True,
    'desc' : True
}

RULES['CT'] = {
    'small_series': check_small_series,
    'no_pixel_array': check_no_pixel_array,
    'secondary_capture': check_secondary_capture,
    'burned_annotation': check_burned_annotation,
    'rgb': check_rgb,
    'desc' : check_desc
}

RULES['MR'] = {
    'small_series': check_small_series,
    'no_pixel_array': check_no_pixel_array,
    'secondary_capture': check_secondary_capture,
    'burned_annotation': check_burned_annotation,
    'rgb': check_rgb,
    'desc': check_desc
}

RULES['PT'] = {
    'small_series': check_small_series,
    'no_pixel_array': check_no_pixel_array,
    'desc': check_desc
}


RULES['CTOTPT'] = {
    'small_series': check_small_series,
    'no_pixel_array': check_no_pixel_array,
    'desc': check_desc
}


RULES['OT'] = {
    'small_series': check_small_series,
    'no_pixel_array': check_no_pixel_array,
    'desc': check_desc
}

RULES['CR'] = {
    'no_pixel_array': check_no_pixel_array,
    'secondary_capture': check_secondary_capture,
    'burned_annotation': check_burned_annotation,
    'rgb': check_rgb         
}

RULES['US'] = {
    'no_pixel_array': check_no_pixel_array,
    'secondary_capture': check_secondary_capture,
}
RULES['XA'] = {
    'no_pixel_array': check_no_pixel_array,
    'secondary_capture': check_secondary_capture,
}
RULES['XR'] = {}
RULES['NM'] = {}
RULES['SR'] = {}

# ===============================================================
# RUN ANONYMIZATION 
# ===============================================================

def run(root, log_name='anon.txt'):
    
    # --- modify root path
    root = root + '/sorted'

    PATH_QUARANTINE = '%s/quarantine' % os.path.dirname(root)
    PATH_ANON = '%s/anon' % os.path.dirname(root)
    PATH_LOGS = '%s/logs' % os.path.dirname(root)
    os.makedirs(PATH_QUARANTINE, exist_ok=True)
    os.makedirs(PATH_ANON , exist_ok=True)
    os.makedirs(PATH_LOGS, exist_ok=True)

    # --- Open log file
    log_path = '%s/%s' % (PATH_LOGS, log_name)
    print('Saving logs to: %s' % log_path)
    log_file = open(log_path, 'w')

    # --- Find all DICOMs
    dcms = glob.glob('%s/*/*/*.dcm' % root)

    # --- Create lists
    move_to_quar = []
    move_to_anon = []

    # --- Apply rules and move folders to anon/quarantine 
    for count, d in enumerate(dcms):
        print('Checking rules: %06i/%06i' % (count + 1, len(dcms)), end='\r')

        acc, series = d.split('/')[-3:-1]
        try:
            dcm = pydicom.read_file(d)
            
            if dcm is not None:

                if hasattr(dcm, 'Modality'):
                    modality = dcm.Modality
                    if modality in RULES:
                        quar = False
                        for name, func in RULES[modality].items():
                            if RULES['use'][name]:

                                # --- Move to quarantine 
                                quar = func(dcm, d)
                                if quar:
                                    log_file.write('QUAR: %s | %s\n' % (acc, d))
                                    dst = '%s/%s/%s' % (PATH_QUARANTINE, acc, series)
                                    os.makedirs(dst, exist_ok=True)
                                    move_to_quar.append((d, dst))
                                    # shutil.move(src=d, dst=dst)
                                    break

                        # -- Move to anon 
                        if not quar:
                            log_file.write('ANON: %s | %s\n' % (acc, d))
                            dst = '%s/%s/%s' % (PATH_ANON, acc, series)
                            os.makedirs(dst, exist_ok=True)
                            move_to_anon.append((d, dst))
                            # shutil.move(src=d, dst=dst)

                    else:
                        log_file.write('ERRS: %s | %s | modality not defined (%s)\n' % (acc, d, modality))
                else:
                    log_file.write('ERRS: %s | %s | modality header not in DICOM\n' % (acc, d))

        except:
            log_file.write('ERRS: %s | %s | pydicom cannot open DICOM \n' % (acc, d))
    print('Checking rules complete                                                             ')

    # --- Move the files
    for n, pair in enumerate(move_to_quar + move_to_anon):
        print('Moving files: %06i%06i' % (n + 1, len(move_to_quar) + len(move_to_anon)), end='\r')
        src, dst = pair
        shutil.move(src=src, dst=dst)
    print('Moving files complete                                                                ')

    log_file.close()

    return dcms

def makedirs(path, root):
    """
    Method to make /accession/SeriesUID/ directory structure in root

    """
    acc, series = path.split('/')[-3:-1]
    os.makedirs('%s/%s' % (root, acc), exist_ok=True)
    os.makedirs('%s/%s/%s' % (root, acc, series), exist_ok=True)

def sort(root):

    # --- run sorting step
    dcms = sort_dcms(root=root)

    # --- run quarantine step
    dcms = run(root=root, log_name='anon.txt')
    
if __name__ == '__main__':
    
    # --- extract root path from arguments
    assert len(sys.argv) == 2

    root = sys.argv[1]

    # --- run sorting step
    dcms = sort_dcms(root=root)

    # --- run quarantine step
    dcms = run(root=root, log_name='anon.txt')

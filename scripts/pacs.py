# ------------------------------------------------------------------
# PACS code written by Dr. Peter Chang
# https://github.com/peterchang77/
# ------------------------------------------------------------------

import gdcm, pandas as pd

# ================================================================== 
# GLOBAL VARIABLES
# ================================================================== 

TAGS_SORTED = ['mrn', 'accession', 'study_date', 'modality', 'study_description', 
    'history', 'setting', 'location', 'age', 'referrer', 
    'body_part', 'studyUID', 'seriesUID']

TAGS = {
    'modality': [0x0008, 0x0060],
    'location': [0x0038, 0x0300],
    'study_date': [0x0008, 0x0020],
    'study_description': [0x0008, 0x1030],
    'mrn': [0x0010, 0x0020],
    'referrer': [0x0008, 0x0090],
    'age': [0x0010, 0x1010],
    'setting': [0x0010, 0x1081],
    'history': [0x0010, 0x21b0],
    'accession': [0x0008, 0x0050],
    'body_part': [0x0018, 0x0015],
    'studyUID': [0x0020, 0x000d],
    'seriesUID': [0x0020, 0x000e]}

TAGS_GDCM = lambda key : gdcm.Tag(*TAGS[key])

# --- Template query dictionary
query = {
    'modality': '*',
    'location': '*',
    'study_date': '*',
    'study_description': '*',
    'mrn': '*',
    'referrer': '*',
    'age': '*',
    'setting': '*',
    'history': '*',
    'accession': '*',
    'body_part': '*',
    'studyUID': '*',
    'seriesUID': '*'}

def set_tag(ds, tag, value):
    """
    Set tag with value and insert into ds

    """
    de = gdcm.DataElement(TAGS_GDCM(tag))
    de.SetByteValue(value, gdcm.VL(len(value)))
    ds.Insert(de)

    return ds

# ================================================================== 
# METHODS 
# ================================================================== 

def perform_find(configs, query={}, verbose=[], max_results=20, csv_file=None, results=None):
    """
    Method to perform PACS query

    Note that configs and query are required parameters. The remainder are optional.

    :params

      (dict) configs : a configuration dictionary that matches the current (local) machine

        Note that several global configs variables are predefined at the bottom of this module:

        * Use configs_mac if using the Mac Pro
        * Use configs_exx if using the Exxact box server

      (dict) query : a query dictionary; to create this dictionary,

        (1) Copy the template query global variable defined at the top of this module
        (2) Note that currently all fields are values are set to wildcards (*)
        (3) Replace any value field with a specific query

      (list) verbose : if provided, print the selected key values returned by search (e.g. ['mrn', 'age'...])
      (int) max_results : if verbose list is provided, this value represents the max number of results to print 
      (str) csv_file : if provided, save results to provided path to *.csv file
      (dict) results : if provided, append current query result to existing results

    :return

      (dict) results

    """
    # --- Configure query
    if results is None:
        results = {}
    ds = gdcm.DataSet()
    for tag, value in query.items():
        ds = set_tag(ds=ds, tag=tag, value=value)
        if tag not in results:
            results[tag] = []

    if 'studyUID' not in query:
        ds = set_tag(ds=ds, tag='studyUID', value='*')
        query['studyUID'] = '*'

    if 'studyUID' not in results:
        results['studyUID'] = []

    # --- Configure query object 
    # --- use 'study' instead of gdcm.ePatientRootType
    cnf = gdcm.CompositeNetworkFunctions()
    theQuery = cnf.ConstructQuery(gdcm.ePatientRootType, gdcm.eStudy, ds)

    # --- Prepare the variable for output
    ret = gdcm.DataSetArrayType()
    
    # --- Perform query
    if len(verbose) > 0: print('Performing C-FIND...')
    cnf.CFind(configs['ip'], configs['port_called'], theQuery, ret, configs['aet_calling'], configs['aet_called'])

    # --- Save/print output
    if len(verbose) > 0: print('A total of %i matches found' % len(ret))

    # --- Print results and save to dictionary 
    for i in range(len(ret)):

        output = ''
        for tag in query:
            value = str(ret[i].GetDataElement(TAGS_GDCM(tag)).GetValue())
            results[tag].append(value)
            if tag in verbose:
                output = '%s | %s' % (output, value)

        if i < max_results and len(verbose) > 0:
            print('%03i / %03i%s' % (i + 1, max_results, output))

    # --- Create dictionary
    if csv_file is not None:
        df = pd.DataFrame(results)
        df = df[[t for t in TAGS_SORTED if t in query]]
        df.to_csv(csv_file, index=False)
        if len(verbose) > 0: print('Results written to %s' % csv_file)

    return results

def perform_sort(root=None):
    """
    Method to sort raw Horos noindex dump into symlinked folders with the following structure
    
        .../[root]/raw/[AccessionNumber]/[SeriesInstanceUID]/[SOPInstanceUID].dcm

    Note, this is only used for Mac-based imports (Horos) or to generically sort a dump of DICOM objects.

    :params

      (str) root: root from which to find DICOM files; if empty will assume from Horos
    
        .../[Horos Data]/[index]/*.dcm (default of Horos dump files) 

    """
    if horos:
        dcms = glob.glob('%s/*/*.dcm' % self.configs['destination'])

    else:
        dcms = glob.glob('%s/**/*.dcm' % root, recursive=True)

    for dcm in dcms:

        d = dicom.read_file(dcm)

def perform_move(configs, query={}, verbose=False):

    # --- Configure query
    ds = gdcm.DataSet()
    for tag, value in query.items():
        ds = set_tag(ds=ds, tag=tag, value=value)

    # --- Configure query object 
    cnf = gdcm.CompositeNetworkFunctions()
    theQuery = cnf.ConstructQuery(gdcm.ePatientRootType, gdcm.eStudy, ds, True)

    # --- Perform move
    if verbose: print('Performing C-MOVE...')
    result = cnf.CMove(configs['ip'], configs['port_called'], theQuery, configs['port_calling'],\
            configs['aet_calling'], configs['aet_called'], configs['destination'])
    if verbose: print('Operation complete')

# =========================================================================
# TEST SERVERS | CONFIGURATIONS
# =========================================================================

# http://www.pixelmed.com/publicdicomserver.html
configs_pixelmed = {
    'ip': '184.73.255.26',
    'port_called': 11112,
    'port_calling': 11112,
    'aet_called': 'AWSPIXELMEDPUB',
    'aet_calling': 'AWSPIXELMEDPUB'
}


# http://www.dicomserver.co.uk/
configs_uk = {
    'ip': 'www.dicomserver.co.uk',
    'port_called': 104,
    'port_calling': 104,
    'aet_called': 'aet_metis',
    'aet_calling': 'aet_metis',
    'destination': '/home/exx/Downloads/'
}

query_uk = {
    'studyUID': '1.2.826.0.1.3680043.8.1055.1.20111103112244831.40200514.30965937'
}

# perform_find(configs_uk, query_uk)
# perform_move(configs_uk, query_uk)

# =========================================================================
# UCI SERVERS | CONFIGURATIONS
# =========================================================================

configs_mac = {
    'ip': '160.87.183.15',
    'port_called': 2104,
    'port_calling': 11112,
    'aet_called': 'Horos_DCM',
    'aet_calling': 'ANY-SCP',
    'destination': '/Users/danielchow/Documents/Horos Data/DATABASE.noindex'
}

configs_exx = {
    # 'ip': '160.87.25.189',
    'ip': '160.87.183.15',
    'port_called': 2104,
    'port_calling': 11112,
    'aet_called': 'CHOW_LINUX',
    'aet_calling': 'CHOW_LINUX',
    'destination': '/home/exx/export'
}

configs_fs1 = {
    # 'ip': '160.87.25.45',
    'ip': '160.87.183.15',
    'port_called': 2104,
    'port_calling': 11112,
    'aet_called': 'CAIDM_F01',
    'aet_calling': 'CAIDM_F01',
    'destination': '/data/dicom/raw'
}

configs_vis = {
    'ip': '160.87.136.67',
    'port_called': 104,
    'port_calling': 11112,
    'aet_called': 'AWS_QUERY',
    'aet_calling': 'CAIDM_F02',
    'destination': '/data/dicom/raw'
}

configs_vm1 = {
    'ip': '160.87.183.15',
    'port_called': 2104,
    'port_calling': 11112,
    'aet_called': 'CAIDM_F02',
    'aet_calling': 'CAIDM_F02',
    'destination': '/data/dicom/raw'
}

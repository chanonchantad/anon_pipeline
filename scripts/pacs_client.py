# ------------------------------------------------------------------
# PACS code written by Dr. Peter Chang
# https://github.com/peterchang77/
# ------------------------------------------------------------------

import os, glob, shutil, pydicom, pandas as pd, pickle
import pacs

class Client():

    def __init__(self, root, configs='vm1'):

        self.root = root

        assert configs in ['exx', 'mac', 'fs1', 'vm1', 'vis']

        if configs == 'exx':
            self.configs = pacs.configs_exx

        if configs == 'mac':
            self.configs = pacs.configs_mac

        if configs == 'fs1':
            self.configs = pacs.configs_fs1

        if configs == 'vm1':
            self.configs = pacs.configs_vm1

        if configs == 'vis':
            self.configs = pacs.configs_vis

    def perform_find(self, suffix=''):
        """
        Method to perform a series of C-FIND based on input *.csv files and 
        save results into various output *.csv files:

          (1) All matches in root/csvs/matches.csv
          (2) All exclude in root/csvs/exclude.csv (do not match any secondary filters)
          (3) All missing in root/csvs/missing.csv (do not match any initial C-FIND query)

        Note that parse_csv() method should be overloaded to handle unique
        input *.csv formatting

        :params

          (str) suffix : suffix to [root]/csvs/query_[suffix].csv query file 

        """
        results = {'mrn': []}
        missing = dict([(k, []) for k in pacs.TAGS_SORTED]) 
        csv_file = '%s/csvs/query_%s.csv' % (self.root, suffix)
        queries = self.parse_csv(csv_file)
        indices = []

        for n, query in enumerate(queries):

            print('Perform C-FIND %04i / %04i' % (n + 1, len(queries)), end='\r')
            results_len = len(results['mrn'])
            results = pacs.perform_find(configs=self.configs, query=query, results=results)
            results_N = len(results['mrn']) - results_len 
            indices += [n] * results_N
            if results_N == 0:
                for tag, value in query.items():
                    missing[tag].append(value)

        matches, exclude = self.filter_query(results, indices, csv_file)

        # --- Write *.csv files
        os.makedirs('%s/csvs' % self.root, exist_ok=True)
        self.write_csv(matches, pacs.TAGS_SORTED, '%s/csvs/matches_%s.csv' % (self.root, suffix)) 
        self.write_csv(exclude, pacs.TAGS_SORTED, '%s/csvs/exclude_%s.csv' % (self.root, suffix))
        self.write_csv(missing, pacs.TAGS_SORTED, '%s/csvs/missing_%s.csv' % (self.root, suffix))

        print('A total of %i studies found based on %i queries' % (len(matches['mrn']), len(queries)))

    def write_csv(self, data, columns, csv_name):

        df = pd.DataFrame(data)
        df = df[columns]
        df.to_csv(csv_name, index=False)

    def parse_csv(self, fname):
        """
        Method to parse *.csv file and return a list of queries

        Note that queries take the form of dictionary, the template of which
        can be found in pacs.query

        """
        return []

    def filter_query(self, results, queries, csv_file):
        """
        Method to further filter query results based on DICOM headers

        The final output will be divided into two dictionaries:

          (1) matches : all studies matching filters
          (2) exclude : all remaining studies that do not match filters

        """
        return {}, {}

    def perform_sort(self, root=None):
        """
        Method to sort raw Horos noindex dump into symlinked folders with the following structure
        
            .../[root]/raw/[PatientID]/[AccessionNumber]/[SeriesInstanceUID]/[SOPInstanceUID].dcm

        Note, this is only used for Mac-based imports (Horos) or to generically sort a dump of DICOM objects.

        :params

          (str) root: root from which to find DICOM files; if empty will assume from Horos
        
            .../[Horos Data]/[index]/*.dcm (default of Horos dump files) 

        """
        if root is None:
            dcms = glob.glob('%s/*/*.dcm' % self.configs['destination'])

        else:
            dcms = glob.glob('%s/**/*.dcm' % root, recursive=True)

        errors = []
        for n, dcm in enumerate(dcms):

            print('Scanning files %08i / %08i' % (n + 1, len(dcms)), end='\r')
            try:
                d = pydicom.read_file(dcm)
                fname = '%s/raw' % self.root
                if 'PatientID' in d and 'AccessionNumber' in d and 'SeriesInstanceUID' in d:
                    complete_name = True
                    for suffix in['PatientID', 'AccessionNumber', 'SeriesInstanceUID']:
                        s = getattr(d, suffix)
                        if len(s) > 0:
                            fname = '%s/%s' % (fname, s)
                            if not os.path.exists(fname):
                                os.makedirs(fname, exist_ok=True)
                        else:
                            complete_name = False

                    if complete_name:
                        suffix = d.SOPInstanceUID if 'SOPInstanceUID' in d else str(hash(dcm))
                        fname = '%s/%s.dcm' % (fname, suffix)

                        if not os.path.exists(fname):
                            os.symlink(src=dcm, dst=fname)
                else:
                    errors.append(dcm)

            except:
                errors.append(dcm)

        print('Total of %i DICOM objects did not contain required headers' % len(errors))
        pickle.dump(errors, open('%s/raw/errors.pickle' % self.root, 'wb'))

    def perform_move(self, root=None, suffix='', overwrite=False, slices=None):
        """
        Method to perform a series of C-MOVE operations based on studies
        recorded in root/csvs/matches.csv file

        :params

          (str) root : location of sorted downloaded files; if None, will use the default self.configs['destination']
          (bool) overwrite : if False, will check for existence of output first before C-MOVE

        """
        matches = '%s/csvs/matches_%s.csv' % (self.root, suffix)
        if not os.path.exists(matches):
            print('Error matches.csv does not exist')
            return

        root = self.configs['destination'] if root is None else root
        df = pd.read_csv(matches)

        # --- Create list of missing studyUIDs
        if not overwrite:
            studyUIDS = []
            for studyUID, mrn, acc in zip(df['studyUID'], df['mrn'], df['accession']):
                src = '%s/%s/%s' % (root, mrn, str(acc).strip())
                if not os.path.exists(src):
                    studyUIDS.append(studyUID)

        else:
            studyUIDS = df['studyUID']

        if slices is None:
            slices = slice(0, len(studyUIDS) + 1)

        for n, studyUID in enumerate(studyUIDS[slices]):
            print('Perform C-MOVE %04i / %04i' % (n + 1, len(studyUIDS)), end='\r')
            pacs.perform_move(configs=self.configs, query={
                'studyUID': studyUID})

    def move_dicoms(self, root=None, suffix='', summary_only=False):
        """
        Method to move all studies in root/csvs/matches.csv file from
        the export location to the root folder

        :params

          (str) root : location of sorted downloaded files; if None, will use the default self.configs['destination']
          (bool) summary_only : if True, provide only summary of download % without moving

        """
        matches = '%s/csvs/matches_%s.csv' % (self.root, suffix)
        if not os.path.exists(matches):
            print('Error matches.csv does not exist')
            return

        root = self.configs['destination'] if root is None else root

        df = pd.read_csv(matches)
        n = 0
        for mrn, acc in zip(df['mrn'], df['accession']):
            acc = str(acc).strip()
            src_root = '%s/%s/%s' % (root, mrn, acc)
            print('Downloaded: %04i | Checking %s' % (n, src_root), end='\r')
            if os.path.exists(src_root):
                n += 1
                if not summary_only:

                    dst_root = '%s/dicoms/%s/%s' % (self.root, mrn, acc)

                    for r, dirs, files in os.walk(src_root):
                        series = os.path.basename(r)
                        os.makedirs('%s/%s' % (dst_root, series), exist_ok=True)
                        for f in files:
                            src = '%s/%s' % (r, f)
                            dst = '%s/%s/%s' % (dst_root, series, f)
                            shutil.move(src=src, dst=dst)

        print('\nA total of %04i out of %04i studies downloaded' % (n, len(df['mrn'])))
    
    def summary(self):

        accs = glob.glob('%s/dicoms/*/*' % self.root)
        dcms = glob.glob('%s/dicoms/*/*/*/*.dcm' % self.root)
        sizes = [os.path.getsize(d) for d in dcms]
        total = sum(sizes) / 1e9

        print('A total of %i studies (%i DICOMs | %.3f GiB) downloaded' % (len(accs), len(dcms), total))

    def remove_empty_dirs(self, suffix):
        """
        Method to remove empty directories in export

        """
        matches = '%s/csvs/matches_%s.csv' % (self.root, suffix)
        if not os.path.exists(matches):
            print('Error matches.csv does not exist')
            return

        df = pd.read_csv(matches)
        n = 0
        for mrn, acc in zip(df['mrn'], df['accession']):
            acc = str(acc).strip()
            src_root = '%s/%s/%s' % (self.configs['destination'], mrn, acc)
            if os.path.exists(src_root):
                dcms = glob.glob('%s/*/*.dcm' % src_root)
                if len(dcms) == 0:
                    print('Removing: %s' % src_root)
                    shutil.rmtree(src_root)

# ------------------------------------------------------------------
# PACS code written by Dr. Peter Chang
# https://github.com/peterchang77/
#
# Modified by: Chanon Chantaduly
# https://github.com/chanonchantad/
# ------------------------------------------------------------------

import argparse, glob
import pacs_client
import sys

def count_lines(fname):

    with open(fname) as f:
        for i, l in enumerate(f):
            pass

    return i

def main(root, mode='download'):

    # --- Find suffix
    matches_files = glob.glob(root + '/csvs/matches_*.csv')

    if len(matches_files) != 1:
        print('Error, incorrect number of mathces CSV files present in csvs/')

    else:

        suffix = matches_files[0].split('/')[-1][:-4]
        suffix = suffix.replace('matches_', '')

        # --- Create client
        client = pacs_client.Client(root=root, configs='vm1')

        # --- Run
        if mode == 'download':

            lines = count_lines(matches_files[0])
            if input('A total of %i exams to be downloaded, please confirm by typing this number: ' % lines) == str(lines):
                client.perform_move(suffix=suffix)

        # --- Count
        elif mode == 'count':
            client.move_dicoms(suffix=suffix, summary_only=True)

        elif mode == 'move':
            if input('This command will move all DICOM objects to local folder; are you sure all files have been downloaded (y/n)? ') == 'y':
                client.move_dicoms(suffix=suffix, summary_only=False)

        elif mode == 'rmdir':
            client.remove_empty_dirs(suffix=suffix)

        elif mode == 'summary':
            client.summary()

if __name__ == '__main__':
    
    if len(sys.argv) == 3:
        root = sys.argv[2]
    else:
        root = '.'
    
    mode = 'download'
    main(root=root, mode=mode)

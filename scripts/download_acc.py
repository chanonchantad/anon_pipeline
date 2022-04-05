import pandas as pd
import pacs, pacs_client
import sys
import os, glob
CONFIGS = 'vis'
class Client(pacs_client.Client):

    def parse_csv(self, fname):
        """
        Method to parse *.csv file and return a list of queries

        Note that queries take the form of dictionary, the template of which
        can be found in pacs.query

        """
        queries = []
        df = pd.read_csv(fname, dtype={'Accession': object})
        for n, row in df.iterrows():
            q = pacs.query.copy()
            
            # --- fix spaces 
            accession = row['Accession'].replace(" ", "")

            # --- capitalize
            accession = accession.upper()
            
            # --- update the query dictionary with clean accession
            q['accession'] = accession
            queries.append(q)

        #import ipdb; ipdb.set_trace()

        return queries

    def filter_query(self, results, query, csv_file):

       matches = results
       exclude = {key:[] for key in matches.keys()}

       return matches, exclude

   
def main(root='.', argv = None):
    suffix = sys.argv[1] if len(sys.argv) > 1 else  ""
    client = Client(root=root, configs=CONFIGS)
    client.perform_find(suffix=suffix)
 
if __name__ == '__main__':
    # ===============================================================
    # INITIALIZE CLIENT
    # ===============================================================

    # --- check that there is one argument
    if len(sys.argv) == 3:
        folder_path = sys.argv[2]
        if not os.path.exists(folder_path):
            raise ValueError("Invalid folder path. Doesn't exist.")
        else:
            root = os.path.normpath(folder_path)
            main(root=root, configs=CONFIGS)
    else:
        raise ValueError("Usage: python download_acc.py <suffix of csv query file> <path_to_folder \"/<date>/name/\" that contains query_<date>.csv")

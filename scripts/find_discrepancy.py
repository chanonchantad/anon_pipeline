import pandas as pd
import sys

def find_discrepancy(requestor, request_date, root):
    """
    Compares query and matches to find what was not found in query.
    Assumes path: root + request_date + requestor + /csvs/ + query_ + request_date + .csv
    """

    query = pd.read_csv(root + request_date + '/' + requestor + '/csvs/query_' + request_date + '.csv')
    query = query.reindex()
    matches = pd.read_csv(root + request_date + '/' + requestor + '/csvs/matches_' + request_date + '.csv')

    for mrn, study_date in zip(matches['mrn'], matches['study_date']):

        # --- filter matches by mrn
        new_query = query[query['MRN'] == mrn]

        # --- filter by date
        study_date = pd.to_datetime(study_date, format='%Y%m%d')
        study_date = study_date.strftime('%Y-%m-%d')
        new_query = new_query[new_query['Date of Scan'] == study_date]
        
        # --- remove from original
        indicies = list(new_query.index)
        if len(indicies) > 0:
            query = query.drop(index=indicies)
    
    # --- output to csv
    query.to_csv(root + request_date + '/' + requestor + '/csvs/discrepancy_' + request_date + '.csv')

if __name__ == '__main__':
    
    # --- parse user input
    if len(sys.argv) == 4:
        request_date = sys.argv[1]
        requestor = sys.argv[2]
        root = sys.argv[3]

        # --- find discrepancy and output csv
        find_discrepancy(requestor, request_date, root)
    else:
        print("Usage: python find_discrepancy.py <request date> <requestor> <root>")

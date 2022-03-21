import pandas as pd
import pacs, pacs_client

# --- sets which PACS server to query from
CONFIGS = 'vis'
class Client(pacs_client.Client):

    def parse_csv(self, fname):
        """
        Method to parse *.csv file and return a list of queries

        Note that queries take the form of dictionary, the template of which
        can be found in pacs.query

        """
        legend = {
            'MRN': 'mrn',
            'Date of Scan': 'study_date',
            'Type of Exam': 'modality'}

        parse_func = {
            'MRN': self.parse_mrn,
            'Date of Scan': self.parse_date,
            'Type of Exam': self.parse_modality}

        queries = []
        df = pd.read_csv(fname)
        for n, row in df.iterrows():
            q = pacs.query.copy()
            for key in legend:
                q[legend[key]] = parse_func[key](row[key])
            queries.append(q)

        #import ipdb; ipdb.set_trace()

        return queries

    def parse_mrn(self, value):
        truevalues = str(value).split('.')
        truevalues = [str(v) for v in truevalues]
        return str('*'+ truevalues[0] + '*') 

    def parse_date(self, value):
        values = value.split('-')
        values = [int(v) for v in values]
        return '%04i%02i%02i' % (values[0], values[1], values[2])
      #  return '20%02i%02i%02i' % (int(values[2]), int(values[0]), int(values[1]))

    def parse_modality(self, value):
        legend = {
                'PET' : 'PT',
                'MRI' : 'MR',
                'CTA' : 'CT',
                'X-RAY' : 'CR'
                }
        modality = str(value.split(' ')[0].upper())       
        
        if modality in legend.keys():
            modality = legend[modality]

        endvalue =  '*' + modality + '*'
        POSSIBILITIES = ['*MR*', '*CT*','*CR*', '*X*','*X-RAY*','*PT*', '*MG*', '*US*']
        if endvalue not in POSSIBILITIES:
             endvalue = '*NM*'
       
        return endvalue

    def filter_query(self, results, indices, csv_file):
        """
        Method to further filter query results based on DICOM headers

        The final output will be divided into two dictionaries:
 
          (1) matches : all studies matching filters
          (2) exclude : all remaining studies that do not match filters

        """
        # --- Load CSV 
        df = pd.read_csv(csv_file)

        # --- Initialize variables 
        matches = {}
        exclude = {}
        inds = []

        # --- Filter by study_description
        for n, result, i in zip(range(len(indices)),
            results['study_description'], 
            indices):
            query = df['Type of Exam'][i]
            synonyms = self.find_synonym(query)
            if synonyms is not None:
                for s in synonyms:
                     if result.upper().find(s.upper()) > -1:
                        # --- filter out RECIST reads
                        if result.upper().find('RECIST') == -1:
                            inds.append(n)

        # --- Remove duplicates
        inds = list(set(inds)) 

        # --- Split dictionaries
        N = len(results['mrn'])
        for key in pacs.TAGS_SORTED:
            matches[key] = [results[key][i] for i in inds]
            exclude[key] = [results[key][i] for i in range(N) if i not in inds]

        return matches, exclude 

    def find_synonym(self, query):
        """
        Method to find list of synonyms for given query

        """  
        # --- Synonyms
        SYNONYMS = [
            ['CT HEAD', 'CT BRAIN'],
            ['CT ABD', 'CT A/P', 'CT BODY'],
            ['CT CHEST', 'CT LUNG'],
            ['CT PERFUSION', 'CT PERF'],
            ['CT SOFT TISSUE NECK', 'CT NECK'],
            ['CT PELVIS'],
            ['CT HIP'],
            ['MRI PELVIS', 'MR PELVIS'],
            ['MRI BRAIN', 'MR BRAIN'],
            ['MRI ABDOMEN', 'MR ABDOMEN', 'MRI ABD', 'MR ABD'],
            ['MRI MAX', 'MRI FACIAL'],
            ['IR ANG', 'ANG'],
            ['CT FOREARM', 'CT ARM'],
            ['MRI MAX', 'MRI FACIAL'],
            ['LOWER EXTREMITY', 'LOW EX'],
            ['NM BONE/JOINT', 'NM', "BONE"],
            ['X-RAY CHEST', 'X-RAY']
        ]
        for s_list in SYNONYMS:
            for s in s_list:
                if query.upper().find(s) > -1:
                    return s_list

def main(root='.',argv = None):
    import sys 
    suffix = sys.argv[1] if len(sys.argv) > 1 else  ""
    client = Client(root=root, configs=CONFIGS)
    client.perform_find(suffix=suffix)
 
if __name__ == '__main__':
    # ===============================================================
    # INITIALIZE CLIENT
    # ===============================================================
     main()

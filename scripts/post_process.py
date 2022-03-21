# --------------------------------------------------
#  This file will be used to do an additional 
#  scrubbing on secondaries. 
#
#  USAGE: scrub_secondary.py <directiory_path> <yaml_path>
# --------------------------------------------------
import yaml
import sys, os, glob
import pydicom
from pydicom.tag import Tag

# --- fields that contain lists
list_fields = ['ImageType']

# --------------------------------------------------
#  main function
# --------------------------------------------------
def anonymize_all(scrub_dict, rules):

    for modality, acc_path_list in scrub_dict.items():

        for path in acc_path_list:

            anonymize(path, rules)

def anonymize(dir_path, rules):

    """
    Traverses through the directory and scrubs all the dcms
    based on the rules provided in the yaml.
    """
    # --- traverse and perform secondary scrub on all dicoms
    print('Scrubbing secondaries.')
    count = 0
    for root, directories, file_paths in os.walk(dir_path):
        
        for file_path in file_paths:
            
            # --- only scrub dcm files
            if file_path.endswith('.dcm'):
                
                # --- create full path and anonymize
                full_path = root + '/' + file_path
                dcm_bool = anonymize_dcm(full_path, rules)
                count += 1
    print('Scrubbed ' + str(count) + ' secondaries.')
# -----------------------------------------------------------
#  dicom checking and processing functions
# -----------------------------------------------------------
def anonymize_dcm(dcm_path, rules):
    """
    Anonymizes a single dcm using the processed rules from
    the YAML file provided.
    """
    # --- load dcm metadata 
    dcm = pydicom.dcmread(dcm_path, stop_before_pixels=True)
    
    # --- load stratified rules by modality and combine it with general rules
    dcm_modality = dcm['Modality'].value
    dcm_image_type = dcm['ImageType'].value

    if 'SECONDARY' in str(dcm_image_type):
#    if 'SECONDARY' in str(dcm_image_type) or 'DERIVED' in str(dcm_image_type):
        if dcm_modality in rules.keys():
            rules = rules[dcm_modality] + rules['OTHER']
        else:
            rules = rules['OTHER']
        
        # --- check and anonymize by looping through rules
        for rule in rules:

            # --- check for a complete match in all fields in the rule
            match = verify_rule(dcm, rule)
            
            # --- if rule match then scrub
            if match == True:

                # --- get coordinates
                coords = rule['coords']

                # --- get pixel data
                dcm = pydicom.dcmread(dcm_path)
                
                # --- decompress dicom and read pixel array
                dcm.decompress()
                dcm_array = dcm.pixel_array

                # --- remove specific pixels
                dcm_array.setflags(write=1)
                
                if pydicom.pixel_data_handlers.numpy_handler.should_change_PhotometricInterpretation_to_RGB(dcm_array):
                    dcm_array = pydicom.pixel_data_handlers.util.convert_color_space(dcm_array, 'YBR_FULL', 'RGB')
                    dcm.PhotometricInterpretation = 'RGB'

                if len(dcm_array.shape) == 4:
                    
                    # --- remove different spots for each coordinate
                    for coord in coords:
                        dcm_array[:, coord['y0'] : coord['y1'], coord['x0'] : coord['x1']] = 0
                    
                elif (len(dcm_array.shape) == 2) or (len(dcm_array.shape) == 3):

                    for coord in coords:
                        dcm_array[coord['y0'] : coord['y1'], coord['x0'] : coord['x1']] = 0

                dcm_array.setflags(write=0)

                # --- save files
                dcm.PixelData = dcm_array.tostring()
                dcm.save_as(dcm_path)

                # --- assume only one rule will be matched, return False to not quarantine
                return False

        # ---- no rules found so remove to quarantine
        return True
    
    return False
            
def verify_rule(dcm, rule):
    """
    Checks the dcm file against the fields given for a single rule.
    
    Parameters:
    dcm - dcm metadata obtained from dcmread(dicom_path)
    rule - dictionary containing specific fields the rule is looking for
    
    Returns:
    match - boolean signifying if there is a rule match
    """
    
    # --- extract all fields in a rule
    fields = rule['fields']
    
    for field, desc in fields.items():
        
        # --- check if field is a tag, if so convert it to a friendly format
        if isTag(field):
            field = format_tag(field)

        # --- check if theres a match
        if field in dcm:
            
            # --- check if field normally contains a list (e.g. ImageType)
            if field in list_fields: # --- see global variable at top
                
                # --- process descriptions into a list and remove spaces in each value
                dcm_list = list(dcm[field].value)
                dcm_list = list(map(lambda x : x.replace(' ', ''), dcm_list))
                
                # --- remove possible spaces in field
                field_desc = desc.replace(' ', '')
                
                # --- check if field is present within list
                if field_desc not in dcm_list:
                    return False
            else: 
                # --- process descriptions. capitalize and remove spaces
                field_desc = str(desc.upper())
                field_desc = field_desc.replace(' ', '')

                dcm_desc = str(dcm[field].value).upper()
                dcm_desc = dcm_desc.replace(' ', '')

                if field_desc != dcm_desc:
                    return False

        else:
            return False
    
    return True
# -----------------------------------------------------------
#  Tag checking and manipulation functions (ex: (0008, 0010))
# -----------------------------------------------------------
def isTag(field):
    """
    Checks if provided field is a tag. 
    """
    # --- check length of string
    if len(field) != 11:
        return False
    elif not ((field[0] == '[') and (field[-1] == ']')):
        return False
    else:
        return True
    
def format_tag(tag):
    """
    Converts string based tag to a dcm friendly tag format.
    """
    
    # --- remove brackets
    tag = tag[1:-1]
    
    # --- remove comma
    tag = tag.replace(',', '')
    
    # --- use dcm library to convert to dcm friendly format
    tag = Tag(tag)
    
    return tag
# --------------------------------------------------
#  load and stratify yaml rules
# --------------------------------------------------
def prepare_yaml(yaml_path):
    """
    Loads the yaml file and stratifies it by modality. If it 
    is modality agnostic, then it is placed into 'OTHER'.
    """
    # --- load rules
    rules = yaml.load(open(yaml_path, 'r'), Loader=yaml.FullLoader)
    
    # --- Sort rules by modality
    rules_stratified = {'OTHER' : []}
    for rule in rules:

        fields = rule['fields']

        if 'Modality' in fields.keys():

            if fields['Modality'] not in rules_stratified.keys():

                # --- if modality isn't present, add it
                rules_stratified[fields['Modality']] = [rule]
            else:
                rules_stratified[fields['Modality']].append(rule)

        # --- otherwise add to other
        else:
            rules_stratified['OTHER'].append(rule)
    
    return rules_stratified

if __name__ == '__main__':
    
    if len(sys.argv) == 3:
        
        # --- check that the directory and files exist
        assert os.path.isdir(sys.argv[1]), 'Error. Directory does not exist.'
        assert os.path.isfile(sys.argv[2]), 'Error. Yaml file does not exist.'
        rules = prepare_yaml(sys.argv[2]) 

        # --- anonymize directory
        anonymize(dir_path=sys.argv[1], rules=rules)
        
    else:
        print('Incorrect number of arguments.')
        print('USAGE: scrub_secondary.py <directory_path> <yaml_path>')

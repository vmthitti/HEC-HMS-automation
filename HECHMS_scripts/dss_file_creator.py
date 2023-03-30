from mil.army.usace.hec.vortex.io import BatchImporter
from mil.army.usace.hec.vortex.geo import WktFactory

import os
import csv

col_names = ['Input_folder_name','Input_run_type','Input_dss_name']
HMS_CONSTANTS_FILE = '/home/HECHMS_GODAVARI/constants.csv'

constants_file = csv.reader(open(HMS_CONSTANTS_FILE))
constants_dict = {}
for row in constants_file:
    constants_dict[row[0]] = row[1]

DSS_FILE_PATH = constants_dict['DSS_FILE_PATH']
NC_FILE_PATH = constants_dict['NC_FILE_PATH']
METADATA_INPUT_FILE = constants_dict['METADATA_INPUT_FILE']

metadata_file = csv.reader(open(METADATA_INPUT_FILE))

input_folder = ''
input_dss_name = ''

for row in metadata_file:
    input_folder = row[0]
    input_dss_name = row[2]
    break

# input_folder = metadata_file['Input_folder_name'][0]
# input_dss_name = metadata_file['Input_dss_name'][0] + '.dss'
print(input_folder,input_dss_name)


if os.path.exists(DSS_FILE_PATH+input_folder+'/'):
    if os.listdir(DSS_FILE_PATH+input_folder+'/') == []:
        os.rmdir(DSS_FILE_PATH+input_folder+'/')
    else:
        for file in os.listdir(DSS_FILE_PATH+input_folder+'/'):
            print(file)
        os.remove(DSS_FILE_PATH+input_folder+'/'+file)
        os.rmdir(DSS_FILE_PATH+input_folder+'/')

os.mkdir(DSS_FILE_PATH+input_folder+'/')

in_files = [NC_FILE_PATH+input_folder+'.nc']

destination = DSS_FILE_PATH + input_folder + '/' + input_dss_name+'.dss'   #'/home/vassar/Desktop/20220809_20220824_20220831_1661311288103.dss'

geo_options = {
    'targetCellSize': '5000',
    'targetEpsg': '32644',
    'resamplingMethod': 'Bilinear'
}

variables = ['rainfall']

write_options = {
    'partA': 'UTM44N',
    'partB': 'TN_AP',
    'partC': 'PRECIPITATION',
    'partF': 'GODAVARI',
    'dataType': 'PER-CUM',
    'units': 'MM'
}

myImport = BatchImporter.builder() \
    .inFiles(in_files) \
    .variables(variables) \
    .geoOptions(geo_options) \
    .destination(destination) \
    .writeOptions(write_options) \
    .build()

myImport.process()


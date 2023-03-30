from hms.model import Project
from hms import Hms
import csv
from exceptions import Exception as PythonException



HMS_CONSTANTS_FILE = '/home/HECHMS_GODAVARI/constants.csv'

constants_file = csv.reader(open(HMS_CONSTANTS_FILE))
constants_dict = {}
for row in constants_file:
    constants_dict[row[0]] = row[1]

METADATA_INPUT_FILE = constants_dict['METADATA_INPUT_FILE']

metadata_file = csv.reader(open(METADATA_INPUT_FILE))

for row in metadata_file:
    forecast_to_compute = row[1]
    break

print("constants_dict['HMS_PROJ_FILE']"+ " ------ ",constants_dict['HMS_PROJ_FILE'])
try:
    myProject = Project.open(constants_dict['HMS_PROJ_FILE'])
    print(myProject)
    myProject.computeForecast(forecast_to_compute)
    myProject.close()
    Hms.shutdownEngine()
except Exception as e:
    raise PythonException('HMS except :: ',e)
    # raise PythonException()

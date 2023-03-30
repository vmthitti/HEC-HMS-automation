from hec.heclib.dss import *
from hec.heclib.dss import HecDss
from hec.heclib.util import HecTime
import csv
from datetime import datetime,timedelta

HMS_CONSTANTS_FILE = '/home/HECHMS_GODAVARI/constants.csv'

constants_file = csv.reader(open(HMS_CONSTANTS_FILE))
constants_dict = {}
for row in constants_file:
    constants_dict[row[0]] = row[1]

# print(constants_dict)
INFLOWS_CSV = constants_dict['INFLOWS_METADATA_CSV']
# print(INFLOWS_CSV)
OUTFLOWS_CSV = constants_dict['OUTFLOWS_METADATA_CSV']
METADATA_INPUT_FILE = constants_dict['METADATA_INPUT_FILE']

metadata_file = csv.reader(open(METADATA_INPUT_FILE))

input_folder = ''
input_dss_name = ''

for row in metadata_file:
    input_folder = row[0]
    input_dss_name = row[1]
    break

OUTPUTDSS_FILE_PATH = constants_dict['MODEL_PATH']+ input_dss_name +'.dss'
OUTPUT_FILE_PATH = constants_dict['OUTPUT_DIR']+ input_folder

# PATH_STRING_LIST = ['/FLOW/01JUL2022/30MIN/','/FLOW/01AUG2022/30MIN/','/FLOW/01SEP2022/30MIN/']

PATH_STRING_LIST = []
flow_type = '/FLOW/'
computation_time_interval = '/30MIN/'

dates = input_folder.split('_')
# for i in dates[:-1]:
#   date = datetime.datetime.strptime(i, '%Y%m%d')
#   path = flow_type + '01'+ date.strftime("%b").upper() + str(date.year) + computation_time_interval
#   if not path in PATH_STRING_LIST:
#     PATH_STRING_LIST.append(path)
startDate = datetime(int(dates[0][0:4]),int(dates[0][4:6]),int(dates[0][6:]))
endDate = datetime(int(dates[2][0:4]),int(dates[2][4:6]),int(dates[2][6:]))
 
noofdays = (endDate-startDate).days
for i in range(0,noofdays+1):
    currDate = startDate + timedelta(days =i)
    path = flow_type + '01'+ currDate.strftime("%b").upper() + str(currDate.year) + computation_time_interval
    if not path in PATH_STRING_LIST:
        PATH_STRING_LIST.append(path)

PATH_TYPE = 'FOR:'+ input_dss_name +'/'

theFile = HecDss.open(OUTPUTDSS_FILE_PATH)
pathNameList = theFile.getCatalogedPathnames()
f= open(OUTPUT_FILE_PATH, 'w')
writer = csv.writer(f,dialect='excel',delimiter=',',lineterminator = '\n')

with open(INFLOWS_CSV) as inflows_file:
  metadata = csv.reader(inflows_file)
  headers = next(metadata)
  nameList_inf = []
  pointList_inf = []
  for row in metadata:
    nameList_inf.append(row[0])
    pointList_inf.append(row[1])
  for i in pointList_inf: 
    for paths in PATH_STRING_LIST:
      path = '//'+i+paths+PATH_TYPE 
      if theFile.recordExists(path) == False:
        print('path doesnt exist -- ',path)
        continue
      gc = theFile.get(path,1)
      stationName= i
      pathType = "inflow"
      stationNamedss = nameList_inf[pointList_inf.index(stationName)]
      for j in range(0, len(gc.times[0:])):
        time = HecTime()
        time.set(int(gc.times[j]))
        year = str(time.year())
        month = int(time.month())
        hr = str(time.hour())
        min = str(time.minute())
        if min == '0':
          continue
        
        if month < 10:
            month = str("0"+str(month))

        day = int (time.day())
        if day < 10:
            day = str("0" +str(day))

        date = str(year)+"-"+str(month)+"-"+str(day)+" "+str(hr)+":"+str(min)
        
        format_str = '%Y-%m-%d %H:%M' # The format
        
        startDate = datetime.strptime(str(date), str(format_str))
        exp_date = startDate+timedelta(minutes = 59)
        exp_datesplit = str(exp_date).split(' ')[0].split('-')
        exp_timesplit = str(exp_date).split(' ')[1].split(':')
        datesplit = str(startDate).split(' ')[0].split('-')
        oneRowList= []
        oneRowList.append(str(stationNamedss))
        oneRowList.append(str(pathType))
        oneRowList.append(str(datesplit[0]))
        oneRowList.append(str(datesplit[1]))
        oneRowList.append(str(datesplit[2]))
        oneRowList.append(hr)
        oneRowList.append(min)
        oneRowList.append(str(exp_datesplit[0]))
        oneRowList.append(str(exp_datesplit[1]))
        oneRowList.append(str(exp_datesplit[2]))
        oneRowList.append(str(exp_timesplit[0]))
        oneRowList.append(str(exp_timesplit[1]))
        oneRowList.append(str(gc.values[j]*35.314666212661))
        writer.writerow(oneRowList)


with open(OUTFLOWS_CSV) as outflows_file:
  metadata = csv.reader(outflows_file)
  headers = next(metadata)
  nameList_out = []
  pointList_out = []
  for row in metadata:
    nameList_out.append(row[0])
    pointList_out.append(row[1])
  for i in pointList_out:
    for paths in PATH_STRING_LIST:
      path = '//'+i+paths+PATH_TYPE
      if theFile.recordExists(path) == False:
        print('path doesnt exist -- ',path)
        continue
      gc = theFile.get(path,1)
      stationName= i
      pathType = "outflow"
      stationNamedss = nameList_out[pointList_out.index(stationName)]
      for j in range(0, len(gc.times[0:])):
        time = HecTime()
        time.set(int(gc.times[j]))
        year = str(time.year())
        month = int(time.month())
        hr = str(time.hour())
        min = str(time.minute())
        if min == '0':
          continue

        if month < 10:
            month = str("0"+str(month))

        day = int (time.day())
        if day < 10:
            day = str("0" +str(day))

        date = str(year)+"-"+str(month)+"-"+str(day)+" "+str(hr)+":"+str(min)
        
        format_str = '%Y-%m-%d %H:%M' # The format
        
        startDate = datetime.strptime(str(date), str(format_str))
        exp_date = startDate+timedelta(minutes = 59)
        exp_datesplit = str(exp_date).split(' ')[0].split('-')
        exp_timesplit = str(exp_date).split(' ')[1].split(':')
        datesplit = str(startDate).split(' ')[0].split('-')
        oneRowList= []
        oneRowList.append(str(stationNamedss))
        oneRowList.append(str(pathType))
        oneRowList.append(str(datesplit[0]))
        oneRowList.append(str(datesplit[1]))
        oneRowList.append(str(datesplit[2]))
        oneRowList.append(hr)
        oneRowList.append(min)
        oneRowList.append(str(exp_datesplit[0]))
        oneRowList.append(str(exp_datesplit[1]))
        oneRowList.append(str(exp_datesplit[2]))
        oneRowList.append(str(exp_timesplit[0]))
        oneRowList.append(str(exp_timesplit[1]))
        oneRowList.append(str(gc.values[j]*35.314666212661))
        writer.writerow(oneRowList)

f.close()







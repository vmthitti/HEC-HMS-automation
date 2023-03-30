import numpy as np
import pandas as pd
import datetime
import os
from zipfile import ZipFile
import shutil
import xarray as xr
import requests
import calendar
import time
import logging
import csv
import smtplib
import subprocess
from email.message import EmailMessage
from os.path import exists as file_exists
from pydsstools.heclib.dss import HecDss
from pydsstools.core import TimeSeriesContainer,UNDEFINED
logging.getLogger().setLevel(logging.INFO)


FORECAST_RUNS = ['Forecast_ECMWF_ENS','Forecast_ECMWF_DET','Forecast_IMD']
SC_HEADER_NAMES = ['stn','type','year','month','day','hour','minute','ex_year','ex_month','ex_day','ex_hour','ex_minute','flow_cusecs']
HEADER_NAMES = ['Stations','Type','Year','Month','Day','Hour','Minute','Flow_in_Cusecs']
CN_HEADER_NAMES = ['uuid','subbasin_id','cn_type','cn_val']

HMS_CONSTANTS_FILE = '/home/HECHMS_GODAVARI/constants.csv'

run_spec_dict = {'IMD_FORECAST':'IMD','ENSEMBLE_FORECAST':'ECMWF_ENS','ENSEMBLE_DETERMINISTIC':'ECMWF_DET'}
forecast_spec_dict = {'ENSEMBLE_DETERMINISTIC':'Forecast_ECMWF_DET.forecast','ENSEMBLE_FORECAST':'Forecast_ECMWF_ENS.forecast','IMD_FORECAST':'Forecast_IMD.forecast'}
forecast_compute_dict = {'ENSEMBLE_DETERMINISTIC':'Forecast_ECMWF_DET','ENSEMBLE_FORECAST':'Forecast_ECMWF_ENS','IMD_FORECAST':'Forecast_IMD'}
FORECAST_OP_DSS = {'ENSEMBLE_DETERMINISTIC':'Forecast_ECMWF_DET.dss','ENSEMBLE_FORECAST':'Forecast_ECMWF_ENS.dss','IMD_FORECAST':'Forecast_IMD.dss'}


REALTIME_INFLOWS_INP_FILE = "/realtime_inflows_input"
REALTIME_OUTFLOWS_INP_FILE = "/realtime_outflows_input"
FC_OUTPUT = 'fc_output'
SC_OUTPUT = 'sc_output'
OBSERVED_DATA = 'observed_data'
CURVE_NUMBER = 'CURVE_NUMBER'
UUID_STRING = 'uuid'
FC_OUTPUT_PATH_STRING = 'fc_output_path'
SC_OUTPUT_PATH_STRING = 'sc_output_path'
INPUT_PATH_STRING = 'input_path'
STATUS_SUCCESS = '1'
STATUS_FAILURE = '2'


def download_flow_file(constants_dict,server_path,local_path) :
    try : 
        logging.info("Downloading File :: %s",server_path)
        # downloading file from directory
        required_file =constants_dict['SERVER_SCP']+" "+constants_dict['SERVER_IP']+":"+server_path+" "+local_path
        print(required_file)
        os.system(required_file)
    
    except Exception as e:
        # logs input data related errors 
        logging.info(e)
        raise Exception ("error downloading input data",server_path)

def send_error_email(error,subject):
    msg = EmailMessage()
    msg.set_content(str(error))

    msg['Subject'] = 'HMS GODAVARI ERROR :: ' +subject
    msg['From'] = "xxxx@xxxx.com"                     #add your email
    msg['To'] = ['xxxx.com']                          #add receiver's email

    # Send the message via our own SMTP server.
    server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
    server.login("xxxxx@xxxx.com", "xxxxx")           #emailid and password
    server.send_message(msg)
    server.quit()
 
def delete_extract_dir(constants_dict):
    # delete model, model_input, nodes_output folders and extract them again from backup folder after each run or when error occurs
    DELETE_FILES_LIST = [constants_dict['MODEL_OUT_DIR_FILE']]
    for file in DELETE_FILES_LIST:
        shutil.rmtree(file)

    FILES_ZIP = ['nodes_data.zip']
    DIR_LIST = [constants_dict['MODEL_OUT_DIR']]
    for i in range(len(FILES_ZIP)):
        with ZipFile(constants_dict['BACKUP_FOLDER']+FILES_ZIP[i], 'r') as zipObj:
            zipObj.extractall(DIR_LIST[i])
    logging.info('extraction done!')

def delete_obs_discharge(constants_dict):
    os.chdir(constants_dict['OBS_DSS_DIR'])
    filelist = os.listdir('.')
    for f in filelist:
        os.remove(os.path.join(constants_dict['OBS_DSS_DIR'], f))

def prepare_runtype(runtype):
    split_runtype = runtype.split('_')
    comb_str  = split_runtype[0]+'_'+split_runtype[1]
    return comb_str

def date_prepare(date_value):
    formatted_date = datetime.date(int(date_value[0:4]),int(date_value[4:6]),int(date_value[6:]))
    return formatted_date

def nc_file_prepare(constants_dict,start_date,end_date,input_grid_data_path,nc_filename):
    start_date_string = start_date[0:4]+'-'+start_date[4:6]+'-'+start_date[6:]
    # start_date_string = '2022-05-02'
    end_date_string = end_date[0:4]+'-'+end_date[4:6]+'-'+end_date[6:]
    date_time=pd.date_range(start=start_date_string,end=end_date_string, freq='D')

    csv_daily=pd.read_csv(input_grid_data_path+start_date,header=None)
    csv_daily.columns = ['lat','lon','rainfall']
    grid_lon=np.array(sorted(set(csv_daily['lon'])))
    grid_lat=np.array(sorted(set(csv_daily['lat'])))
    grid=np.zeros((len(date_time),len(grid_lat),len(grid_lon)),dtype=float)*np.nan

    for i,tm_ in enumerate(date_time):
        Date=tm_.strftime("%Y%m%d")
        # print(Date,i)
        csv_daily=pd.read_csv(input_grid_data_path+Date,header=None)
        csv_daily.columns = ['lat','lon','rainfall']
        grid[i,:,:]=csv_daily.pivot( 'lat','lon', 'rainfall').values   

    OBS = xr.Dataset({'rainfall': (['time','lat','lon'], grid,{'units':'mm'})},
                    coords={'lon': (['lon'], grid_lon,{'units':'degrees_east'}),
                            'lat': (['lat'], grid_lat,{'units':'degrees_north'}),
                            'time': date_time})
    OBS.attrs['Conventions'] = 'CF-1.7'
    OBS.rainfall.attrs['missing_value'] = -9999
    xr.decode_cf(OBS)
    OBS.to_netcdf(constants_dict['NC_FILE_PATH']+nc_filename+'.nc',format = 'NETCDF4')

def creating_metadatafile(INPUT_FOLDER_NAME,dss_file_type,dss_file_name,constants_dict):
    METADATA_INPUT_FILE = constants_dict['METADATA_INPUT_FILE']
    metadata_file= open(METADATA_INPUT_FILE, 'w')
    writer = csv.writer(metadata_file,delimiter=',',lineterminator = '\n')
    rowList= []
    rowList.append(str(INPUT_FOLDER_NAME))
    rowList.append(str(dss_file_type))
    rowList.append(str(dss_file_name))
    writer.writerow(rowList)
    metadata_file.close()

def realtime_data_parse(constants_dict,obs_file):
    
    realtime_data_file = pd.read_csv(constants_dict['OBS_FLOWS_DIR']+obs_file,header=None,index_col=False,names =HEADER_NAMES)
    
    inflows = realtime_data_file.loc[realtime_data_file['Type']=='Inflow'].drop('Type',axis=1)
    outflows = realtime_data_file.loc[realtime_data_file['Type']=='Outflow'].drop('Type',axis=1)
    
    status_flag = False
    flag = False

    stns = pd.read_excel(constants_dict['STATIONS_DATA'],header = 0, index_col= None)
    NODES_DATA = stns['stn'].to_list()

    for i in range(len(NODES_DATA)) :
        inflows_df = inflows.loc[inflows['Stations']==NODES_DATA[i]]
        outflows_df = outflows.loc[outflows['Stations']==NODES_DATA[i]]
        status_flag = status_flag or (inflows_df['Flow_in_Cusecs']<0).any().any()
        # print('inf',status_flag)
        status_flag = status_flag or (outflows_df['Flow_in_Cusecs']<0).any().any()
        # print('out',status_flag)
        inflows_df.to_csv(constants_dict['MODEL_INP_PATH']+NODES_DATA[i]+REALTIME_INFLOWS_INP_FILE, index=False,header=False)
        outflows_df.to_csv(constants_dict['MODEL_INP_PATH']+NODES_DATA[i]+REALTIME_OUTFLOWS_INP_FILE, index=False,header=False)
    
    return flag

def observed_flows_data_prep(constants_dict):
    
    INPUT_CSV_PATH = constants_dict['OBS_DSS_FILE_PATH']
    NODES_DATA_PATH = constants_dict['MODEL_INP_PATH']
    OBSERVED_DISCHARGE_PATH = constants_dict['OBS_DSS_DIR']
    METADATA_INPUT_FILE = constants_dict['METADATA_INPUT_FILE']
    metadata_file = csv.reader(open(METADATA_INPUT_FILE))

    input_folder = ''
    for row in metadata_file:
        input_folder = row[0]
        break

    date = datetime.datetime.strptime(input_folder.split('_')[0], '%Y%m%d')
    start_date = str(date.day)+date.strftime("%B")+ str(date.year)

    start_time = '08:30:00'
    with open(INPUT_CSV_PATH) as metadata_file:
        metadata = csv.reader(metadata_file)
        headers = next(metadata)
        for row in metadata:
            print(row)
            stn_name = str(row[1])
            with open(NODES_DATA_PATH+ row[0] +'/'+ row[4]) as csv_file:
                data = csv.reader(csv_file)
                dss_arr = []
                for row in data:
                    if float(row[-1]) < 0:
                        # print(row[0])
                        dss_arr.append(float(-3.4028234663852886e+38))
                    else:
                        dss_arr.append(float(row[-1])*0.028316847)
                # startTime = start_date+' '+start_time
                tsc = TimeSeriesContainer()
                tsc.pathname = '/GODAVARI/OBSERVED/FLOW//1DAY/OBSERVED/'
                tsc.interval = 1
                tsc.values = dss_arr
                tsc.startDateTime = start_date +" "+ start_time
                tsc.numberValues = len(dss_arr)
                tsc.units = 'M3/S'
                tsc.type = 'PER-AVER'
                dssFile = HecDss.Open(OBSERVED_DISCHARGE_PATH + stn_name +'.dss')
                dssFile.put(tsc)
                dssFile.close()

def get_file_fromstatus(status_flag,run_type):
    if status_flag == False:
        file = forecast_spec_dict.get(run_type)
        model_run_type = 'forecast_spec'
    else:
        file = run_spec_dict.get(run_type)
        model_run_type = 'run_spec' 
    return file,model_run_type

def forecast_file_date_parsing(date_value):
    yesterday_date = date_value
    #  - datetime.timedelta(1)   
    day = str(yesterday_date.day)
    month = str(yesterday_date.strftime("%B"))
    month = month[0].upper()+month[1:]
    year = str(yesterday_date.year)
    formatted_date = day+" "+month+" "+year
    return formatted_date

def forecast_file(constants_dict,file,start_date,forecast_date,end_date):
    print("inside forecast file parsing")
    start_date_string = forecast_file_date_parsing(start_date)
    forecast_date_string = forecast_file_date_parsing(forecast_date)
    end_date_string = forecast_file_date_parsing(end_date)


    with open(constants_dict['FORECAST_FILE_PATH'] + file,'r') as forecast_file:
        forecast_file_data = forecast_file.read()

    word_1 = 'Start Date'
    word_2 = 'Forecast Date'
    word_3 = 'End Date'
    sentences = forecast_file_data.split('\n')

    for sentence in sentences:
        if word_1 in sentence:
            a = sentences.index(sentence)
        elif word_2 in sentence:
            b = sentences.index(sentence)
        elif word_3 in sentence:
            c = sentences.index(sentence)

    startdate_string = sentences[a].split(':')
    startdate_string[1] = ' '+ start_date_string                              #'02 August 2020'
    # startdate_string[1] = ' '+ '01 December 2021'
    startdate_string = ':'.join(startdate_string)
    sentences[a] = startdate_string

    forecastdate_string = sentences[b].split(':')
    forecastdate_string[1] = ' '+ forecast_date_string                              #'02 August 2020'
    forecastdate_string = ':'.join(forecastdate_string)
    sentences[b] = forecastdate_string

    enddate_string = sentences[c].split(':')
    enddate_string[1] = ' '+ end_date_string                                  #'03 August 2020'
    enddate_string = ':'.join(enddate_string)
    sentences[c] = enddate_string

    open(constants_dict['FORECAST_FILE_PATH'] + file,"w").close()

    forecast_file_write = open(constants_dict['FORECAST_FILE_PATH'] + file,"w+")

    # print(sentences)

    for i in sentences:
        forecast_file_write.write(i)
        forecast_file_write.write('\n')
    forecast_file_write.close()

def grid_file_date_parsing(date_value,index):
    if index == 0:
        day = date_value.day - 1
    else:
        day = date_value.day 

    if day<10:
        day = '0'+str(day)
    else:
        day =str(day)
    month = str(date_value.strftime("%B")[:3].upper())
    year = str(date_value.year)
    formatted_date = day+month+year+':1200'    #TODO time formatting
    return formatted_date

def grid_file(constants_dict,start_date,end_date):
    print("inside grid file parsing")
    start_date_string = grid_file_date_parsing(start_date,0)
    end_date_string = grid_file_date_parsing(start_date,1)
    with open(constants_dict['GRID_FILE_PATH'],'r') as grid_file:
        grid_file_data = grid_file.read()

    word = 'DSS Pathname'
    sentences = grid_file_data.split('\n')

    idx = []
    for i in range(len(sentences)):
        if word in sentences[i]:
            idx.append(i)

    for j in idx:
        dss_string = sentences[j].split('/')
        dss_string[4] = start_date_string                        #'02JAN2020:1200'
        # dss_string[4] = '01MAY2022:1200'
        # dss_string[5] = '02MAY2022:1200'
        dss_string[5] = end_date_string                          #'03JAN2020:1200'
        dss_string = '/'.join(dss_string)

        sentences[j] = dss_string

    open(constants_dict['GRID_FILE_PATH'],"w").close()

    grid_file_write = open(constants_dict['GRID_FILE_PATH'],"w+")

    for i in sentences:
        grid_file_write.write(i)
        grid_file_write.write('\n')
    grid_file_write.close()

def basin_file(constants_dict,CN_PATH,BASIN_PATH):
    # cn_data = pd.read_csv(CN_PATH,header=None,index_col=False,names =CN_HEADER_NAMES)
    # subbasin_ids = cn_data['subbasin_id'].to_list()
    # cn_vals = cn_data['cn_val'].to_list()
    cn_data = pd.read_csv(constants_dict['CN_GODAVARI'],header=0,index_col=False)
    subbasin_ids = cn_data['Name'].to_list()
    cn_vals = cn_data['CN3'].to_list()
    # print(subbasin_ids,cn_vals)

    with open(BASIN_PATH,'r') as basin_file:
            basin_file_data = basin_file.read()
    sentences = basin_file_data.split('\n')

    idx1 = []
    idx2 = []
    start = []
    end = []
    for idx in range(len(subbasin_ids)):
        word_1 = 'Subbasin: '+str(subbasin_ids[idx])
        for i in range(len(sentences)):
            if word_1 in sentences[i]:
                idx1.append(i)

    for i in idx1:
        clip_sentence = sentences[i:]
        word_2 = 'End:'
        for j in range(len(clip_sentence)):
            if word_2 in clip_sentence[j]:
                idx2.append(i+j)
                break

    for x in range(len(idx1)):
        clip_sentences = sentences[idx1[x]:idx2[x]+1]
        clip_sentences[-12] = '     Curve Number: '+str(cn_vals[x])
        for y in range(len(clip_sentences)):
            sentences[idx1[x]+y] = clip_sentences[y]

    open(BASIN_PATH,"w").close()

    basin_file_write = open(BASIN_PATH,"w+")

    for i in sentences:
        basin_file_write.write(i)
        basin_file_write.write('\n')
    basin_file_write.close()

def gage_file_date_parsing(date_value,offset):
    day = str(date_value.day)
    month = str(date_value.strftime("%B"))
    month = month[0].upper()+month[1:]
    year = str(date_value.year)

    if offset == 0:
        formatted_date = day+" "+month+" "+year+', 08:30'
    else:
        formatted_date = day+" "+month+" "+year+', 08:30'
    
    return formatted_date

def gage_file(constants_dict, start_date, forecast_date, end_date):
    print("inside gage file parsing")
    start_date_string = gage_file_date_parsing(start_date,0)
    obs_startdate_string = gage_file_date_parsing(start_date,1)
    obs_enddate_string = gage_file_date_parsing(forecast_date,1)
    end_date_string = gage_file_date_parsing(forecast_date,0)

    with open(constants_dict['GAGE_FILE_PATH'],'r') as gage_file:
        gage_file_data = gage_file.read()

    word_1 = 'Gage:'
    word_2 = 'End:'

    sentences = gage_file_data.split('\n')

    idx1 = []
    idx2 = []
    for i in range(len(sentences)):
        if word_1 in sentences[i]:
            idx1.append(i)
        if word_2 in sentences[i]:
            idx2.append(i)

    idx2 = idx2[1:]

    for i in range(len(idx1)):
        clip_sentences = sentences[idx1[i]:idx2[i]+1]

        clip_sentences[-4] = '       '+'Start Time:'+' '+ start_date_string              #'1 January 2021, 12:00'
        clip_sentences[-3] = '       '+'End Time:'+' '+ end_date_string                     #'26 January 2021, 12:00'
        for j in range(len(clip_sentences)):
            sentences[idx1[i]+j] = clip_sentences[j]

    # obs_stns_data = pd.read_csv(OBS_DSS_FILE_PATH,header = 0,index_col=False)
    # obs_stns = obs_stns_data['Gage_name'].to_list()
    
    # for j in obs_stns:
    #     word_1 = 'Gage: '+ j
    #     word_2 = 'End:'
    #     start = []
    #     end = []
        
    #     for k in range(len(sentences)):
    #         if word_1 not in sentences[k]:
    #             continue
    #         else:
    #             start.append(k)
    #             break

    #     for l in idx2:
    #         if start[0] > l:
    #             continue
    #         else:
    #             end.append(l)
    #             break

    #     for x in range(len(start)):
    #         clip_sentences = sentences[start[x]:end[x]+1]
    #         clip_sentences[-4] = '       '+'Start Time:'+' '+ obs_startdate_string              #'1 January 2021, 12:00'
    #         clip_sentences[-3] = '       '+'End Time:'+' '+ obs_enddate_string                     #'26 January 2021, 12:00'
    #         for y in range(len(clip_sentences)):
    #             sentences[start[x]+y] = clip_sentences[y]

    open(constants_dict['GAGE_FILE_PATH'],"w").close()

    gage_file_write = open(constants_dict['GAGE_FILE_PATH'],"w+")

    for i in sentences:
        gage_file_write.write(i)
        gage_file_write.write('\n')
    gage_file_write.close()

def sc_merge(constants_dict,INPUT_FILE):
    sc_map = {}

    with open(constants_dict['SC_METADATA_PATH']) as sc_file:
        datareader = csv.reader(sc_file)
        for row in datareader:
            if row[0] in sc_map.keys():
                if row[2] == 'add':
                    sc_map[row[0]]['add'].append(row[1])
                else:
                    sc_map[row[0]]['remove'].append(row[1])
            else:
                sc_map[row[0]] = {}
                sc_map[row[0]]['add'] = []
                sc_map[row[0]]['remove'] = []
                if row[2] == 'add':
                    sc_map[row[0]]['add'].append(row[1])
                else:
                    sc_map[row[0]]['remove'].append(row[1])

    sc_output_csv = pd.read_csv(constants_dict['SC_INPUT_CSV_PATH']+INPUT_FILE, header=None, index_col=False, names=SC_HEADER_NAMES)
    sc_metadata = pd.read_csv(constants_dict['SC_METADATA_PATH'],header=None, index_col=False).iloc[0].iloc[1]

    final_df = pd.DataFrame()

    base_df = (sc_output_csv[sc_output_csv['stn'] == sc_metadata].iloc[:, 1:-1]).reset_index(drop = True)

    sc_points = list(sc_map.keys())
    # print(sc_points)
    for stn in sc_points:
        add_remove_map = sc_map[stn]
        stn_df = pd.DataFrame()
        for i in list(add_remove_map.keys()):
            for sc in sc_map[stn][i]:
                # print(sc)
                sub_df = (sc_output_csv[sc_output_csv['stn'] == sc].iloc[:, -1:]).reset_index(drop = True)
                if stn_df.empty:
                    stn_df = pd.concat([stn_df,sub_df],axis=0)
                else:
                    stn_df = stn_df.add(sub_df)
        merge_df = pd.concat([base_df,stn_df],axis=1)
        merge_df.insert(0, 'stn', stn)
        
        final_df = pd.concat([final_df,merge_df],axis=0)

    final_df.to_csv(constants_dict['SC_OUTPUT_FILE_PATH']+INPUT_FILE,header = False,index=False)

def server_file_upload(constants_dict,FINAL_OUT_DIR, OUTPUT_FILENAME,SERVER_PATH_TO_UPLOAD):
    model_output_file = constants_dict['SERVER_SCP']+" "+FINAL_OUT_DIR+OUTPUT_FILENAME+" "+constants_dict['SERVER_IP']+":"+SERVER_PATH_TO_UPLOAD
    cmd = os.system(model_output_file)
    print(cmd)
    return cmd

def main():   ##TODO upon exception extracting data from backup folder.
    while(True):
        try:
            try:
                try:                
                    # constants_metadata = pd.read_excel(HMS_CONSTANTS_FILE,header=0, index_col= None,sheet_name=0)   ##creation constants from constants file
                    # constants_dict = dict(zip(constants_metadata.constant, constants_metadata.path))
                    constants_file = csv.reader(open(HMS_CONSTANTS_FILE))
                    constants_dict = {}
                    for row in constants_file:
                        constants_dict[row[0]] = row[1]
                except Exception as e:
                    send_error_email(e,'constant file importing error')
                    print("invalid constant file :: ",e)
                    continue

                try:  
                    response = requests.get(constants_dict['REQUEST_API'])               ##getting response from server
                except Exception as e:
                    send_error_email(e,'Server response error')
                    print("Server response error :: ",e)
                    time.sleep(60)
                    continue
                print("status code :: %s",str(response.status_code))  ## printing response status code
                
                if response.status_code != 200:             ##if status code is not 200 sleep 30 ms and hit again
                    time.sleep(900)
                    continue
                
                try:        
                    UUID = response.json().get(UUID_STRING)    ##getting UUID of the task given
                    print(response.json())
                except Exception as e:
                    send_error_email(e,'Error no UUID_String')
                    print("Error no UUID_String :: ",e)
                    continue
                
                try:
                    INPUT_PATH = response.json().get('dex').get(INPUT_PATH_STRING)  ##getting Input path of data from server
                except Exception as e:
                    send_error_email(e,'Input path error in request')
                    print("Input path error in request :: ",e)
                    continue
                
                try:
                    FC_OUTPUT_PATH = response.json().get('dex').get(FC_OUTPUT_PATH_STRING)  ##getting full catchment output path from the server
                except Exception as e:
                    send_error_email(e,'Full catchment output path error')
                    print("Full catchment output path error :: ",e)
                    continue

                try:    
                    SC_OUTPUT_PATH = response.json().get('dex').get(SC_OUTPUT_PATH_STRING)  ##getting self catchment output path from the server
                except Exception as e:
                    send_error_email(e,'self catchment output path error')
                    print("self catchment output path error :: ",e)
                    continue

                try:
                    source = response.json().get('dex').get('source')  ##getting type of source from the task
                except Exception as e:
                    send_error_email(e,'source data error ')
                    print("source data error :: ",e)
                    continue
                
                if source == 'ACTUAL_SOURCE':    ##ignoring Actual_source task as there will be no forecast
                    continue
                
                try:
                    download_flow_file(constants_dict,INPUT_PATH,constants_dict['INPUT_GRID_DIR'])     ##downloading input data of the task
                except Exception as e:
                    send_error_email(e,source + ' :: '+'error downloading input file')
                    print("error downloading input file :: ",e)
                    continue

                runtype = prepare_runtype(source)  ## if ENSEMBLE_DETERMINISTIC_FORECAST is source runtype is ENSEMBLE_DETERMINISTIC
                print(runtype)
                # source = 'IMD_FORECAST'
                # runtype = prepare_runtype('ENSEMBLE_FORECAST')
                # INPUT_FILE = '20230303_20230318_20230427_1679144521408.zip'
                INPUT_FILE = INPUT_PATH.split('/')[-1]
                INPUT_FOLDER_NAME = INPUT_FILE.split('.')[0]

                try:
                    if os.path.exists(constants_dict['INPUT_GRID_DIR']+INPUT_FOLDER_NAME+'/'):  ##if INPUT_GRID_DIR folder exisits delete the path
                        shutil.rmtree(constants_dict['INPUT_GRID_DIR']+INPUT_FOLDER_NAME+'/')
                except Exception as e:
                    send_error_email(e,source + ' :: '+'error removing ' + constants_dict['INPUT_GRID_DIR']+ 'path')
                    print('error removing ' + constants_dict['INPUT_GRID_DIR']+ 'path',e)
                    continue

                try:
                    with ZipFile(constants_dict['INPUT_GRID_DIR']+INPUT_FILE, 'r') as zipObj:  ##unziping input data
                        zipObj.extractall(constants_dict['INPUT_GRID_DIR'])
                except Exception as e:
                    send_error_email(source + ' :: '+str(e),'unzip error of grid data ' + constants_dict['INPUT_GRID_DIR'])
                    print('unzippiing error :: ',e)
                    continue
                
                try:
                    os.chdir(constants_dict['INPUT_GRID_DIR']+INPUT_FOLDER_NAME+'/')  #copying observed data from input data to observed data folder 
                    os.rename(OBSERVED_DATA , OBSERVED_DATA+'_'+INPUT_FOLDER_NAME)
                    shutil.move(constants_dict['INPUT_GRID_DIR']+INPUT_FOLDER_NAME+'/'+OBSERVED_DATA+'_'+INPUT_FOLDER_NAME , constants_dict['OBS_FLOWS_DIR']+OBSERVED_DATA+'_'+INPUT_FOLDER_NAME)
                except Exception as e:
                    send_error_email(e,source + ' :: '+'error copying observed flows file '+ OBSERVED_DATA+'_'+INPUT_FOLDER_NAME)
                    print('error copying observed flows file '+ OBSERVED_DATA+'_'+INPUT_FOLDER_NAME + ' :: ',e )
                    continue
                
                try:
                    req_dates = INPUT_FOLDER_NAME.split('_')  ##getting simulation dates from input folder name
                    start_date = date_prepare(req_dates[0])
                    # start_date = datetime.date(int(2022),int(5),int(2))
                    forecast_date = date_prepare(req_dates[1])
                    end_date = date_prepare(req_dates[2])
                    print(start_date,forecast_date,end_date)
                except Exception as e:
                    send_error_email(e,source + ' :: '+'input file name issue')
                    print('input file name issue :: ',e)
                    continue

                if not file_exists(constants_dict['INPUT_GRID_DIR']+INPUT_FOLDER_NAME+'/'+CURVE_NUMBER+'_'+str(req_dates[1])): ##CN file copying to CN folder
                    print(CURVE_NUMBER+'_'+str(req_dates[1])," this Curve number file not available. ")
                    send_error_email('source is '+ str(source)+ ' and input file name '+str(INPUT_FOLDER_NAME),'Curve number file not exists')
                    response = requests.get(constants_dict['RESPONSE_API']+'/'+UUID+'/'+STATUS_FAILURE)
                    delete_extract_dir()
                
                    os.chdir(constants_dict['OBS_DSS_DIR'])
                    filelist = os.listdir('.')
                    for f in filelist:
                        os.remove(os.path.join(constants_dict['OBS_DSS_DIR'], f))
                    time.sleep(5)
                    continue
                else:
                    os.rename(CURVE_NUMBER+'_'+str(req_dates[1]) , CURVE_NUMBER+'_'+str(req_dates[1])+'_'+INPUT_FOLDER_NAME)
                    shutil.move(constants_dict['INPUT_GRID_DIR']+INPUT_FOLDER_NAME+'/'+CURVE_NUMBER+'_'+str(req_dates[1])+'_'+INPUT_FOLDER_NAME , constants_dict['CN_DIR']+CURVE_NUMBER+'_'+str(req_dates[1])+'_'+INPUT_FOLDER_NAME)
                
                try:
                    nc_file_prepare(constants_dict,req_dates[0],req_dates[2],constants_dict['INPUT_GRID_DIR']+INPUT_FOLDER_NAME+'/',INPUT_FOLDER_NAME)
                except Exception as e:             ##preparing NC file
                    send_error_email(e,source + ' :: '+'error when creating NC file ')
                    print ("error when creating NC file ", e)
                    response = requests.get(constants_dict['RESPONSE_API']+'/'+UUID+'/'+STATUS_FAILURE)
                    delete_extract_dir(constants_dict)
                
                    os.chdir(constants_dict['OBS_DSS_DIR'])
                    filelist = os.listdir('.')
                    for f in filelist:
                        os.remove(os.path.join(constants_dict['OBS_DSS_DIR'], f))
                    time.sleep(5)
                    continue                  

                try:
                    dss_file_type = forecast_compute_dict.get(runtype)
                    dss_file_name = run_spec_dict.get(runtype)
                    creating_metadatafile(INPUT_FOLDER_NAME,dss_file_type,dss_file_name,constants_dict)  ##creating metadata file
                except Exception as e:
                    send_error_email(e,source + ' :: '+'error creating metadata file')
                    print("error creating metadata file",e)
                    continue

                try:
                    os.chdir(constants_dict['HMS_DIR_PATH'])
                    os.system("./hec-hms.sh -script " + constants_dict['DSS_FILE_CREATE_SCRIPT_PATH']) ##calling rainfall DSS file creation script
                except Exception as e:
                    send_error_email(e,source + ' :: '+'error creating input rainfall dss file')
                    print(e)
                    continue

                try:
                    dss_file_name = run_spec_dict.get(runtype)  ##parsing observed data according to stations
                    missing_data_status = realtime_data_parse(constants_dict,OBSERVED_DATA+'_'+INPUT_FOLDER_NAME)
                    print('missing_data_status',missing_data_status)
                except Exception as e:
                    send_error_email(e,source + ' :: '+'error parsing observed flows')
                    print(e)
                    continue

                try:
                    observed_flows_data_prep(constants_dict) ##preparing observed blending dss data files
                except Exception as e: 
                    send_error_email(e,source + ' :: '+'observed flows creation error')
                    print('observed flows creation error ::',e)
                    continue

                try:    
                    file, model_run_type = get_file_fromstatus(missing_data_status,runtype) ##copying rainfall dss file to model folder
                    print(file,model_run_type)
                    shutil.copy(constants_dict['DSS_FILE_PATH']+INPUT_FOLDER_NAME+'/'+dss_file_name+'.dss', constants_dict['MODEL_INPUT_DSS_PATH'])
                except Exception as e:
                    send_error_email(e,source + ' :: '+'dss file copy error')
                    print('dss file copy error :: ' ,e)
                    continue

                try:
                    forecast_file(constants_dict,file,start_date,forecast_date,end_date) ##making changes to forecast file
                except Exception as e: ##TODO copy file if error occurs
                    send_error_email(e,source + ' :: '+'forecast file parsing exception')
                    print('forecast file parsing exception :: ',e)
                    continue
                
                try:
                    grid_file(constants_dict,start_date,end_date) ##making changes to grid file
                except Exception as e:   ##TODO copy file if error occurs 
                    send_error_email(e,source + ' :: '+'grid file parsing exception')
                    print('grid file parsing exception :: ',e)
                    continue

                # try:
                #     if runtype != 'ENSEMBLE_FORECAST':  ##making changes to basin file by modifying the CN values for subbasins
                #         basin_file(constants_dict,constants_dict['CN_DIR']+CURVE_NUMBER+'_'+str(req_dates[1])+'_'+INPUT_FOLDER_NAME,constants_dict['BASIN_FILE_PATH'])
                #     else:
                #         basin_file(constants_dict,constants_dict['CN_DIR']+CURVE_NUMBER+'_'+str(req_dates[1])+'_'+INPUT_FOLDER_NAME,constants_dict['VIRGIN_BASIN_FILE_PATH'])
                # except Exception as e:    ##TODO copy file if error occurs
                #     send_error_email(e,source + ' :: '+'basin file parsing exception')
                #     print('basin file parsing exception :: ',e)
                #     continue
                
                try:
                    os.remove(constants_dict['GAGE_FILE_PATH']) ##making changes to gage file
                    shutil.copyfile(constants_dict['BACKUP_FOLDER']+constants_dict['GAGE_FILE_SRC'] , constants_dict['MODEL_PATH']+constants_dict['GAGE_FILE_SRC'])
                    gage_file(constants_dict,start_date,forecast_date,end_date) 
                except Exception as e:   ##TODO copy file if error occurs
                    send_error_email(e,source + ' :: '+'gage file parsing exception')
                    print('gage file parsing exception :: ',e)
                    continue

                try:
                    forecast_dss = FORECAST_OP_DSS.get(runtype) ##copying output empty dss file before running model
                    print('copying  --- '+forecast_dss)
                    os.remove(constants_dict['MODEL_PATH']+forecast_dss)
                    shutil.copyfile(constants_dict['BACKUP_FOLDER']+forecast_dss , constants_dict['MODEL_PATH']+forecast_dss)
                except Exception as e:
                    send_error_email(e,source + ' :: '+'error copying' + forecast_dss + ' dss file')
                    print('error copying' + forecast_dss + ' dss file :: ',e)
                    continue

                try:
                    os.chdir(constants_dict['HMS_DIR_PATH'])   #running forecast spcification for model
                    return_type = os.system("./hec-hms.sh -script " + constants_dict['FORECAST_SCRIPT_FILE_PATH'])
                    print('return-type :::   ',return_type)
                except Exception as e:
                    send_error_email(e,source + ' :: '+'HMS run execution error')
                    print('HMS run execution error :: ',e)
                    print('sleeping 20 sec')
                    time.sleep(20)
                    continue
                
                try:
                    os.system("./hec-hms.sh -script " + constants_dict['DSSSCRIPT_FILE_PATH'])  ##extracting full catchment out[ut]
                except Exception as e:
                    send_error_email(e,source + ' :: '+'error extracting full catchment output')
                    print('error extracting full catchment output :: ',e)
                    continue            

                try:
                    os.system("./hec-hms.sh -script " + constants_dict['SC_DSSSCRIPT_FILE_PATH'])  ##extracting self catchment output
                    sc_merge(constants_dict,INPUT_FOLDER_NAME)
                except Exception as e:
                    send_error_email(e,source + ' :: '+'error extracting self catchment output')
                    print('error extracting self catchment output :: ',e)
                    continue
                
                # time.sleep(5000)

                ##uploading self and full catchment data
                FC_file_upload_status = server_file_upload(constants_dict,constants_dict['FINAL_OUT_PATH']+FC_OUTPUT+'/',INPUT_FOLDER_NAME,FC_OUTPUT_PATH)
                SC_file_upload_status = server_file_upload(constants_dict,constants_dict['FINAL_OUT_PATH']+SC_OUTPUT+'/',INPUT_FOLDER_NAME,SC_OUTPUT_PATH)
                


                ##if file is not uploaded trying to reupload
                if (FC_file_upload_status != 0):
                    for i in range(3):
                        FC_file_upload_status = server_file_upload(constants_dict,constants_dict['FINAL_OUT_PATH']+FC_OUTPUT+'/',INPUT_FOLDER_NAME,FC_OUTPUT_PATH)
                        if FC_file_upload_status == 1:
                            continue
                        else:
                            FC_file_upload_status = 0
                            break
                elif(SC_file_upload_status !=0):
                    for i in range(3):
                        SC_file_upload_status = server_file_upload(constants_dict,constants_dict['FINAL_OUT_PATH']+FC_OUTPUT+'/',INPUT_FOLDER_NAME,SC_OUTPUT_PATH)
                        if SC_file_upload_status == 1:
                            continue
                        else:
                            SC_file_upload_status = 0
                            break
                
                ## based on error code handling the response for input request
                if (FC_file_upload_status == 1 or SC_file_upload_status == 1):
                    response = requests.get(constants_dict['RESPONSE_API']+'/'+UUID+'/'+STATUS_FAILURE)   
                else:
                    try:
                        response = requests.get(constants_dict['RESPONSE_API']+'/'+UUID+'/'+STATUS_SUCCESS)
                        if response.status_code == 200:
                            logging.info("API response :: %s",str(response.text))
                        elif response.status_code != 200:
                            for i in range(3):
                                time.sleep(30)
                                response = requests.get(constants_dict['RESPONSE_API']+'/'+UUID+'/'+STATUS_SUCCESS)
                                if response.status_code != 200:
                                    continue
                                else:
                                    break
                    except Exception as e:
                        send_error_email(e,source + ' :: '+'error upon acknowledging the success status')
                        print('error upon acknowledging the success status',e)
                        delete_extract_dir()
                        delete_obs_discharge(constants_dict)
                        continue
                
                #after model run is done prepaing for file structure for next run
                delete_extract_dir(constants_dict)
                delete_obs_discharge(constants_dict)
                print('will sleep 5 sec and continue')
                time.sleep(5)
                continue

            except Exception as e:
                send_error_email(e,'code execution error')
                print("code execution error",e)
                continue
        
        except Exception as e:
            continue 

if __name__ == "__main__":
    main()

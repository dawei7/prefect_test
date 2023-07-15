import csv
from datetime import datetime
import json
import copy
import time
import paramiko
from prefect import flow, task
from axpogeneral.blob_util import BlobUtil
import io

# General
blob_service_client = BlobUtil(SECRET)

@flow(name="ENTSOE_Extract",log_prints=True)
def extract(files:list):
    # create ssh client 
    ssh_client = paramiko.SSHClient()

    # remote server credentials
    host = "sftp-transparency.entsoe.eu"
    username = "david.a.schmid@axpo.com"
    password = "Entso#12345678"
    port = 22

    # Define the remote file path and local destination path
    remote_file_path = "/TP_export/"
    local_destination_path = 'C:/Users/ddavischm/Desktop/CLONE/entsoe_poc_pytasks/LocalStorage/Input/'

    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname=host,port=port,username=username,password=password)

    ftp = ssh_client.open_sftp()

    blob_destination_path = "Prefect/Input"

    for file in files:
        # ftp.get(remote_file_path+file, local_destination_path+file.split("/")[1])
        
        sftp_file = ftp.open(remote_file_path+file)
        blob_service_client.upload_data("entsoe-poc", blob_destination_path + "/" + file.split("/")[1], sftp_file.read())

    # close the connection
    ftp.close()
    ssh_client.close()

@task(name="ENTSOE_Transform_Task1")
def read_csv_to_dict(file_path):
    
    blob_client = blob_service_client.blobClient('entsoe-poc', file_path)
    blob_data = blob_client.download_blob()
    blob_file = io.StringIO(blob_data.content_as_text())
    
    reader = csv.reader(blob_file,delimiter=';')
    
    data_dict = {}
        
    # The first row is the header
    header = next(reader)
        
    # get dictionary with all the header cols
    dict_header = dict()
    for i,col in enumerate(header):
        dict_header[col]=i
        
    for row in reader: # Loop begins with second row
        # Assuming the first column of the CSV is the key
        key = row[dict_header["values"]] #Config col "values"
        data_dict[key] = [row[dict_header["mesap_id"]], #Config col "mesap_id"
                                  row[dict_header["columns"]], #Config col "columns" -> We need this to match correct column numbers in raw file
                                  row[dict_header["value_column"]], # Config col "value_column" -> We need to match the correct column number
                                  row[dict_header["product"]], #Config col "product" -> We need this for filtering the config_dict for specific raw files
            ]
            
    return data_dict


@task(name="ENTSOE_Transform_Task2")
def read_raw_timeseries(file_path,config:dict={}):
    
    # Initialize
    data_dict = {} # resulting dictionary, which will be returned
    
    # get Product name from file_name
    product = file_path.split("/")[-1][8:-4] # pattern: C:/Users/ddavischm/Desktop/ENTSOE_PREFECT/LocalStorage/Input/2023_06_AggregatedGenerationPerType_16.1.B_C.csv
    
    blob_client = blob_service_client.blobClient('entsoe-poc', file_path)
    blob_data = blob_client.download_blob()
    blob_file = io.StringIO(blob_data.content_as_text(encoding='utf-8-sig'))
    
    reader = csv.reader(blob_file,delimiter='\t')

    # reader = csv.reader(csvfile,delimiter='\t')
        
    # The first row is the header
    header = next(reader)
        
    # get dictionary with all the header cols
    dict_header = dict()
    for i,col in enumerate(header):
        dict_header[col]=i
            
    # Important deepcopy > copy all values not pointers
    mod_config = copy.deepcopy(config) # for every raw file take a fresh full copy, "more general universal approach"
    # filter config file to contain only valid mapping_values for specific raw_file
    for k,v in config.items():
        if v[3] != product: # Check Column product in config file
            del mod_config[k] # delete all non matchng products for specific raw file
        
    # Fixed Column numbers in evey timeseries for Entsoe
    col_date_t1 = dict_header["DateTime"]
    col_date_t2 = dict_header["UpdateTime"]
    col_value = dict_header[next(iter(mod_config.values()))[2]] #Peek first entry of mod_config and 3 field, it does not matter which entry, because product value column is consistent per product
    cols_mapping = [dict_header[col] for col in next(iter(mod_config.values()))[1].split(",")] # Peek first entry, get array of all columns in correct order
        
    # Main Loop
    for row in reader:
        concat_key = ",".join([row[el] for el in cols_mapping])
            
        try: # some TS are missing in config file, therefore we have to try except
            ts_id = mod_config[concat_key][0]
        except:
            continue
            
            
        timeseries = [row[col_date_t1],
                          row[col_date_t2],
                          row[col_value]
                          ]
            
        if ts_id in data_dict:
            data_dict[ts_id].append(timeseries)
        else:
            data_dict[ts_id]=[]
            data_dict[ts_id].append(timeseries)
            
    return data_dict

@flow(name="ENTSOE_Transform",log_prints=True)
def transform():
    # LocalStorage Paths
    
    # path_input = "C:/Users/ddavischm/Desktop/CLONE/entsoe_poc_pytasks/LocalStorage/Input/"
    path_input = "Prefect/Input/"
    
    path_config = "Prefect/Config/config.csv"
    
    # path_output = "C:/Users/ddavischm/Desktop/CLONE/entsoe_poc_pytasks/LocalStorage/Output/"
    path_output = "Prefect/Output/"

    # Read config File as Dictionary    
    blob_client = blob_service_client.blobClient('entsoe-poc', path_config)
    blob_config = blob_client.download_blob()
    
    dict_config = read_csv_to_dict(blob_config)


    files = [
            "2023_06_AggregatedGenerationPerType_16.1.B_C",
            "2023_06_TotalCommercialSchedules_12.1.F",
            "2023_06_TotalImbalanceVolumes_17.1.H"
    ]

    for file in files:
                
        dict_timeseries = read_raw_timeseries(file_path=path_input+file+".csv",config=dict_config)

        file_path = path_output + file + ".json"
        

        class DateTimeEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                return super().default(obj)
            

        # Convert the dictionary to a JSON string
        json_data = json.dumps(dict_timeseries, cls=DateTimeEncoder)

        # Upload the JSON string to the blob container
        blob_service_client.upload_data("entsoe-poc", file_path, json_data)

        """
        with open(file_path, "w") as json_file:
            json.dump(dict_timeseries, json_file, cls=DateTimeEncoder)
            
        """
            


@flow(name="ENTSOE_ETL",log_prints=True)
def main_flow():
    # Call Subflow Extract
    
    """"
    extract(
        files = [
        "AggregatedGenerationPerType_16.1.B_C/2023_06_AggregatedGenerationPerType_16.1.B_C.csv",
        "TotalCommercialSchedules_12.1.F/2023_06_TotalCommercialSchedules_12.1.F.csv",
        "TotalImbalanceVolumes_17.1.H/2023_06_TotalImbalanceVolumes_17.1.H.csv"
    ])
    """
    
    transform()



if __name__ == '__main__':
     # Start time
    start_time = time.time() 

    main_flow()
    
    # End time
    end_time = time.time()
    
    # Calculate the execution time
    execution_time = end_time - start_time

    # Print the execution time
    print(f"Execution time: {execution_time} seconds")

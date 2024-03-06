import anvil.files
from anvil.files import data_files

# Copyright(C) Val-Cloud Ltd 2023. All rights reserved
import anvil.secrets
import anvil.email
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
import anvil.http
from anvil import app
import pandas as pd
import decimal
import time
import numpy as np
import io 
import cryptography
from cryptography.fernet import Fernet
from rich import box
from rich.console import Console
from rich.table import Table
import Project_types as pt
import Benchmarks as bm
from tabulate import tabulate

import anvil.media
import openpyxl 
import Connections as Connections
import pyodbc
import sqlalchemy as salch
import urllib.parse
import urllib3
import json
import kv_calcs as kc
from datetime import datetime as dt
import calendar

import sys, traceback
from dateutil.relativedelta import *


# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.
#
# To allow anvil.server.call() to call functions here, we mark
# them with @anvil.server.callable.
# Here is an example - you can replace it with your own:
#
# @anvil.server.callable
# def say_hello(name):
#   print("Hello, " + name + "!")
#   return 42
#
# Set up the database connection 

# Test cryptography
#key = 'O&P&F&dfY1JySQ'

  
def connect_to_database_azure_odbc(published):
  dbconnection = None
  try:
    if Connections.connection == 'Not connected':
      print('In connect_to_database Azure odbc')
      print('anvil.app.environment.name')
      print(anvil.app.environment.name)
      print('published')
      print(published)
      database = "onepointfive_uk_dev_HUB4"
  #    database = "onepointfive_uk_dev"
      print(" ")
      print("======================")
      print("DATABASE BEING USED: -")
      print(database)
      print("======================")
      print(" ")
  #    Connection string from Azure portal: -    
  #    Driver={ODBC Driver 13 for SQL Server};Server=tcp:onepointfive-uk.database.windows.net,1433;Database=onepointfive_uk_dev;Uid=onepointfive-uk;Pwd={your_password_here};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;
  
      server   = "onepointfive-uk.database.windows.net"
      port     = 1433
      user     = "onepointfive-uk"
      password = anvil.secrets.get_secret('prod_database_password')
      driver   = "ODBC Driver 17 for SQL Server"
      yes      = "yes"
      no       = "no"
      ct       = "30"
      
      print('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+user+';PWD='+ password)
      
      dbconnection = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+user+';PWD='+ password)
      Connections.connection_string = 'DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+user+';PWD='+ password
      #print('After connect_to_database_azure_odbc - dbconnection ')
      #print(dbconnection)
      Connections.connection = dbconnection 
      return dbconnection # Normal return
  except Exception as e:       
    #dbconnection.close()
    msg     = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    logmess = f"*****Critical database connection error \n {msg}"
    print(logmess)
    return dbconnection # Exception return
  
# Initialise database connection below is called by login form and connection made available to other server modules
# via Server_Globals.

@anvil.server.callable
def initialise_database_connection(published):
  print(f"\n######## In initialise_database_connection - Connections.connection = {Connections.connection}\n")
  if Connections.connection == 'Not connected':
    conn = connect_to_database_azure_odbc(published)
  else:
    conn = Connections.connection
  return conn

@anvil.server.callable
def close_database_connection():
  
  print('close_database_connection has been called-----')
  
  return
  
@anvil.server.background_task
def upload_estate_H4_PC_001_bt(file,entity, partner, client, published, user_name, dt_str):
# 1 - Upload has been rationalised to upload from front input sheet with user friendly column headers.
# 2 - Building types are non capitalized CIBSE 2021 on the reference sheet.
# 3 - This is a half-way house to full partner, client and entity structure. The partner is fixed as 'OPF Admin Partner' and client as 'OPF Admin Client'. These fields are stored in Globals.
# 4 - The entity has been written to the 'Auth' sheet (by the download forms option) along with the fixed Partner and Client names. This is the entity selected from the dropdown on the Home screen.
# 5 - Authentication key has been written to the 'Key' sheet (by the download forms option) and contains the encrypted Partner, Client and Entity details.
# 6 - The encryption key is held in anvil.secrets - upload_auth_key.
# 7 - 2 stage authentication: -
  #      (i)  - Check that the details on the 'Auth' sheet have not been tampered with - decode the Authentication key and compare results with Partner, Client and Entity read from 'Auth' sheet.
  #      (ii) - Check the user is running the upload for the entity selected on the Home screen
  #   These 2 stages combined ensure that building data is not loaded into the wrong entity.
# 
# This is the Background Task version for uploading raw estate data from an OPF Excel data entry workbook at release H4_PC_001 and higher. This is the half-way version of estate
# loading for the Partner Channel (hence PC).  
#
# After validation valid records are processed sequentially and the following process is applied : -
# 1 - If the building record does NOT already exist in the raw_estate_data table, the record is inserted; the emission calculations performed and the results are inserted
#     into the results_raw_estate_data table.
# 2 - If the building record already exists in the raw_estate_data table, the existing record is updated; the equivalent record in the results_raw_estate_data table is deleted; 
#     the emission calculations are performed and the results are inserted into the results_raw_estate_data table.
# 3 - Building records that are in the database but not in the upload file are deleted together with their results from the results_raw_estate_data table.
# 5 - When all input records in the upload file have been processed the controlled estate summary results are recalculated for the entity and updated
#     in the controlled_estate_summary table.
# 6 - The baseline energy usage and energy unit costs are copied into the actual_energy_usage and actual_energy_costs tables respectively for the data year. NOTE: this will 
#     cause an overwrite of data for the data year if actuals jobs have been run since the last estate lite upload.
# 7 - The project energy and carbon savings are also recalculated to reflect changes to baseline usage and costs in the input.
# 8 - The PBI tables are also recalculated to ensure any changes above can be picked up in the dashboards. 
#
# NOTE: Duplicate building IDs (uprn) in the upload file will result in an error and the upload will stop with an exception.

# Create header for the summary and the log. Initialize upload log and summary messages.
# dt_str = dd/mm/YY H:M:S
  task_name           = "Upload estate version H4_PC_001"
  print(f"Upload estate version H4_PC_001\n---------------------\n")
  anvil.server.task_state['pc_complete'] = "0"
  anvil.server.task_state['status'] = f"{task_name} upload starting "    
  header                 = f"Estate upload by user - {user_name} run on {dt_str} for Partner: {partner}, Client: {client}, Entity {entity} \n File selected : {file.name}\n Task name - {task_name}\n "
  task_context           = f"/{partner}/{client}/{entity}"
  up_log                 = header
  
  try:
      
  # Open database connection
    conn                    = initialise_database_connection(published)
  # Retrieve the entity number from entities table
          
    entity_number           = anvil.server.call('get_entity_number_v002',entity) 

    # Retrieve client and partner owning this entity from the database

    #[partner,client,rc,ef]  = kc.get_partner_client_from_entity_number(conn, entity_number)
  
  # Get background task ID and initialise log
  
    task_id                 = anvil.server.context.background_task_id 
    task_id                 = f"{task_id}{task_context}"
    print('====task_id from upload estate')
    print(task_id)

    from datetime import datetime

    kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
    
    with anvil.media.TempFile(file) as file_name:
    
      if file == None or entity == None:
        up_log                        = up_log  + f"++++++++ No file or entity supplied\n"
        anvil.server.task_state['pc_complete'] = "0"
        anvil.server.task_state['status'] = "****FAILED - No file or entity supplied"
        kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
        return 
  
      else:
      
        # Placeholder for the entity column as selected by the user
        dent                = {'entity':entity,'entity_number':entity_number}
        
        # Check sheet named 'Input sheet', 'Reference', 'Auth' and 'Key' are in workbook
        shn                 = ['Input Sheet','Reference','Auth','Key']
        xl                  = pd.ExcelFile(file_name)
        snames              = xl.sheet_names  # see all sheet names
        serr                = 0
        for tsn in shn:
          if tsn not in snames:
            serr              = serr + 1
            up_log            = up_log  + f"****Error - cannot find sheet called {tsn} required for estate upload\n"
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            
        if serr > 0:
          anvil.server.task_state['pc_complete'] = "0"
          anvil.server.task_state['status'] = f"****FAILED - cannot find sheet called {tsn} required for estate upload"          
          return
          
        # Read in the Auth dataframe and check Partner, Client and Entity are the same as encrypted in the Key on the Key sheet.. If they are different then raise an error and exit.
        
#      Authenticate_workbook
        auts                = pd.read_excel(file_name, sheet_name = 'Auth', dtype = object) 
        keys                = pd.read_excel(file_name, sheet_name = 'Key', dtype = object)
        ret                 = kc.authenticate_workbook(auts, keys,  partner, client, entity)
        ef                  = ret['ef']
        msg                 = ret['em']

        if ef == 1:
          up_log            = up_log + f"***Workbook authentication error: -\n{msg}\n"
          anvil.server.task_state['pc_complete'] = "0"
          anvil.server.task_state['status'] = f"****FAILED - Workbook authentication error, see upload log for details"
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
          return          
        if ef == 2:
          if 'cryptography.exceptions.InvalidSignature: Signature did not match digest' in msg:
            up_log            = up_log + f"***Invalid key found in workbook - does not correspond to context detils. Possibly the Auths or Key sheets have been edited."
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****FAILED - Invalid key found in workbook - does not correspond to context detils. Possibly the Auths or Key sheets have been edited."
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            return    
          else: 
            up_log            = up_log + f"***Authentication failure - inconsistency between current context and the key stored in the workbook. Possibly attempting to load the workbook to the wrong entity: -\n{msg}\n"
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****Authentication failure - inconsistency between current context and the key stored in the workbook. Possibly attempting to load the workbook to the wrong entity"
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            return        
        
          
        # Read in the Input dataframe from 2nd row (row index = 1) - i.e. Excel row 2 contains column headers (row 1 are the grouping labels (e.g. building identification))
        
        df                = pd.read_excel(file_name, sheet_name = 'Input Sheet', header = 1, dtype = object)

        # Rename the columns to be the same as database columns in raw_estate_data table. NOTE: if the column sequence in the input Excel workbook is changed
        # then remember to update the sequence in the command below so it matches the workbook.
        
        df.set_axis(['uprn','building_name','building_type','address','postcode','latitude_dd','longitude_dd','gia_m2','roof_space_m2','data_year','baseline_annual_elec_kwh',\
                     'baseline_annual_gas_kwh','baseline_annual_oil_kwh','baseline_annual_lpg_kwh','onsite_generation_annual_kwh','exist_solar_pv_annual_kwh','exist_solar_thermal_annual_kwh','baseline_annual_cert_green_kwh',\
                     'exist_non_solar_decarb_heat_annual_kwh','baseline_elec_cost_per_kwh','baseline_gas_cost_per_kwh','baseline_oil_cost_per_kwh',\
                     'baseline_lpg_cost_per_kwh','baseline_cert_green_cost_per_kwh','source_of_heating','source_of_dhw','dec_score','epc'],axis = 1, inplace = True)
        
        # Cleansing and validation of input dataframe
        # Replace nans with 0 in numeric columns
        print(f"Before cleaning df is: ++++++- \n {df.to_string()}")
        
        df['uprn']                                   = df['uprn'].fillna(0)
        df['gia_m2']                                 = df['gia_m2'].fillna(0)
        df['roof_space_m2']                          = df['roof_space_m2'].fillna(0)
        df['latitude_dd']                            = df['latitude_dd'].fillna(0)
        df['longitude_dd']                           = df['longitude_dd'].fillna(0)
        df['data_year']                              = df['data_year'].fillna(0)
        df['baseline_annual_elec_kwh']               = df['baseline_annual_elec_kwh'].fillna(0)
        df['baseline_annual_gas_kwh']                = df['baseline_annual_gas_kwh'].fillna(0)
        df['baseline_annual_oil_kwh']                = df['baseline_annual_oil_kwh'].fillna(0)
        df['baseline_annual_lpg_kwh']                = df['baseline_annual_lpg_kwh'].fillna(0)
        df['onsite_generation_annual_kwh']           = df['onsite_generation_annual_kwh'].fillna(0)
        df['baseline_annual_cert_green_kwh']         = df['baseline_annual_cert_green_kwh'].fillna(0)
        df['dec_score']                              = df['dec_score'].fillna(0)
        df['epc']                                    = df['epc'].fillna(0)
        df['baseline_elec_cost_per_kwh']             = df['baseline_elec_cost_per_kwh'].fillna(0)
        df['baseline_gas_cost_per_kwh']              = df['baseline_gas_cost_per_kwh'].fillna(0)
        df['baseline_oil_cost_per_kwh']              = df['baseline_oil_cost_per_kwh'].fillna(0)
        df['baseline_lpg_cost_per_kwh']              = df['baseline_lpg_cost_per_kwh'].fillna(0)
        df['baseline_cert_green_cost_per_kwh']       = df['baseline_cert_green_cost_per_kwh'].fillna(0)
        df['exist_non_solar_decarb_heat_annual_kwh'] = df['exist_non_solar_decarb_heat_annual_kwh'].fillna(0)
        df['exist_solar_pv_annual_kwh']              = df['exist_solar_pv_annual_kwh'].fillna(0)
        df['exist_solar_thermal_annual_kwh']         = df['exist_solar_thermal_annual_kwh'].fillna(0)

        # Replace nans with null in string columns
                
        df['building_name']                          = df['building_name'].fillna('')
        df['address']                                = df['address'].fillna('')
        df['postcode']                               = df['postcode'].fillna('')
        df['building_type']                          = df['building_type'].fillna('')
        df['source_of_heating']                      = df['source_of_heating'].fillna('')
        df['source_of_dhw']                          = df['source_of_dhw'].fillna('')
  
        # Convert all strings to UPPER except building name and building_type,

        df['address']                                = df['address'].str.upper()
        df['postcode']                               = df['postcode'].str.upper()
        df['source_of_heating']                      = df['source_of_heating'].str.upper()
        df['source_of_dhw']                          = df['source_of_dhw'].str.upper() 

        # Remove all single quotes in free text fields (otherwise they mess with the python formatting f" of sql strings)
        
        print(f"After cleaning df is: ++++++- \n {df.to_string()}")

        df['building_name']                          = df['building_name'].astype(str).str.replace("[']", "", regex=True)
        df['address']                                = df['address'].astype(str).str.replace("[']", "", regex=True) 
          
        print(f"After removing single quotes df is: ++++++- \n {df.to_string()}")
        #  Insert column holding row numbers as seen by user in Excel
      
        df.insert(loc=0,column    ='excel_row_num',value = df.reset_index().index + 3)
        num_rows_read             = df.shape[0]
        up_log                    = up_log + f"Number of records read from input file - {num_rows_read}\n"

        print('====Cleaning complete df now is:')
        print(df.to_string())
        print(f"\n building name is: - \n {df['building_name']}")
        

        # Insert copy of dataframe to log file
        #print(f"+++++ Dataframe before validation: -\n{df.to_string()}\n")
        #up_log                    = up_log + f"+++++ Dataframe before validation: -\n{df.to_string()}\n"
        kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
        
        # Validate the dataframe
        #{'ef':0,'em':'Validation completed successfully','validated_df':'','validation_messages':'','nvw':0,'nve':0}
        validation          = kc.validate_estate_upload_H4_PC_001_1(conn, entity, entity_number, df)
        ef                  = validation['ef']
        nve                 = validation['nve']
        em                  = validation['em']
        vm                  = validation['validation_messages']
        df                  = validation['validated_df']

        print('After validation --------')
        print(f"ef = {ef}")
        print(f"nve = {nve}")
        print(f"em = {em}")
        print(f"vm = {vm}")
        
        #up_log              = up_log + f"+++++ Dataframe AFTER validation: -\n{df.to_string()}\n {vm} \n {em}\n"
        kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
  
        if ef > 0:
          up_log            = up_log + f"***Error exception occurred validating upload file: -\n{em}\n"
          anvil.server.task_state['pc_complete'] = "0"
          anvil.server.task_state['status'] = f"****FAILED - Error exception occurred validating upload file, see upload log for details"
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
          return
          
        df                  = validation['validated_df']
        vm                  = validation['validation_messages']
        nvw                 = validation['nvw']
        nve                 = validation['nve']
        up_log              = up_log + f"Validation messages:\n {vm}\n"
        up_log              = up_log + f"Results of validation: - \n    Number of warnings - {nvw}\n    Number of errors  - {nve}\n" 
  
        if nve > 0:
          anvil.server.task_state['pc_complete'] = "0"
          anvil.server.task_state['status'] = f"****FAILED - Estate upload file has failed validation, see upload log for details. Please correct and re-submit."
          up_log = up_log + f"\n ****FAILED - Estate upload file has failed validation - see above. Please correct and re-submit."
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
          return
        if nvw > 0:
          anvil.server.task_state['pc_complete'] = "20"
#         anvil.server.task_state['status'] = f"****Warning - Estate upload file validation has generated warnings, see upload log for details. Please review."
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
        else:
          print('nothing')
          anvil.server.task_state['pc_complete'] = "20"
          anvil.server.task_state['status'] = f"Estate upload file validation completed successfully."

        with conn.cursor() as cursor:
          
        # At this point we have a dataframe with at worst warnings and no errors so now we can DELETE and UPDATE records in the database.

        # Initialise counters
          nupdates      = 0 # Number of records successfully updated in raw_estate_data table
          ninserts      = 0 # Number of records successfully inserted in raw_estate_data table
          ndeletes      = 0 # Number of records successfully deleted from raw_estate_data table
          rec_num       = 0 # Number of records in dataframe
                            # rec_num should equal (nupdates + ninserts + ndeletes) unless errors
          
          nrawdeldber   = 0 # Number of errors during delete from raw_estate_data table
          ninsrawwa     = 0 # Number of warnings during insert into raw_estate_data table
          ninsrawer     = 0 # Number of errors during insert into raw_estate_data table
          nupdrawwa     = 0 # Number of warnings during update of raw_estate_data table
          nupdrawer     = 0 # Number of errors during update of raw_estate_data table
          
          ncalresok     = 0 # Number of building emission calculations completed successfully
          ncalreswa     = 0 # Number of building emission calculations completed with warnings
          ncalreser     = 0 # Number of building emission calculations that failed
          
          ninsresok     = 0 # Number of building emission inserts completed successfully
          ninsresdber   = 0 # Number of building emission inserts completed with warnings
          ninsresdbwa   = 0 # Number of building emission inserts that failed
          
          nrec_delra    = 0
          nrec_delre    = 0
          esumdber      = 0
          eok           = 0
          eflag         = False
          nr_in_df      = df.shape[0]

        # First get a list of all buildings in raw_estate_data for this entity_number so we can test if there are any buildings in
        # the database that are not in the upload, and if so delete them from the database.

          sqlo           = f"SELECT entity_number, uprn FROM raw_estate_data WHERE entity_number = {entity_number};" 
          
          cursor.execute(sqlo)
          npdb     = cursor.fetchall()
          for t in npdb: #t is a list of tuples in which 2nd element is uprn
            uindb  = t[1]
            if not df.uprn.isin([uindb]).any(): # A building with uprn uindb has been found in the database which is not in the upload dataframe.
              print(f"uprn {uindb} is not in the dataframe column uprn")
              sqldu = f"DELETE FROM raw_estate_data WHERE ((entity_number = {entity_number}) AND (uprn = {uindb}))"
              cursor.execute(sqldu)
              try:
                cursor.execute(sqldu)
                conn.commit()
                ndeletes = ndeletes + 1
         
              except (pyodbc.Error) as e:
            # Rolling back in case of error
                exnum             = 1
                nrawdeldber       = nrawdeldber + 1
                conn.rollback()
                up_log            = up_log + f"Exception number {exnum} - error deleting record from database for estate: {entity} building id: {uindb}. Database returned: - \n{e}\n"
                anvil.server.task_state['pc_complete'] = "0"
                anvil.server.task_state['status'] = f"****FAILED - Exception number {exnum} - error deleting record from database for entity: {entity} uprn: {uindb}. Please see log file for details"
                kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
                return                      
        # Iterate through rows of the upload dataframe
          
          for d in df.to_dict(orient="records"):
            # d is now a dict of {columnname -> value} for this row
            # Add in the entity column, as selected by the user, as the first column

            d4            = dent.copy()
            d4.update(d)
      
            rec_num       = rec_num + 1
            entity_number = d4['entity_number']
            uprn          = d4['uprn']
            building_name = d4['building_name']
            
            # Delete the record in results_raw_data_table (if one exists) to avoid duplicate entry errors
            
            sqlre  = f"DELETE FROM results_raw_estate_data WHERE entity_number = {entity_number} AND uprn = {uprn}"
            cursor.execute(sqlre)
            conn.commit() 
            
            exists = kc.uprn_exists_in_raw_data(conn, uprn, entity_number)
            
            if not exists: #This building does not already exist in raw data so insert it
              
              ts = f"\'{d4['entity']}\',{d4['entity_number']},{d4['uprn']},\'{d4['building_name']}\',\'{d4['address']}\',\'{d4['postcode']}\',\'{d4['building_type']}\'\
              ,{d4['latitude_dd']},{d4['longitude_dd']},{d4['gia_m2']},{d4['roof_space_m2']},{d4['data_year']}\
              ,{d4['baseline_annual_elec_kwh']},{d4['baseline_annual_gas_kwh']},{d4['baseline_annual_oil_kwh']},{d4['baseline_annual_lpg_kwh']},{d4['baseline_annual_cert_green_kwh']}\
              ,\'{d4['source_of_heating']}\',\'{d4['source_of_dhw']}\',{d4['dec_score']},{d4['epc']}\
              ,{d4['baseline_elec_cost_per_kwh']},{d4['baseline_gas_cost_per_kwh']},{d4['baseline_oil_cost_per_kwh']},{d4['baseline_lpg_cost_per_kwh']},{d4['baseline_cert_green_cost_per_kwh']}\
              ,{d4['onsite_generation_annual_kwh']},{d4['exist_non_solar_decarb_heat_annual_kwh']},{d4['exist_solar_pv_annual_kwh']},{d4['exist_solar_thermal_annual_kwh']}"
              sql1 = "INSERT INTO raw_estate_data (entity,entity_number,uprn,building_name,address,postcode,building_type,latitude_dd,longitude_dd,\
              gia_m2,roof_space_m2,data_year,baseline_annual_elec_kwh,baseline_annual_gas_kwh,baseline_annual_oil_kwh,baseline_annual_lpg_kwh,baseline_annual_cert_green_kwh,\
              source_of_heating,source_of_dhw,dec_score,epc,\
              baseline_elec_cost_per_kwh,baseline_gas_cost_per_kwh,baseline_oil_cost_per_kwh,baseline_lpg_cost_per_kwh,baseline_cert_green_cost_per_kwh,\
              onsite_generation_annual_kwh,exist_non_solar_decarb_heat_annual_kwh,exist_solar_pv_annual_kwh,exist_solar_thermal_annual_kwh,g_saving_flag, e_saving_flag) VALUES("
              sql2 = ",0,0);"
              sql  = f"{sql1} {ts} {sql2}"

              #up_log = up_log + f"++++++INSERT sql \n {sql}\n"
              
              uprn = d4['uprn']
                
              try:
                cursor.execute(sql)
                conn.commit()
                ninserts         = ninserts + 1
            
              except (pyodbc.Error) as e:
                exnum             = 2
                ninsrawer         = ninsrawer + 1
                conn.rollback()
                eflag             = True
                up_log            = up_log + f"Exception number {exnum} - INSERT into OPF database - ****ERROR on record number {rec_num}. DB returned: - \n{e}\n"
                anvil.server.task_state['pc_complete'] = "0"
                anvil.server.task_state['status'] = f"****FAILED - Exception number {exnum} - error on INSERT record number {rec_num}. Please see log file for details"
                kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
                return  
          
            else: #This building already exists in raw data so update the values

              upsql = f"UPDATE raw_estate_data \
                      SET entity                             = \'{d4['entity']}\',\
                      entity_number                          = {d4['entity_number']},\
                      uprn                                   = {d4['uprn']},\
                      building_name                          = \'{d4['building_name']}\',\
                      address                                = \'{d4['address']}\',\
                      postcode                               = \'{d4['postcode']}\',\
                      latitude_dd                            = {d4['latitude_dd']},\
                      longitude_dd                           = {d4['longitude_dd']},\
                      building_type                          = \'{d4['building_type']}\',\
                      gia_m2                                 = {d4['gia_m2']},\
                      roof_space_m2                          = {d4['roof_space_m2']},\
                      data_year                              = {d4['data_year']},\
                      baseline_annual_elec_kwh               = {d4['baseline_annual_elec_kwh']},\
                      baseline_annual_gas_kwh                = {d4['baseline_annual_gas_kwh']},\
                      baseline_annual_oil_kwh                = {d4['baseline_annual_oil_kwh']},\
                      baseline_annual_lpg_kwh                = {d4['baseline_annual_lpg_kwh']},\
                      baseline_annual_cert_green_kwh         = {d4['baseline_annual_cert_green_kwh']},\
                      source_of_heating                      = \'{d4['source_of_heating']}\',\
                      source_of_dhw                          = \'{d4['source_of_dhw']}\',\
                      dec_score                              = {d4['dec_score']},\
                      epc                                    = {d4['epc']},\
                      baseline_elec_cost_per_kwh             = {d4['baseline_elec_cost_per_kwh']},\
                      baseline_gas_cost_per_kwh              = {d4['baseline_gas_cost_per_kwh']},\
                      baseline_oil_cost_per_kwh              = {d4['baseline_oil_cost_per_kwh']},\
                      baseline_lpg_cost_per_kwh              = {d4['baseline_lpg_cost_per_kwh']},\
                      baseline_cert_green_cost_per_kwh       = {d4['baseline_cert_green_cost_per_kwh']},\
                      onsite_generation_annual_kwh           = {d4['onsite_generation_annual_kwh']},\
                      exist_non_solar_decarb_heat_annual_kwh = {d4['exist_non_solar_decarb_heat_annual_kwh']},\
                      exist_solar_pv_annual_kwh              = {d4['exist_solar_pv_annual_kwh']}, \
                      exist_solar_thermal_annual_kwh         = {d4['exist_solar_thermal_annual_kwh']} \
                      WHERE ((entity_number = {entity_number}) and (uprn = {uprn}));"

              try:
                
                cursor.execute(upsql)
                conn.commit()
                nupdates         = nupdates + 1
            
              except (pyodbc.Error) as e:
                exnum             = 14
                nupdrawer         = nupdrawer + 1
                conn.rollback()
                eflag             = True
                up_log            = up_log + f"Exception number {exnum} - UPDATE raw_estate_data table - ****ERROR on record number {rec_num}. DB returned: - \n{e} \n SQL is: \n {upsql}\n"

            if not eflag : # As long as there wasn't an error writing it to raw_estate_data table
              
              # INSERT energy kwh usage and unit costs to actual_energy_usage and actual_energy costs respectively. Delete any existing records beforehand.
              
              dsql1 = f"DELETE FROM actual_energy_usage WHERE ((entity_number = {entity_number}) AND (uprn = {uprn}) AND (year = {d4['data_year']}))"
              cursor.execute(dsql1)
              conn.commit() 
              dsql1 = f"DELETE FROM actual_energy_costs WHERE ((entity_number = {entity_number}) AND (uprn = {uprn}) AND (year = {d4['data_year']}))"
              
              cursor.execute(dsql1)
              conn.commit() 

              # The following formatted strings had to be split up because Anvil didn't like them combined together although syntax checkers were OK with it.
              
              dsqlu = f"INSERT INTO actual_energy_usage (entity_number, uprn, year, energy_code, kwh) VALUES ({entity_number},{uprn},{d4['data_year']},1,{d4['baseline_annual_elec_kwh']});\
              INSERT INTO actual_energy_usage (entity_number, uprn, year, energy_code, kwh) VALUES ({entity_number},{uprn},{d4['data_year']},2,{d4['baseline_annual_gas_kwh']});" 
              dsqlv = f"INSERT INTO actual_energy_usage (entity_number, uprn, year, energy_code, kwh) VALUES ({entity_number},{uprn},{d4['data_year']},3,{d4['baseline_annual_oil_kwh']});\
              INSERT INTO actual_energy_usage (entity_number, uprn, year, energy_code, kwh) VALUES ({entity_number},{uprn},{d4['data_year']},4,{d4['baseline_annual_lpg_kwh']});\
              INSERT INTO actual_energy_usage (entity_number, uprn, year, energy_code, kwh) VALUES ({entity_number},{uprn},{d4['data_year']},5,{d4['exist_solar_pv_annual_kwh']});\
              INSERT INTO actual_energy_usage (entity_number, uprn, year, energy_code, kwh) VALUES ({entity_number},{uprn},{d4['data_year']},6,{d4['exist_solar_thermal_annual_kwh']});\
              INSERT INTO actual_energy_usage (entity_number, uprn, year, energy_code, kwh) VALUES ({entity_number},{uprn},{d4['data_year']},13,{d4['baseline_annual_cert_green_kwh']});\
              INSERT INTO actual_energy_usage (entity_number, uprn, year, energy_code, kwh) VALUES ({entity_number},{uprn},{d4['data_year']},14,{d4['exist_non_solar_decarb_heat_annual_kwh']});"
              try:
              
                cursor.execute(dsqlu)
                conn.commit()
                cursor.execute(dsqlv)
                conn.commit()
              
              except (pyodbc.Error) as e:
                exnum             = 24
                nupdrawer         = nupdrawer + 1
                conn.rollback()
                eflag             = True
                up_log            = up_log + f"Exception number {exnum} - UPDATE actual_energy_usage - ****ERROR on record number {rec_num}. DB returned: - \n{e} \n SQL is: \n {upsql}\n"                
                kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
                return
                
              dsqlx = f"INSERT INTO actual_energy_costs (entity_number, uprn, year, energy_code, cost_per_kwh) VALUES ({entity_number},{uprn},{d4['data_year']},1,{d4['baseline_elec_cost_per_kwh']});\
              INSERT INTO actual_energy_costs (entity_number, uprn, year, energy_code, cost_per_kwh) VALUES ({entity_number},{uprn},{d4['data_year']},2,{d4['baseline_gas_cost_per_kwh']});" 
              dsqly = f"INSERT INTO actual_energy_costs (entity_number, uprn, year, energy_code, cost_per_kwh) VALUES ({entity_number},{uprn},{d4['data_year']},3,{d4['baseline_oil_cost_per_kwh']});\
              INSERT INTO actual_energy_costs (entity_number, uprn, year, energy_code, cost_per_kwh) VALUES ({entity_number},{uprn},{d4['data_year']},4,{d4['baseline_lpg_cost_per_kwh']});\
              INSERT INTO actual_energy_costs (entity_number, uprn, year, energy_code, cost_per_kwh) VALUES ({entity_number},{uprn},{d4['data_year']},5,{0});\
              INSERT INTO actual_energy_costs (entity_number, uprn, year, energy_code, cost_per_kwh) VALUES ({entity_number},{uprn},{d4['data_year']},6,{0});\
              INSERT INTO actual_energy_costs (entity_number, uprn, year, energy_code, cost_per_kwh) VALUES ({entity_number},{uprn},{d4['data_year']},13,{d4['baseline_cert_green_cost_per_kwh']});\
              INSERT INTO actual_energy_costs (entity_number, uprn, year, energy_code, cost_per_kwh) VALUES ({entity_number},{uprn},{d4['data_year']},14,{0});"
              try:
              
                cursor.execute(dsqlx)
                conn.commit()
                cursor.execute(dsqly)
                conn.commit()
                
              except (pyodbc.Error) as e:
                exnum             = 25
                nupdrawer         = nupdrawer + 1
                conn.rollback()
                eflag             = True
                up_log            = up_log + f"Exception number {exnum} - UPDATE actual_energy_costs - ****ERROR on record number {rec_num}. DB returned: - \n{e} \n SQL is: \n {upsql}\n"                
                kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
                return              
              
              anvil.server.task_state['pc_complete'] = "40"
              anvil.server.task_state['status'] = f"Raw estate data upload completed successfully - now calculating emissions" 
              # Calculate emissions for this building as identified by its uprn and entity number
              em = kc.calc_building_co2_emissions(conn, entity_number, uprn)
  
              if em['ef'] < 2:
                if em['ef'] == 1:
                  ncalreswa    = ncalreswa + 1
                  exnum        = 4
                  up_log  = up_log + f"++++Exception number {exnum} - WARNING - calculating emission results for building - {building_name} building id {uprn}. However results will be stored in database.\n {em['em']}\n"
                if em['ef'] == 0 or em['ef'] == 1:
                  ncalresok    = ncalresok + 1
                  ts   = f"{uprn},\'{entity}\',{entity_number},'YES',\'{d4['building_name']}\',{em['elec_co2']},{em['gas_co2']},{em['oil_co2']},{em['lpg_co2']},{em['gas_wtt_scope_3']},{em['elec_t_d_scope_3']},\
                  {em['elec_wtt_t_d_scope_3']},{em['elec_wtt_gen_scope_3']},{em['oil_wtt_scope_3']},{em['lpg_wtt_scope_3']},{em['total_scope_1']},{em['total_scope_2']},\
                  {em['total_scope_3']},{em['total_co2_tco2e']},{em['annual_elec_cost']},{em['annual_gas_cost']},{em['annual_oil_cost']},{em['annual_lpg_cost']},{em['annual_energy_cost']},\
                  {em['total_kwh']},{em['elec_kwh_m2']},{em['gas_kwh_m2']},{em['bmark_elec_kwh_m2b']},{em['bmark_gas_kwh_m2b']},{em['elec_2b_saved_2_typical']},{em['gas_2b_saved_2_typical']}"
                  sql3 = "INSERT INTO results_raw_estate_data (uprn,entity,entity_number,under_control,building_name,elec_co2,gas_co2,oil_co2,lpg_co2,gas_wtt_scope_3,elec_t_d_scope_3,elec_wtt_t_d_scope_3,\
                  elec_wtt_gen_scope_3,oil_wtt_scope_3,lpg_wtt_scope_3,total_scope_1,total_scope_2,total_scope_3,total_co2_tco2e,annual_elec_cost,annual_gas_cost,annual_oil_cost,annual_lpg_cost,\
                  annual_energy_cost,total_kwh,elec_kwh_m2,gas_kwh_m2,bmark_elec_kwh_m2b,bmark_gas_kwh_m2b,elec_2b_saved_2_typical,gas_2b_saved_2_typical ) VALUES("
                  sql4 = ");"
                  sql  = f"{sql3} {ts} {sql4}"
  
          # Write emission results to database - results_raw_estate_data table
                try:
                  cursor.execute(sql)
                  conn.commit()
                  ninsresok      = ninsresok + 1
              
                except (pyodbc.Error) as me:
                  exnum             = 5
                  ninsresdber       = ninsresdber + 1                
                  conn.rollback()
                  up_log            = up_log + f"Exception number {exnum} - INSERT into results_raw_estate_data table - ***ERROR on record number {rec_num}. DB returned: - \n{me}\n"
                  kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
                  return          
              else:
                exnum             = 7
                ncalreser         = ncalreser + 1
                up_log            = up_log + f"Exception number {exnum} - ****ERROR - calculating emission results for building - {building_name} uprn {uprn}. Calculation returned \n{em['em']}\n"
                kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
                return                
          fs = ''
          fs = fs + f"\n"
          fs = fs + f"Number of records read from upload file                            : {rec_num} \n"
          fs = fs + f"Number of records successfully updated                             : {nupdates} \n"
          fs = fs + f"Number of records successfully inserted                            : {ninserts} \n"
          fs = fs + f"Number of warnings during insert into raw_estate_data table        : {ninsrawwa} \n"
          fs = fs + f"Number of errors during insert into raw_estate_data table          : {ninsrawer} \n"
          fs = fs + f"Number of warnings during update of raw_estate_data table          : {nupdrawwa} \n"
          fs = fs + f"Number of errors during update of raw_estate_data table            : {nupdrawer} \n"
          fs = fs + f"Number of building emmission calculations completed successesfully : {ncalresok} \n"
          fs = fs + f"Number of building emmission calculations completed with warnings  : {ncalreswa} \n"
          fs = fs + f"Number of building emmission calculations that failed              : {ncalreser} \n"
          fs = fs + f"Number of emmission result record inserts completed successfully   : {ninsresok} \n"
          fs = fs + f"Number of emmission result record inserts completed with warnings  : {ninsresdbwa} \n"
          fs = fs + f"Number of emmission result record inserts that failed              : {ninsresdber} \n"          
          
          up_log = up_log + f"{fs}\n"
          
          totalerrs  = ninsrawer + ncalreser + ninsresdber
          totalwarn  = ninsrawwa + ncalreswa + ninsresdbwa
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
          
          if totalerrs > 0:
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****FAILED - calculating emissions. Please see upload log for details"
            return 
          if totalwarn > 0:
            anvil.server.task_state['pc_complete'] = "60"
            anvil.server.task_state['status'] = f"Calculating emissions completed with warnings. Please review details in upload log"
          else:
            print('nothing')
            anvil.server.task_state['pc_complete'] = "60"
            anvil.server.task_state['status'] = f"Calculating emissions completed successfully"
  #        time.sleep(5)  
      # Delete summary controlled_estate_summary already existing for this entity and re-calculate them
  
          dsql1 = "DELETE FROM controlled_estate_summary WHERE entity_number ="
          dsql2 = f"{entity_number}"
          dsql  = f"{dsql1} {dsql2}"
          cursor.execute(dsql)
          conn.commit() 
        
        # Calculate summary results for the controlled estate.
  
          esumer  = 0
          print('>>>>>>>>>About to calc controlled estate summary)')
          es      = kc.calc_controlled_estate_summary(conn, entity_number)
          print('>>>>es after calc controlled summary')
          print(es)
          
          csef = es['ef']
          csem = es['em']
          
          if csef == 2:
            up_log = f"****Error returned from calculation of controlled estate summary - \n {csem}\n"
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****FAILED - calculating controlled estate summary. Please see upload log for details"
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            return             
          if csef < 2:
            if csef == 1:
              exnum   = 8
              esumwa  = esumwa + 1
              up_log  = up_log + f"++++WARNING - calculating summary results for entity - {entity}. However results will be stored in database.\n {csem}\n"            
              kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            if csef == 0:
              up_log = up_log + f"---- Controlled estate summary calculated successfully \n"
              kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
              
            ts   = f"{entity_number},{es['total_elec_kwh']},{es['total_gas_kwh']},{es['total_oil_kwh']},{es['total_lpg_kwh']},{es['total_solar_pv_kwh']},{es['total_solar_thermal_kwh']},{es['total_energy_kwh']},{es['total_area_m2']},\
                  {es['total_estates_portfolio']},{es['elec_cost_kwh_gbp']},{es['gas_cost_kwh_gbp']},{es['average_dec_score']},\'{es['average_dec_rating']}\',{es['co2_scope_1']},\
                  {es['co2_scope_2']},{es['co2_scope_3']},{es['co2_total']},{es['stock_ave_elec_use_kwh_m2']},{es['stock_ave_gas_use_kwh_m2']},{es['stock_ave_total_use_kwh_m2']},{es['use_good_bm_elec_use_kwh_m2']},\
                  {es['use_good_bm_gas_use_kwh_m2']},{es['use_good_bm_total_use_kwh_m2']},{es['stock_ave_vs_good_elec_kwh_m2']},{es['stock_ave_vs_good_gas_kwh_m2']},\
                  {es['stock_ave_vs_good_total_kwh_m2']},{es['elec_2b_saved_2get_good']},{es['gas_2b_saved_2get_good']},{es['pc_tot_energy_elec']},{es['pc_tot_energy_gas']},{es['pc_tot_energy_oil']},{es['pc_tot_energy_lpg']},{es['pc_tot_energy_zero_carbon_elec']},{es['pc_tot_energy_zero_carbon_heat']} "
            sql3 = "INSERT INTO controlled_estate_summary (entity_number,total_elec_kwh,total_gas_kwh,total_oil_kwh,total_lpg_kwh,total_solar_pv_kwh,total_solar_thermal_kwh, total_energy_kwh,total_area_m2,\
                  total_estates_portfolio,elec_cost_kwh_gbp,gas_cost_kwh_gbp,average_dec_score,average_dec_rating,co2_scope_1,co2_scope_2,co2_scope_3,co2_total,stock_ave_elec_use_kwh_m2,stock_ave_gas_use_kwh_m2,stock_ave_total_use_kwh_m2,\
                  use_good_bm_elec_use_kwh_m2,use_good_bm_gas_use_kwh_m2,use_good_bm_total_use_kwh_m2,stock_ave_vs_good_elec_kwh_m2,\
                  stock_ave_vs_good_gas_kwh_m2,stock_ave_vs_good_total_kwh_m2,elec_2b_saved_2get_good,gas_2b_saved_2get_good, pc_tot_energy_elec, pc_tot_energy_gas, pc_tot_energy_oil, pc_tot_energy_lpg, pc_tot_energy_zero_carbon_elec, pc_tot_energy_zero_carbon_heat) VALUES("
            sql4 = ");"
            sql  = f"{sql3} {ts} {sql4}"
            
            try:
                
              # Write controlled estate summary results to database - controlled_estate_summary table
              cursor.execute(sql)
              conn.commit()
              anvil.server.task_state['pc_complete'] = "70"
              anvil.server.task_state['status'] = f"Controlled estate summary updated successfully." 
              up_log = up_log + f"---- Controlled estate summary updated successfully \n"
              kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
              time.sleep(5)
              
            except (pyodbc.Error) as e:
              exnum             = 9
              esumdber          = esumdber + 1
              conn.rollback()
              up_log            = up_log + f"Exception number {exnum} - error INSERT into controlled estate summary results for entity - {entity}. DB returned: - \n{e}\n"
              anvil.server.task_state['pc_complete'] = "0"
              anvil.server.task_state['status'] = f"****FAILED - error updating controlled estate summary - see upload log for details"
              kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
              return  
        
  
            # Because annual kWh energy use may have changed run the calculations to update project energy and carbon savings 
            print('>>>>>>About to calc project savings')
            if entity_number != 10:
              ret            = kc.calc_project_energy_carbon_savings_v4(conn, entity_number)
            else:
              ret            = kc.calc_project_energy_carbon_savings_v5_PC01(conn, entity_number)
            print('>>>>>after calc project savings ret=')
            print(ret)
            
            cpef = ret['ef']
            cpem = ret['em']
            
            if cpef == 2:
              up_log = up_log + f"****Error occured calculating project energy and carbon savings \n {cpem}\n"
              anvil.server.task_state['pc_complete'] = "0"
              anvil.server.task_state['status'] = f"****FAILED - Error updating project energy and carbon savings - see upload log for details"
              kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
              return                
            else:
              anvil.server.task_state['pc_complete'] = "80"
              anvil.server.task_state['status'] = f"Calculating project energy and carbon savings completed successfully" 
              up_log = up_log + f"---- Calculating project energy and carbon savings completed successfully \n"
              kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
              
            # Finally recreate the PBI tables for the dashboard
            print('>>>>>>About to create pbi tables')
            ret           =   kc.create_pbi_tables_v3( conn, entity_number)
            print('>>>>After create pbi tables ret=')
            print(ret)
            
            cbef = ret['ef']
            cbem = ret['em']
            
            if cbef == 2:
              up_log = up_log + f"****Error occured creating Power BI tables \n {cbem}\n"
              anvil.server.task_state['pc_complete'] = "0"
              anvil.server.task_state['status'] = f"****FAILED - Error updating Power BI tables - see upload log for details"
              kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
              return                
            else:
              if totalwarn > 0:
                anvil.server.task_state['pc_complete'] = "100"
                anvil.server.task_state['status'] = f"Estate upload completed with {totalwarn} warnings - see upload log for details"
                up_log          = up_log + f"+++++++Estate upload completed with {totalwarn} warnings"
  
              else:
                print('In totalwarn>0 - ELSE')
                up_log          = up_log + f"-------Estate upload has completed successfully"
                anvil.server.task_state['pc_complete'] = "100"
                anvil.server.task_state['status'] = f"Estate upload completed successfully"
                # This is the return when everything has worked ok
              kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
              print('>>>>>uplog as write===============================')
              print(up_log)
              return
          
  except Exception as e:       
    # conn.close()
      msg = "".join(traceback.format_exception(type(e), e, e.__traceback__))
      print('In exception****')
      print(msg)
      exnum             = 12
      up_log            = up_log + f"Exception number {exnum} - ****ERROR - occurred during Estate upload: - \n{msg}\n"
      print ('In exception at end')
      print(up_log)
      anvil.server.task_state['pc_complete'] = "0"
      anvil.server.task_state['status'] = f"****FAILED - Exception occured during Estate upload - see upload log for details"
      kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
      return 

@anvil.server.background_task
def upload_project_initialisation_data_bt(file,entity, partner, client, published, user_name, dt_str, start_date):
# NOTE: For Partner Channel version onwards the project assessment is known to partners as Project Initialisation (although we can't rename all assessment references in the code - too many - so title Project Initialisation 
# is used for labels and titles shown to users.).  
# This is the background task for uploading project initialisation data from an Excel file (.xlsx) where rows are buildings. This version (uk) is valid for the UK realm in which buildings are uniquely identified by their 
# Unique Property Reference Number (UPRN), the unique identifier for every addressable location in the UK (see: https://www.local.gov.uk/uprn).
#
# The uploaded dataframe contains records for a number of buildings identified by their uprn. The uprn's are first checked to ensure they are within 
# valid ranges for the entity and that there are no duplicates (Duplicate uprns in the upload file are not allowed and will cause the upload to terminate
# with an error message.). The buildings (uprns) must already be in the raw_estate_data table. A warning is issued for any uprns that are found without
# a coresponding entry in the raw_estate_data table and these buildings are skipped. 

# For each building there are 23 potential types of remedial work to reduce emissions which are: -
#
#   Fabric Roof, Fabric Windows, Fabric Doors, Fabric (Walls), Pipe Insulation, Heating Controls, LED Lighting,
#   BMS Upgrade/Controls, Variable Speed Drives, Voltage optimisation, Smart Microgrid, Energy Efficient Chillers/Ventilation, Boiler Upgrade, CHP, Heat pump (GAS SAVING),
#   Battery Demand Response, Thermal energy store, Solar PV Power, Solar Thermal, Wind Power, Hydropower, Bioenergy, Heat Network
#
# Each building record shows which of these 24 potential works have been considered as appropriate for the type of building and have been assessed. The results 
# of these assessments are classified as - 'FIRM', 'LIKELY', 'POSSIBLE', 'POTENTIAL', 'IN PLACE', 'FTHR IMPV', 'ASSESSED/NV'. This function fills in the 'assessed_delivery_date'.
# The value of 'assessed_delivery_date' is based on the value in the 'assessed' field and the 'estimated_start_date' (for the programme for this building) which is held in the 'entities' table in Anvil.
# The 'assessed_delivery_date' is calculated when the project is initially setup: -
#
# Assessed value    Delivered within     Value used in assessed_delivery_date
# --------------    ----------------     ------------------------------------
# FIRM              within 1 year         6 months from start date
# LIKELY            1 - 2 years          18 months from start date
# POSSIBLE          2 - 3 years          30 months from start date
# POTENTIAL         3 - 5 years          48 months from start date
# IN PLACE          N/A                  Null - and project status should be set to 'Completed'
# ASSESSED/NV       N/A                  Null - and project status should be set to 'Cancelled'
# FTHR IMPV*        Unknown but assume   48 months from start date
#                   same as POTENTIAL
# * - to be clarified by JH
#
# Note: assessed_delivery_date vs delivery_date_mode - when the Hub writes data for charting etc. in Power BI it needs to write a delivery date. The following rules
# are applied: -
# if delivery_date_mode is '1900-01-01' - use assessed_delivery_date
# if delivery_date_mode is a date - use delivery_date_mode.
#
# After cleansing and validation valid records (buildings) are  processed sequentially and the following process is applied: -
# Step 1 - Each potential remedial work which has been assessed (e.g. Fabric Windows, Fabric Doors etc.) is regarded as a project. An entry is made in the
#          projects table identifying the building by its uprn, the type of project and generating a unique project_id. 
# Step 2 - A new record is added to the projects table keyed by the unique project_id. This record is initialised ready to hold basic project information.
#
# IMPORTANT NOTE: Projects for this building already in the database (having been processed by a previous upload) that are also on this new  
# upload record will have their assessed status updated but will otherwise be un-touched. Projects for this building already in the database 
# which are NOT on this new upload record will be deleted. Projects for this building which are not already in the database will be added to the projects table.
#
  import Project_types
  try:
    print('In load project initialisation data uk bt')
    # Create header for the summary and the log. Initialize upload log and summary messages.
    # dt_str = dd/mm/YY H:M:S
    task_name           = "Upload project initialisation data"
    anvil.server.task_state['pc_complete'] = "0"
    anvil.server.task_state['status'] = f"{task_name} upload starting "
    
    header                 = f"Project initialisation upload by user - {user_name} run on {dt_str} for Partner: {partner}, Client: {client}, Entity: {entity} \n File selected : {file.name}\n Task name - {task_name}\n "
    task_context           = f"/{partner}/{client}/{entity}"
    up_log                 = header

    # Open database connection
      
    conn                   = initialise_database_connection(published) 

    # Retrieve the entity number from entities table
        
    entity_number = anvil.server.call('get_entity_number_v002',entity)
    
    # Get background task ID and initialise log

  # Get background task ID and initialise log
  
    task_id                 = anvil.server.context.background_task_id 
    task_id                 = f"{task_id}{task_context}"
    kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)    
    
    with anvil.media.TempFile(file) as file_name:
   
      if file == None or entity == None:
        up_log              = up_log  + f"++++++++ No file or entity supplied\n"
        anvil.server.task_state['pc_complete'] = "0"
        anvil.server.task_state['status'] = "****FAILED"
        kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
        return 
      else:
        
        # Check sheets named 'Projects', 'Auth' and 'Key' are in workbook
        shn                 = ['Projects','Auth','Key']
        xl                  = pd.ExcelFile(file_name)
        snames              = xl.sheet_names  # see all sheet names
        serr                = 0
        print(f"*****In project initialisation upload - values in snames - {snames}\n")
        for tsn in shn:
          if tsn not in snames:
            serr              = serr + 1
            up_log            = up_log  + f"****Error - cannot find sheet called {tsn} required for project initialisation upload\n"
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            
        if serr > 0:
#          anvil.server.task_state['pc_complete'] = "0"
#          anvil.server.task_state['status'] = f"****FAILED - cannot find sheet called {tsn} required for project initialisation upload"          
          return 
      
        # Read in the Auth dataframe and check Partner, Client and Entity are the same as encrypted in the Key on the Key sheet.. If they are different then raise an error and exit.
        
#      Authenticate_workbook
        auts                = pd.read_excel(file_name, sheet_name = 'Auth', dtype = object) 
        keys                = pd.read_excel(file_name, sheet_name = 'Key', dtype = object)
        ret                 = kc.authenticate_workbook(auts, keys,  partner, client, entity)
        ef                  = ret['ef']
        msg                 = ret['em']

        if ef == 1:
          up_log            = up_log + f"***Workbook authentication error: -\n{msg}\n"
          anvil.server.task_state['pc_complete'] = "0"
          anvil.server.task_state['status'] = f"****FAILED - Workbook authentication error, see upload log for details"
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
          return          
        if ef == 2:
          if 'cryptography.exceptions.InvalidSignature: Signature did not match digest' in msg:
            up_log            = up_log + f"***Invalid key found in workbook - does not correspond to context detils. Possibly the Auths or Key sheets have been edited."
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****FAILED - Invalid key found in workbook - does not correspond to context detils. Possibly the Auths or Key sheets have been edited."
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            return    
          else: 
            up_log            = up_log + f"***Authentication failure - inconsistency between current context and the key stored in the workbook. Possibly attempting to load the workbook to the wrong entity: -\n{msg}\n"
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****Authentication failure - inconsistency between current context and the key stored in the workbook. Possibly attempting to load the workbook to the wrong entity"
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            return        
        
        df                  = pd.read_excel(file_name, sheet_name = 'Projects', dtype = object)
        
        # Validate dataframe column headings (keys)
      
        col_heads_read      = list(df.columns.values)

        valid_col_heads     = ['Building ID', 'Building name', 'Building type', 'Fabric Roof','Fabric Windows','Fabric Doors','Fabric (Walls)',\
                               'Pipe Insulation','Heating Controls','LED Lighting','BMS Upgrade/Controls','Variable Speed Drives','Voltage optimisation','Smart Microgrid',\
                               'Energy Efficient Chillers/Ventilation','Boiler Upgrade','CHP','Heat pump (GAS SAVING)','Battery Demand Response','Thermal energy store',\
                               'Solar PV Power','Solar Thermal','Wind Power','Hydropower','Bioenergy','Heat Network']

        column_xl           = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z","AA","AB"]
      
        len_expected        = len(valid_col_heads)
        len_read            = len(col_heads_read)
      
        if len_read != len_expected:
          up_log            = up_log + f"****ERROR - Mismatch in number of columns found on input file. Expected {len_expected} but found {len_read}\n"
          anvil.server.task_state['pc_complete'] = "0"
          anvil.server.task_state['status'] = f"****FAILED - Mismatch in number of columns found on input file. Expected {len_expected} but found {len_read}"
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
          return 
      
        check1             = f"\nExpected the following column names but did not find them:- \n"                      
        ic                 = -1
        nerr1              = 0
        for c in valid_col_heads:
          if c not in col_heads_read:
            nerr1         = nerr1 + 1
            check1        = check1 + f"{c}, "
            
        if nerr1 > 0:
          up_log          = up_log + f"****Error - missing columns in upload - see upload log for details\n"
          up_log          = up_log + f"{check1}\n"
  
        check2            = f"\nFound the following column names which are not valid:- \n"

        nerr2             = 0
        for c in col_heads_read:
          ic              = ic + 1    
          if c not in valid_col_heads:
            nerr2         = nerr2 + 1
            check2        = check2 + f"{c} in Excel column {column_xl[ic]}\n "
           
        if nerr2 > 0:
          up_log          = up_log + f"****Error - invalid columns found in upload - see upload log for details\n"
          up_log          = up_log + f"{check2}\n"
        
        if nerr1 > 0 or nerr2 > 0:
          anvil.server.task_state['pc_complete'] = "0"
          anvil.server.task_state['status'] = f"****FAILED - invalid columns found in upload"
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
          return 
        
        # Cleansing and validation of input dataframe
        # Replace nans with null in all project type columns
        #df['Full Retrofit']                         = df['Full Retrofit'].fillna('')
        df['Fabric Roof']                           = df['Fabric Roof'].fillna('')
        df['Fabric Windows']                        = df['Fabric Windows'].fillna('')
        df['Fabric Doors']                          = df['Fabric Doors'].fillna('') 
        df['Fabric (Walls)']                        = df['Fabric (Walls)'].fillna('')      
        df['Pipe Insulation']                       = df['Pipe Insulation'].fillna('')      
        df['Heating Controls']                      = df['Heating Controls'].fillna('')
        df['LED Lighting']                          = df['LED Lighting'].fillna('')
        df['BMS Upgrade/Controls']                  = df['BMS Upgrade/Controls'].fillna('')
        df['Variable Speed Drives']                 = df['Variable Speed Drives'].fillna('')
        df['Voltage optimisation']                  = df['Voltage optimisation'].fillna('')
        df['Smart Microgrid']                       = df['Smart Microgrid'].fillna('')
        df['Energy Efficient Chillers/Ventilation'] = df['Energy Efficient Chillers/Ventilation'].fillna('')
        df['Boiler Upgrade']                        = df['Boiler Upgrade'].fillna('')
        df['CHP']                                   = df['CHP'].fillna('')
        df['Heat pump (GAS SAVING)']                = df['Heat pump (GAS SAVING)'].fillna('')
        df['Battery Demand Response']               = df['Battery Demand Response'].fillna('')
        df['Thermal energy store']                  = df['Thermal energy store'].fillna('')
        df['Solar PV Power']                        = df['Solar PV Power'].fillna('')
        df['Solar Thermal']                         = df['Solar Thermal'].fillna('')
        df['Wind Power']                            = df['Wind Power'].fillna('')
        df['Hydropower']                            = df['Hydropower'].fillna('') 
        df['Bioenergy']                             = df['Bioenergy'].fillna('')
        df['Heat Network']                          = df['Heat Network'].fillna('') 
        
        # Replace nans in other columns (Reminder - building_type and building_name are optional - only used to help user 
        # complete the upload form)
        
       # Convert UPRN from text, as read from Excel, to number (BIGINT) for internal processing

        df['Building ID']        = df['Building ID'].astype(int)
        df['Building ID']        = df['Building ID'].fillna(0)
        
        #  Insert column holding row numbers as seen by user in Excel
      
        df.insert(loc=0,column    ='excel_row_num',value = df.reset_index().index + 2)
        num_rows_read             = df.shape[0]
        up_log                    = up_log + f"Number of records read from input file - {num_rows_read}\n"

        # Validate the dataframe
        #{'ef':0,'em':'Validation completed successfully','validated_df':'','validation_messages':'','nvw':0,'nve':0}
        validation          = kc.validate_projects_initialisation_upload(conn, entity, entity_number, df)
        print('Exited from validate')
        ef                  = validation['ef']
        em                  = validation['em']

        if ef > 0:
          up_log            = up_log + f"***Error occurred validating upload file: -\n{em}\n"
          anvil.server.task_state['pc_complete'] = "0"
          anvil.server.task_state['status'] = f"****FAILED - Error occurred validating upload file, see upload log for details"
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
          return

        df                  = validation['validated_df']
        vm                  = validation['validation_messages']
        nvw                 = validation['nvw']
        nve                 = validation['nve']
        up_log              = up_log + f"Validation messages:\n {vm}\n"
        up_log              = up_log+ f"Results of validation: - \n Number of warnings - {nvw}\n Number of errors  - {nve}\n" 
        if nve > 0:
          anvil.server.task_state['pc_complete'] = "0"
          anvil.server.task_state['status'] = f"****FAILED - Upload file has failed validation, see upload log for details. Please correct and re-submit."
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
          return
        if nvw > 0:
          anvil.server.task_state['pc_complete'] = "20"
          anvil.server.task_state['status'] = f"****Warning - Upload file validation has generated warnings, see upload log for details. Please review."
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
        else:
          anvil.server.task_state['pc_complete'] = "20"
          anvil.server.task_state['status'] = f"Upload file validation completed successfully."
          
        # At this point we have a dataframe with at worst warnings and no errors so now we can UPDATE records in the database.          
        # Check each building (uprn) already has a record in the raw_estate_database table. Print warning and remove any which do not comply

        nnotinred      = 0
        ninred         = 0
        nproj          = 0
        nprin          = 0
        
        dft            = df[[ 'Fabric Roof','Fabric Windows','Fabric Doors','Fabric (Walls)',\
                               'Pipe Insulation','Heating Controls','LED Lighting','BMS Upgrade/Controls','Variable Speed Drives','Voltage optimisation','Smart Microgrid',\
                               'Energy Efficient Chillers/Ventilation','Boiler Upgrade','CHP','Heat pump (GAS SAVING)','Battery Demand Response','Thermal energy store',\
                               'Solar PV Power','Solar Thermal','Wind Power','Hydropower','Bioenergy','Heat Network']].copy()

        # Count number of projects in validated df

        for index, row in dft.iterrows():
          rv = row.values
          for c in rv:
            if c != '':
              nprin = nprin + 1

        with conn.cursor() as cursor:
          
          # Retrieve the list of project types (name) and their associated id (id)
          # Note - pt is the import of Project_types module
          
          sql1 = f"SELECT name,project_type_id FROM project_types;"

          cursor.execute(sql1)
          t_name_id  = cursor.fetchall()
          keys       = ("name","project_type_id")
          name_id    = [dict(zip(keys, values)) for values in t_name_id]
         
          # Set up counters
          npdeleted = 0
          npupdated = 0
          npcreated = 0
          
          # Get number of projects in projects table for this entity at start
          
          sqlnp    = f"SELECT * FROM projects WHERE entity_number = {entity_number};"
          cursor.execute(sqlnp)
          npdb     = cursor.fetchall()

          npstart  = len(npdb)
          
          for d in df.to_dict(orient="records"):        

            exists = kc.uprn_exists_in_raw_data(conn, d['Building ID'], entity_number) 

            if exists:
              ninred     = ninred + 1
        
        # Initialise projects for this building.
              res        = kc.initialise_building_projects(conn, d, entity_number, name_id, start_date)
              
              ef         = res['ef']
              em         = res['em']
              npdeleted  = npdeleted + res['npdeleted']
              npupdated  = npupdated + res['npupdated']
              npcreated  = npcreated + res['npcreated']
            
              if ef > 0:
                up_log   = up_log + f"****ERROR - in initialise_building_projects upload will terminate - \n {em} \n"
                anvil.server.task_state['pc_complete'] = "0"
                anvil.server.task_state['status'] = f"****FAILED - exception occured during initialisation of projects. Please see log file for details"
                kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
                return

            else:
              
              nnotinred  = nnotinred + 1
              ferr       = f"++++Warning - building with ID - {d['Building ID']} has not been set up in the database and projects cannot be assigned to it\n"
              up_log     = up_log + ferr
            
          anvil.server.task_state['pc_complete'] = "70"
          anvil.server.task_state['status'] = f"Initialising projects completed successfully"    
          up_log = up_log + f"Initialising projects completed successfully \n" 
          
          cursor.execute(sqlnp)
          npdb     = cursor.fetchall()
          npend    = len(npdb)          

          if nnotinred > 0:
            up_log       = up_log + f"++++Warning - {nnotinred} buildings are not currently set up in the database and projects cannot be set up for them. Please see project upload log for details\n"
          fs             =      f"Number of building records read from upload file : {num_rows_read} \n"
          fs             = fs + f"Number of building records already in database : {ninred} \n"
          fs             = fs + f"Number of building records not found in database : {nnotinred} \n"
          fs             = fs + f"Number of projects found in upload : {nprin} \n"
          fs             = fs + f"Number of projects found in database for entity {entity} at start of upload : {npstart} \n"
          fs             = fs + f"Number of projects already in the database that were deleted : {npdeleted} \n"
          fs             = fs + f"Number of projects already in the database that were updated : {npupdated} \n"
          fs             = fs + f"Number of  new projects that were created                    : {npcreated}\n"          
          fs             = fs + f"Number of projects found in database for entity {entity} at end of upload : {npend} \n"          
          up_log         = up_log  + fs

          #Finally for projects that were deleted or updated run the calculations to update project energy and carbon savings 

          if entity_number != 10:
            ret            = kc.calc_project_energy_carbon_savings_v4(conn, entity_number)
          else:
            ret            = kc.calc_project_energy_carbon_savings_v5_PC01(conn, entity_number)
          if ret['ef'] == 2:
            em = ret['em']
            up_log = up_log + f"****Error occurred calculating project energy and carbon savings \n {em}\n"              
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****FAILED - Error updating project energy and carbon savings - see upload log for details"
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            return                
          else:
            anvil.server.task_state['pc_complete'] = "80"
            anvil.server.task_state['status'] = f"Calculating project energy and carbon savings completed successfully"    
            up_log = up_log + f"Calculating project energy and carbon savings completed successfully \n"  

          ret           =   kc.create_pbi_tables_v3( conn, entity_number) 
          if ret['ef'] == 2:
            em = ret['em']
            up_log = up_log + f"****Error occurred creating Power BI tables \n {em}\n"              
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****FAILED - Error creating Power BI tables - see upload log for details"
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            return                
          else:
            anvil.server.task_state['pc_complete'] = "100"
            anvil.server.task_state['status'] = f"Upload of project initialisation data completed successfully" 
            up_log = up_log + f"Creation of Power BI tables completed successfully \n Upload of project initialisation data completed successfully"
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
  except Exception as e: 
    up_log  = up_log + '**** Error - an exception has occurred - please see upload log for details'
    msg = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    print('In exception****')
    print(msg)    
    up_log  = up_log +f"Exception exit 1 \n {msg}"
    anvil.server.task_state['pc_complete'] = "0"
    anvil.server.task_state['status'] = f"****FAILED - Exception occured during upload of project initialisation data - see upload log for details"
    kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
    return 

def test_co2_emissions(published):

  building_id = 45;
  
  # Open database connection
  
  conn = initialise_database_connection(published)
  
  em = kc.calc_building_co2_emissions(conn, building_id)
  conn.close()

@anvil.server.callable
def export_estate_lite_upload_form_to_excel( entity_number, entity, published, user_name, dt_str, option):

# This subroutine assembles the dataframe containing the data that is required to be written to the estate lite data capture template in Excel.
# The subroutine write_raw_estate_lite_data_to_excel then writes the assembled dataframe to Estate Lite data capture template for downloading by the user.

  ret_mess  = {'ef':0, 'em':0, 'rmedia':''}
  print('entities:-----')
  print(f" Entity number: {entity_number}\n")
  print(f" Entity: {entity}")
  conn                 = initialise_database_connection(app.branch)
  try:
    with conn.cursor() as cur:
    
  # Get required data for all buildings for this entity_number from raw_estate_data table.

      sqlj1                  = f"SELECT entity                               AS entity_id,\
                                        uprn                                 AS building_id, \
                                        building_name                        AS building_name, \
                                        building_type                        AS building_type, \
                                        address                              AS address, \
                                        postcode                             AS postcode, \
                                        latitude_dd                          AS latitude, \
                                        longitude_dd                         AS longitude,\
                                        gia_m2                               AS gia_m2, \
                                        roof_space_m2                        AS roof_space_m2, \
                                        data_year                            AS data_year, \
                                        baseline_annual_elec_kwh             AS annual_electricity_usage,\
                                        baseline_annual_gas_kwh              AS annual_gas_usage,\
                                        baseline_annual_oil_kwh              AS annual_oil_usage,\
                                        baseline_annual_lpg_kwh              AS annual_lpg_usage,\
                                        exist_solar_pv_annual_kwh            AS annual_solar_pv_usage,\
                                        exist_solar_thermal_annual_kwh       AS annual_solar_thermal_usage,\
                                        exist_non_solar_decarb_heat_annual_kwh  AS annual_non_solar_decarb_heat_usage, \
                                        source_of_heating                    AS heating_source, \
                                        source_of_dhw                        AS hot_water_source, \
                                        dec_score                            AS dec_score, \
                                        epc                                  AS epc, \
                                        baseline_elec_cost_per_kwh           AS electricity_cost_per_kwh, \
                                        baseline_gas_cost_per_kwh            AS gas_cost_per_kwh, \
                                        baseline_oil_cost_per_kwh            AS oil_cost_per_kwh, \
                                        baseline_lpg_cost_per_kwh            AS lpg_cost_per_kwh, \
                                        onsite_generation_annual_kwh         AS onsite_kwh_generated_total, \
                                        baseline_annual_cert_green_kwh       AS electricity_kwh_purchased_from_REGO_sources,\
                                        baseline_cert_green_cost_per_kwh     AS rego_electricity_cost_per_kwh \
                                  FROM raw_estate_data WHERE entity_number = {entity_number} ;" 
#      print('sqlj1--------')
#      print(sqlj1)
      
      cur.execute(sqlj1)
      t_output_pr          = cur.fetchall()
      keys                 = ( "entity_id","building_id","building_name","building_type","address","postcode","latitude","longitude","gia_m2","roof_space_m2","data_year",\
                               "annual_electricity_usage","annual_gas_usage","annual_oil_usage","annual_lpg_usage", "annual_solar_pv_usage","annual_solar_thermal_usage","annual_non_solar_decarb_heat_usage",\
                               "heating_source","hot_water_source","dec_score","epc","electricity_cost_per_kwh","gas_cost_per_kwh","oil_cost_per_kwh","lpg_cost_per_kwh",\
                               "onsite_kwh_generated_total","electricity_kwh_purchased_from_REGO_sources","rego_electricity_cost_per_kwh")
      output_pr            =  [dict(zip(keys, values)) for values in t_output_pr]

#      print('*******************************')
#      print('output_pr after fetchall')
#      print(output_pr)
#      print('*******************************')

      if len(output_pr) == 0:
        ret_mess['em'] = "+++Warning - no buildings have been set up for this entity"
        ret_mess['ef'] = 1
        return ret_mess

  # Convert output_pr from dict to pandas dataframe 
      df1                = pd.DataFrame.from_dict (output_pr)
      print('df = ')
      print(df1.to_string())
      
      nr                 = df1.shape[0]
      print(f"Number of rows in df1 : {nr}\n")
 
  # Convert decimal numbers in dataframe to numeric (from fetchall they appear in the dataframe as strings)

      df1['latitude']                                 = pd.to_numeric(df1.latitude)
      df1['longitude']                                = pd.to_numeric(df1.longitude)
      df1['gia_m2']                                   = pd.to_numeric(df1.gia_m2)
      df1['roof_space_m2']                            = pd.to_numeric(df1.roof_space_m2)
      df1['epc']                                      = pd.to_numeric(df1.epc)
      df1['annual_electricity_usage']                 = pd.to_numeric(df1.annual_electricity_usage)
      df1['annual_gas_usage']                         = pd.to_numeric(df1.annual_gas_usage)
      df1['annual_oil_usage']                         = pd.to_numeric(df1.annual_oil_usage)
      df1['annual_lpg_usage']                         = pd.to_numeric(df1.annual_lpg_usage)
      df1['annual_solar_pv_usage']                    = pd.to_numeric(df1.annual_solar_pv_usage)
      df1['annual_solar_thermal_usage']               = pd.to_numeric(df1.annual_solar_thermal_usage)
      df1['annual_non_solar_decarb_heat_usage']       = pd.to_numeric(df1.annual_non_solar_decarb_heat_usage)    
      df1['electricity_cost_per_kwh']                 = pd.to_numeric(df1.electricity_cost_per_kwh)
      df1['gas_cost_per_kwh']                         = pd.to_numeric(df1.gas_cost_per_kwh)
      df1['oil_cost_per_kwh']                         = pd.to_numeric(df1.oil_cost_per_kwh)
      df1['lpg_cost_per_kwh']                         = pd.to_numeric(df1.lpg_cost_per_kwh)
      df1['onsite_kwh_generated_total']               = pd.to_numeric(df1.onsite_kwh_generated_total)
      df1['electricity_kwh_purchased_from_REGO_sources'] = pd.to_numeric(df1.electricity_kwh_purchased_from_REGO_sources)
      df1['rego_electricity_cost_per_kwh']            = pd.to_numeric(df1.rego_electricity_cost_per_kwh)

      # Split dataframe df1 into 2 parts: -
      #   dfl - the 2 columns to the left of the progress column
      #   dfr - the 28 columns to the right of the progress column

      dfl                = df1.iloc[:,[0,1]]
      dfr                = df1.iloc[:,[2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28]]

#      print('dfl = ')
#      print(dfl.to_string())
#      print('dfr = ')
#      print(dfr.to_string())
      
      # Write dataframes to Excel 
      ret                = kc.write_raw_estate_lite_data_to_excel(dfl, dfr, entity, entity_number, dt_str)
    
      ret_mess['ef']     = ret['ef']
      ret_mess['em']     = ret['em']
      ret_mess['rmedia'] = ret['rmedia']
    return ret_mess
  except Exception as e:
    ret_mess['ef'] = 1
    msg = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    ret_mess['em'] = msg
    print(msg)
  return ret_mess

@anvil.server.callable
def export_estate_upload_form_to_excel( entity_number, entity, partner, client, published, user_name, dt_str, option):

# This subroutine assembles the dataframe containing the data that is required to be written to the estate data capture form in Excel.
# The subroutine write_raw_estate_data_to_excel then writes the assembled dataframe to an excel file for downloading by the user.

  ret_mess  = {'ef':0, 'em':0, 'rmedia':''}
  
  conn                 = initialise_database_connection(app.branch)
  try:
    with conn.cursor() as cur:
    
  # Get required data for all buildings for this entity_number from raw_estate_data table which will have been partially populated by the estate initialisation job
  # (uprn and building_name).

      sqlj1                  = f"SELECT uprn                                 AS uprn, \
                                        building_name                        AS building_name, \
                                        building_type                        AS building_type, \
                                        address                              AS address, \
                                        postcode                             AS postcode, \
                                        under_control                        AS under_control, \
                                        remain_in_portfolio                  AS remain_in_portfolio, \
                                        listed                               AS listed, \
                                        construction_year                    AS construction_year, \
                                        gia_m2                               AS gia_m2, \
                                        roof_space_m2                        AS roof_space_m2, \
                                        data_year                            AS data_year, \
                                        baseline_annual_elec_kwh             AS baseline_annual_elec_kwh,\
                                        baseline_annual_gas_kwh              AS baseline_annual_gas_kwh,\
                                        baseline_annual_oil_kwh              AS baseline_annual_oil_kwh,\
                                        baseline_annual_lpg_kwh              AS baseline_annual_lpg_kwh,\
                                        source_of_heating                    AS source_of_heating, \
                                        source_of_dhw                        AS source_of_dhw, \
                                        dec_score                            AS dec_score, \
                                        epc                                  AS epc, \
                                        baseline_elec_cost_per_kwh           AS baseline_elec_cost_per_kwh, \
                                        baseline_gas_cost_per_kwh            AS baseline_gas_cost_per_kwh, \
                                        baseline_oil_cost_per_kwh            AS baseline_oil_cost_per_kwh, \
                                        baseline_lpg_cost_per_kwh            AS baseline_lpg_cost_per_kwh,"
      
      sqlj2                  =        f"onsite_generation_annual_kwh         AS onsite_generation_annual_kwh, \
                                        exist_solar_pv_annual_kwh            AS exist_solar_pv_annual_kwh,\
                                        exist_solar_thermal_annual_kwh       AS exist_solar_thermal_annual_kwh,\
                                        exist_non_solar_decarb_heat_annual_kwh  AS exist_non_solar_decarb_heat_annual_kwh, \
                                        car_park_available                   AS car_park_available, \
                                        number_of_ev_charge_sockets          AS number_of_ev_charge_sockets, \
                                        charging_capacity_kwh                AS charging_capacity_kwh \
                                  FROM raw_estate_data WHERE entity_number = {entity_number} ;" 
      sqlj3                = sqlj1 + sqlj2                            
      print(sqlj3)                            

      cur.execute(sqlj3)
      t_output_pr          = cur.fetchall()
      keys                 = ( "uprn","building_name","building_type","address","postcode","under_control","remain_in_portfolio","listed","construction_year",\
                               "gia_m2","roof_space_m2","data_year","baseline_annual_elec_kwh","baseline_annual_gas_kwh","baseline_annual_oil_kwh","baseline_annual_lpg_kwh",\
                               "source_of_heating","source_of_dhw","dec_score","epc","baseline_elec_cost_per_kwh","baseline_gas_cost_per_kwh","baseline_oil_cost_per_kwh","baseline_lpg_cost_per_kwh",\
                               "onsite_generation_annual_kwh","exist_solar_pv_annual_kwh","exist_solar_thermal_annual_kwh", "exist_non_solar_decarb_heat_annual_kwh","car_park_available","number_of_ev_charge_sockets","charging_capacity_kwh")
      output_pr            =  [dict(zip(keys, values)) for values in t_output_pr]

      print('*******************************')
      print('output_pr after fetchall')
      print(output_pr)
      print('*******************************')
      
      if len(output_pr) == 0:
        ret_mess['em'] = "+++Warning - no buildings have been set up for this entity"
        ret_mess['ef'] = 1
        columns = ["uprn","building_name","building_type","address","postcode","under_control","remain_in_portfolio","listed","construction_year",\
                               "gia_m2","roof_space_m2","data_year","baseline_annual_elec_kwh","baseline_annual_gas_kwh","baseline_annual_oil_kwh","baseline_annual_lpg_kwh",\
                               "source_of_heating","source_of_dhw","dec_score","epc","baseline_elec_cost_per_kwh","baseline_gas_cost_per_kwh","baseline_oil_cost_per_kwh","baseline_lpg_cost_per_kwh",\
                               "onsite_generation_annual_kwh","exist_solar_pv_annual_kwh","exist_solar_thermal_annual_kwh", "exist_non_solar_decarb_heat_annual_kwh","car_park_available","number_of_ev_charge_sockets","charging_capacity_kwh"
                               ]
        df1 = pd.DataFrame(columns = columns)        

  # Convert output_pr from dict to pandas dataframe
      else:
        df1                = pd.DataFrame.from_dict (output_pr)
      
      print('df initialisation= ')
      print(df1.to_string())

      nr                 = df1.shape[0]
      
  # Insert 2 columns at front to hold 'action' and 'delete_reason'
  
      df1.insert(0,'delete_reason',"",allow_duplicates = False)
      df1.insert(0,'action',"UPDATE",allow_duplicates = False)      
 
  # Put the columns in the order we want them to appear in Excel 
  
      df1                = df1[['action','delete_reason','uprn','building_name','building_type','address','postcode','under_control','remain_in_portfolio','listed','construction_year',\
                                'gia_m2','roof_space_m2','data_year','baseline_annual_elec_kwh','baseline_annual_gas_kwh','baseline_annual_oil_kwh','baseline_annual_lpg_kwh',\
                                'source_of_heating','source_of_dhw','dec_score','epc','baseline_elec_cost_per_kwh','baseline_gas_cost_per_kwh','baseline_oil_cost_per_kwh','baseline_lpg_cost_per_kwh',\
                                'onsite_generation_annual_kwh','exist_solar_pv_annual_kwh','exist_solar_thermal_annual_kwh', 'exist_non_solar_decarb_heat_annual_kwh','car_park_available','number_of_ev_charge_sockets','charging_capacity_kwh']]

  # Convert decimal numbers in dataframe to numeric (from fetchall they appear in the dataframe as strings)

      df1['gia_m2']                                   = pd.to_numeric(df1.gia_m2)
      df1['roof_space_m2']                            = pd.to_numeric(df1.roof_space_m2)
      df1['epc']                                      = pd.to_numeric(df1.epc)
      df1['baseline_annual_elec_kwh']                 = pd.to_numeric(df1.baseline_annual_elec_kwh)
      df1['baseline_annual_gas_kwh']                  = pd.to_numeric(df1.baseline_annual_gas_kwh)
      df1['baseline_annual_oil_kwh']                  = pd.to_numeric(df1.baseline_annual_oil_kwh)
      df1['baseline_annual_lpg_kwh']                  = pd.to_numeric(df1.baseline_annual_lpg_kwh)
      df1['baseline_elec_cost_per_kwh']               = pd.to_numeric(df1.baseline_elec_cost_per_kwh)
      df1['baseline_gas_cost_per_kwh']                = pd.to_numeric(df1.baseline_gas_cost_per_kwh)
      df1['baseline_oil_cost_per_kwh']                = pd.to_numeric(df1.baseline_oil_cost_per_kwh)
      df1['baseline_lpg_cost_per_kwh']                = pd.to_numeric(df1.baseline_lpg_cost_per_kwh)
      df1['onsite_generation_annual_kwh']             = pd.to_numeric(df1.onsite_generation_annual_kwh)
      df1['exist_non_solar_decarb_heat_annual_kwh']   = pd.to_numeric(df1.exist_non_solar_decarb_heat_annual_kwh)
      df1['exist_solar_pv_annual_kwh']                = pd.to_numeric(df1.exist_solar_pv_annual_kwh)
      df1['exist_solar_thermal_annual_kwh']           = pd.to_numeric(df1.exist_solar_thermal_annual_kwh)
      df1['charging_capacity_kwh']                    = pd.to_numeric(df1.charging_capacity_kwh)
      
  # Get the building types from the benchmark table and put in a dataframe

      sqlbt              = f"SELECT building_type FROM {bm.benchmark_table_name};"
      cur.execute(sqlbt)
      t_output_bt          = cur.fetchall()
      keys                 = ( "building_type" )
      output_bt            =  [dict(zip(keys, values)) for values in t_output_bt]
    
      if len(output_bt) == 0:
        ret_mess['em'] = "+++Warning - no buildings types found"
        ret_mess['ef'] = 1
        return ret_mess
  # Convert output_bt from dict to pandas dataframe 
      dfbt                 = pd.DataFrame.from_dict (output_bt) 

      print('In export before write to excel')
      print(df1.to_string())
      # Write dataframes to Excel 
      ret                = kc.write_raw_estate_data_to_excel(df1, dfbt, entity)
    
      ret_mess['ef']     = ret['ef']
      ret_mess['em']     = ret['em']
      ret_mess['rmedia'] = ret['abm']
    return ret_mess
  except Exception as e:
    ret_mess['ef'] = 1
    msg = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    ret_mess['em'] = msg
    print(msg)
  return ret_mess

@anvil.server.callable
def export_estate_upload_form_to_excel_PC_01( entity_number, entity, partner, client, published, user_name, dt_str, option):
# Partner channel version
# This subroutine assembles the dataframe containing the data that is required to be written to the estate data capture form in Excel.
# The subroutine write_raw_estate_data_to_excel_PC_01 then writes the assembled dataframe to an excel file for downloading by the user.

  ret_mess  = {'ef':0, 'em':0, 'rmedia':''}
  
  conn                 = initialise_database_connection(app.branch)
  try:
    with conn.cursor() as cur:
    
  # Get required data for all buildings for this entity_number from raw_estate_data table 

      sqlj1  = f"SELECT uprn                      AS 'Building ID', \
             building_name                        AS 'Building Name', \
             building_type                        AS 'Building Type (choose from drop down)', \
             address                              AS 'Address', \
             postcode                             AS 'Postcode', \
             latitude_dd                          AS 'Latitude (decimal degrees)', \
             longitude_dd                         AS 'Longitude (decimal degrees)',"            
      sqlj2  = f" gia_m2                          AS 'Gross Internal Area (square metres)', \
             roof_space_m2                        AS 'Roof space (square metres)', \
             data_year                            AS 'Year (enter the year the energy usage relates to)', \
             baseline_annual_elec_kwh             AS 'Annual Electricity Usage (kWh)',\
             baseline_annual_gas_kwh              AS 'Annual Gas Usage (kWh)',\
             baseline_annual_oil_kwh              AS 'Annual Oil Usage (kWh)',\
             baseline_annual_lpg_kwh              AS 'Annual LPG Usage (kWh)',\
             onsite_generation_annual_kwh         AS 'Total onsite generation (kWh)', \
             exist_solar_pv_annual_kwh            AS 'Annual Solar PV usage (kWh)',\
             exist_solar_thermal_annual_kwh       AS 'Annual Solar Thermal usage (kWh)',\
             baseline_annual_cert_green_kwh       AS 'Electricity purchased from REGO sources (kWh)', \
             exist_non_solar_decarb_heat_annual_kwh  AS 'Annual non-solar decarbonised heat usage (kWh)', \
             baseline_elec_cost_per_kwh           AS 'Electricity cost per kWh', \
             baseline_gas_cost_per_kwh            AS 'Gas cost per kWh', \
             baseline_oil_cost_per_kwh            AS 'Oil cost per kWh', \
             baseline_lpg_cost_per_kwh            AS 'LPG cost per kWh',\
             baseline_cert_green_cost_per_kwh     AS 'REGO electricity cost per kWh',\
             source_of_heating                    AS 'Heating source', \
             source_of_dhw                        AS 'Hot water source', \
             dec_score                            AS 'DEC Score', \
             epc                                  AS 'EPC Score' \
             FROM raw_estate_data WHERE entity_number = {entity_number} ;" 
      sqlj3                = sqlj1 + sqlj2                            
      print(sqlj3)                            

      cur.execute(sqlj3)
      t_output_pr          = cur.fetchall()
      keys                 = ("Building ID","Building Name","Building Type (choose from drop down)", "Address", "Postcode","Latitude (decimal degrees)","Longitude (decimal degrees)",         
      "Gross Internal Area (square metres)","Roof space (square metres)","Year (enter the year the energy usage relates to)",
      "Annual Electricity Usage (kWh)","Annual Gas Usage (kWh)","Annual Oil Usage (kWh)","Annual LPG Usage (kWh)","Total onsite generation (kWh)", 
      "Annual Solar PV usage (kWh)","Annual Solar Thermal usage (kWh)","Electricity purchased from REGO sources (kWh)", "Annual non-solar decarbonised heat usage (kWh)", 
      "Electricity cost per kWh", "Gas cost per kWh", "Oil cost per kWh","LPG cost per kWh","REGO electricity cost per kWh",
      "Heating source", "Hot water source", "DEC Score", "EPC Score" ) 
      output_pr            =  [dict(zip(keys, values)) for values in t_output_pr]

      print('*******************************')
      print('output_pr after fetchall')
      print(output_pr)
      print('*******************************')
      
      columns = ["Building ID","Building Name","Building Type (choose from drop down)", "Address", "Postcode","Latitude (decimal degrees)","Longitude (decimal degrees)",            
                   "Gross Internal Area (square metres)","Roof space (square metres)","Year (enter the year the energy usage relates to)", 
                   "Annual Electricity Usage (kWh)","Annual Gas Usage (kWh)","Annual Oil Usage (kWh)","Annual LPG Usage (kWh)","Total onsite generation (kWh)", 
                   "Annual Solar PV usage (kWh)","Annual Solar Thermal usage (kWh)","Electricity purchased from REGO sources (kWh)", "Annual non-solar decarbonised heat usage (kWh)", 
                   "Electricity cost per kWh", "Gas cost per kWh", "Oil cost per kWh","LPG cost per kWh","REGO electricity cost per kWh",
                   "Heating source", "Hot water source", "DEC Score", "EPC Score" ]
      
      if len(output_pr) == 0:
        ret_mess['em'] = "+++Warning - no buildings have been set up for this entity"
        ret_mess['ef'] = 1

        dft              = pd.DataFrame(columns = columns)

  # Convert output_pr from dict to pandas dataframe
      else:
        dft              = pd.DataFrame.from_dict (output_pr)

      ndata            = [[None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]]
      dfb              = pd.DataFrame(data = ndata, columns = columns) # Contains 1 row of Nulls - to be concatenated to te end of df1 so resulting dataframe
                                                                       # can never be empty - if it is xlswriter won't create an Excel Table
      df1             = pd.concat([dft, dfb])
      print('df initialisation= ')
      
      nr                 = df1.shape[0]
      
  # Put the columns in the order we want them to appear in Excel 
  
      df1                = df1[['Building ID','Building Name','Building Type (choose from drop down)', 'Address', 'Postcode','Latitude (decimal degrees)','Longitude (decimal degrees)',            
                                'Gross Internal Area (square metres)','Roof space (square metres)','Year (enter the year the energy usage relates to)',
                                'Annual Electricity Usage (kWh)','Annual Gas Usage (kWh)','Annual Oil Usage (kWh)','Annual LPG Usage (kWh)','Total onsite generation (kWh)', 
                                'Annual Solar PV usage (kWh)','Annual Solar Thermal usage (kWh)','Electricity purchased from REGO sources (kWh)', 'Annual non-solar decarbonised heat usage (kWh)',
                                'Electricity cost per kWh', 'Gas cost per kWh', 'Oil cost per kWh','LPG cost per kWh','REGO electricity cost per kWh',
                                'Heating source', 'Hot water source', 'DEC Score', 'EPC Score']]
      print(df1.to_string())
#      ret_mess['em'] = 'Return on request from export_estate_upload_form_to_excel_PC_01'
#      print(ret_mess['em'])
#      return ret_mess
  # Convert decimal numbers in dataframe to numeric (from fetchall they appear in the dataframe as strings)


      df1['Latitude (decimal degrees)']                             = pd.to_numeric(df1['Latitude (decimal degrees)'])
      df1['Longitude (decimal degrees)']                             = pd.to_numeric(df1['Longitude (decimal degrees)'])
      df1['Gross Internal Area (square metres)']                    = pd.to_numeric(df1['Gross Internal Area (square metres)'])
      df1['Roof space (square metres)']                             = pd.to_numeric(df1['Roof space (square metres)'])
      df1['Year (enter the year the energy usage relates to)']      = pd.to_numeric(df1['Year (enter the year the energy usage relates to)'])
      df1['Annual Electricity Usage (kWh)']                         = pd.to_numeric(df1['Annual Electricity Usage (kWh)'])
      df1['Annual Gas Usage (kWh)']                                 = pd.to_numeric(df1['Annual Gas Usage (kWh)'])
      df1['Annual Oil Usage (kWh)']                                 = pd.to_numeric(df1['Annual Oil Usage (kWh)'])
      df1['Annual LPG Usage (kWh)']                                 = pd.to_numeric(df1['Annual LPG Usage (kWh)'])
      df1['Total onsite generation (kWh)']                          = pd.to_numeric(df1['Total onsite generation (kWh)'])
      df1['Annual Solar PV usage (kWh)']                            = pd.to_numeric(df1['Annual Solar PV usage (kWh)'])
      df1['Annual Solar Thermal usage (kWh)']                       = pd.to_numeric(df1['Annual Solar Thermal usage (kWh)'])
      df1['Electricity purchased from REGO sources (kWh)']          = pd.to_numeric(df1['Electricity purchased from REGO sources (kWh)'])
      df1['Annual non-solar decarbonised heat usage (kWh)']         = pd.to_numeric(df1['Annual non-solar decarbonised heat usage (kWh)'])
      df1['Electricity cost per kWh']                               = pd.to_numeric(df1['Electricity cost per kWh'])
      df1['Gas cost per kWh']                                       = pd.to_numeric(df1['Gas cost per kWh'])
      df1['Oil cost per kWh']                                       = pd.to_numeric(df1['Oil cost per kWh'])
      df1['LPG cost per kWh']                                       = pd.to_numeric(df1['LPG cost per kWh'])
      df1['REGO electricity cost per kWh']                          = pd.to_numeric(df1['REGO electricity cost per kWh'])     
      df1['EPC Score']                                              = pd.to_numeric(df1['EPC Score'])
      df1['DEC Score']                                              = pd.to_numeric(df1['DEC Score'])

      print(f"\n ++++++++df1 after to_numeric: -\n {df1.to_string()} \n")
  # Get the building types from the benchmark table and put in a dataframe

      sqlbt              = f"SELECT building_type FROM {bm.benchmark_table_name};"
      cur.execute(sqlbt)
      t_output_bt          = cur.fetchall()
      keys                 = ( "building_type" )
      output_bt            =  [dict(zip(keys, values)) for values in t_output_bt]
    
      if len(output_bt) == 0:
        ret_mess['em'] = "+++Warning - no buildings types found"
        ret_mess['ef'] = 1
        return ret_mess
  # Convert output_bt from dict to pandas dataframe 
      dfbt                 = pd.DataFrame.from_dict (output_bt) 

      print('In export before write to excel')
      print(df1.to_string())
      # Write dataframes to Excel 
  #    ret                = kc.write_raw_estate_data_to_excel(df1, dfbt, entity)
      ret                = kc.write_raw_estate_data_to_excel_PC_01(df1, dfbt, entity, partner, client)
      ret_mess['ef']     = ret['ef']
      ret_mess['em']     = ret['em']
      ret_mess['rmedia'] = ret['abm']
    return ret_mess
  except Exception as e:
    ret_mess['ef'] = 1
    msg = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    ret_mess['em'] = msg
    print(msg)
  return ret_mess


@anvil.server.callable
def export_project_details_form_to_excel( entity_number, entity, partner, client, published, user_name, dt_str, option):

# This subroutine assembles the dataframe containing the data that is required to be written to the project details data capture form in Excel.
# The subroutine write_project_details_to_excel then writes the assembled dataframe to an excel file for downloading by the user.

  ret_mess  = {'ef':0, 'em':0, 'rmedia':''}
  
  conn                 = initialise_database_connection(app.branch)
  try:
    with conn.cursor() as cur:
    
  # Get data for all projects for this entity_number from projects table.

      sqlj               = f"SELECT  uprn                                AS uprn, \
                                   project_type_id                     AS type_id, \
                                   baselined                           AS baselined, \
                                   assessed                            AS assessed, \
                                   project_status                      AS status, \
                                   utility                             AS utility, \
                                   salix_pf                            AS persistence_factor, \
                                   saving_percent                      AS saving_percent, \
                                   hp_scop                             AS hp_scop,\
                                   hp_elec_add_kwh_pa                  AS hp_elec_add_kwh_pa,\
                                   cost_capex_mode                     AS capex, \
                                   delivery_date_mode                  AS delivery_date, \
                                   solar_angle                         AS solar_angle, \
                                   solar_roof_type                     AS solar_roof_type, \
                                   solar_thermal_area_m2               AS solar_thermal_area_m2, \
                                   solar_pv_area_m2                    AS solar_pv_area_m2, \
                                   solar_kw_peak                       AS solar_kw_peak, \
                                   solar_thermal_corrected_ann_gen_kwh AS solar_thermal_corrected_ann_gen_kwh, \
                                   solar_pv_corrected_ann_gen_kwh      AS solar_pv_corrected_ann_gen_kwh \
                           FROM projects WHERE entity_number = {entity_number} AND baselined = 'NO' AND assessed != 'IN PLACE' AND assessed != 'ASSESSED/NV';" 
  
      print(sqlj) 
      cur.execute(sqlj)
      t_output_pr          = cur.fetchall()
      keys                 = ( "uprn","type_id","baselined","assessed","status","utility","persistence_factor","saving_percent","hp_scop","hp_elec_add_kwh_pa","capex","delivery_date","solar_angle","solar_roof_type","solar_thermal_area_m2","solar_pv_area_m2","solar_kw_peak","solar_thermal_corrected_ann_gen_kwh","solar_pv_corrected_ann_gen_kwh")
      output_pr            =  [dict(zip(keys, values)) for values in t_output_pr]
      
      if len(output_pr) == 0:
        ret_mess['em'] = "+++Warning - no projects have been set up for this entity"
        ret_mess['ef'] = 1
        return ret_mess

  # Get the list of project types
  
      sqlt              = f"SELECT project_type_id, name FROM project_types;"
      cur.execute(sqlt)
      t_output_pt       = cur.fetchall()
      keys              = ("project_type_id","name")
      output_pt         = [dict(zip(keys, values)) for values in t_output_pt]
      
  # Get uprn, building name and building type for this entity
  
      sqlb              = f"SELECT uprn, building_name, building_type FROM raw_estate_data WHERE entity_number = {entity_number} AND under_control = 'Yes';"
      cur.execute(sqlb)
      t_output_bl       = cur.fetchall()
      keys              = ("uprn","building_name","building_type")
      output_bl         = [dict(zip(keys, values)) for values in t_output_bl]
      
      print('In export_project_details_form_to_excel ')
  # Convert output_pr from dict to pandas dataframe 
      df1                = pd.DataFrame.from_dict (output_pr)
      print('df = ')
      print(df1.to_string())

      nr                 = df1.shape[0]
  # Insert columns to hold building_name, building_type and project_type
  
      df1                = df1.assign(building_name = [''] * nr,
                                    building_type = [''] * nr,
                                    project_type  = [''] * nr)

  # Insert building_name, building_type and project_type (name) 

      for index, row in df1.iterrows():
        uprn             = row['uprn']
        ptypeid          = row['type_id']
      
        for n in output_bl:
          if n['uprn'] == uprn:
            row['building_name']  = n['building_name']
            row['building_type']  = n['building_type']
            break
        for n in output_pt:
          if n['project_type_id'] == ptypeid:
            row['project_type']   = n['name']
            break
        df1.iloc[index]       = row
      
  # Sort the dataframe by uprn to get all projects for buildings together

      df1                = df1.sort_values(by=['uprn','utility'])
    
  # Put the columns in the order we want them to appear in Excel 
  
      df1                = df1[['uprn','building_name','building_type','project_type', 'baselined', 'assessed', 'status', 'utility', 'persistence_factor', 
                              'saving_percent', 'hp_scop','hp_elec_add_kwh_pa','capex', 'delivery_date', 'solar_roof_type', 'solar_angle', 'solar_pv_area_m2', 
                              'solar_kw_peak', 'solar_pv_corrected_ann_gen_kwh','solar_thermal_area_m2', 'solar_thermal_corrected_ann_gen_kwh']]
  
  # Remove projects where assessed is either 'IN PLACE' or 'ASSESSED/NV'
      print('Before assessed filter')
      print(df1.to_string())
      df1                = df1.loc[(df1['assessed'] != 'IN PLACE') & (df1['assessed'] != 'ASSESSED/NV')]
      print('After assessed filter')
      print(df1.to_string())    
  # Separate out the projects into 3 dataframes - dfnonsolar (all non solar projects), dfsolarpv (solar PV projects) and dfsolarthermal (solar thermal projects)
  
      dfnonsolar         = df1.loc[(df1['project_type'] != 'Solar PV Power') & (df1['project_type'] != 'Solar Thermal')]
      dfsolarpv          = df1.loc[(df1['project_type'] == 'Solar PV Power')]
      dfsolarthermal     = df1.loc[(df1['project_type'] == 'Solar Thermal')]
 
  # Select the columns for each dataframe
  
      dfnonsolar         = dfnonsolar[['uprn','building_name','building_type','project_type', 'baselined', 'assessed', 'status', 'utility', 'persistence_factor', 
                              'saving_percent', 'hp_scop','hp_elec_add_kwh_pa','capex', 'delivery_date']]

  # Convert decimal numbers in dataframe to numeric (from fetchall they appear in the dataframe as strings)

      dfnonsolar['persistence_factor']                   = pd.to_numeric(dfnonsolar.persistence_factor)      
      dfnonsolar['saving_percent']                       = pd.to_numeric(dfnonsolar.saving_percent) 
      dfnonsolar['hp_scop']                              = pd.to_numeric(dfnonsolar.hp_scop) 
      dfnonsolar['hp_elec_add_kwh_pa']                   = pd.to_numeric(dfnonsolar.hp_elec_add_kwh_pa) 
     
      print('non solar')
      print(dfnonsolar.to_string())
   
      dfsolarpv          = dfsolarpv[['uprn','building_name','building_type','project_type', 'baselined', 'assessed', 'status', 'utility', 'persistence_factor', 
                                  'capex', 'delivery_date', 'solar_roof_type', 'solar_angle', 'solar_pv_area_m2', 'solar_kw_peak', 'solar_pv_corrected_ann_gen_kwh' ]]

  # Convert decimal numbers in dataframe to numeric (from fetchall they appear in the dataframe as strings)

      dfsolarpv['persistence_factor']                   = pd.to_numeric(dfsolarpv.persistence_factor)      
      dfsolarpv['solar_pv_area_m2']                     = pd.to_numeric(dfsolarpv.solar_pv_area_m2)       
      dfsolarpv['solar_kw_peak']                        = pd.to_numeric(dfsolarpv.solar_kw_peak)
      dfsolarpv['solar_pv_corrected_ann_gen_kwh']       = pd.to_numeric(dfsolarpv.solar_pv_corrected_ann_gen_kwh)
      
      print('solarpv')
      print(dfsolarpv.to_string())
    
      dfsolarthermal     = dfsolarthermal[['uprn','building_name','building_type','project_type', 'baselined', 'assessed', 'status', 'utility', 'persistence_factor', 
                                  'capex', 'delivery_date','solar_thermal_area_m2', 'solar_thermal_corrected_ann_gen_kwh']]

  # Convert decimal numbers in dataframe to numeric (from fetchall they appear in the dataframe as strings)
      
      dfsolarthermal['persistence_factor']                  = pd.to_numeric(dfsolarthermal.persistence_factor)
      dfsolarthermal['solar_thermal_area_m2']               = pd.to_numeric(dfsolarthermal.solar_thermal_area_m2)
      dfsolarthermal['solar_thermal_corrected_ann_gen_kwh'] = pd.to_numeric(dfsolarthermal.solar_thermal_corrected_ann_gen_kwh)
      
      print('solarthermal')
      print(dfsolarthermal.to_string())
    
  # Write dataframe to Excel
      ret                = kc.write_project_details_to_excel(dfnonsolar, dfsolarpv, dfsolarthermal, entity, partner, client)
    
      ret_mess['ef']     = ret['ef']
      ret_mess['em']     = ret['em']
      ret_mess['rmedia'] = ret['abm']
    return ret_mess
  except Exception as e:
    ret_mess['ef'] = 1
    msg = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    ret_mess['em'] = msg
    print(msg)
  return ret_mess

@anvil.server.callable
def export_project_initialisation_form_to_excel( entity_number, entity, partner, client, published, user_name, dt_str, option):

# This subroutine assembles the dataframe containing the data that is required to be written to the project initialisation data capture form in Excel.
# The subroutine write_project_initialisation_to_excel then writes the assembled dataframe to an excel file for downloading by the user.

  ret_mess  = {'ef':0, 'em':0, 'rmedia':''}
  
  conn                 = initialise_database_connection(app.branch)
  try:
    with conn.cursor() as cur:
    
  # Get data for all projects for this entity_number from projects table.

      sqlj               = f"SELECT  pr.uprn                                AS uprn, \
                                     pr.project_type_id                     AS type_id, \
                                     pr.assessed                            AS assessed, \
                                     pt.name                                AS project_type \
                           FROM projects AS pr \
                           LEFT JOIN project_types AS pt \
                           ON pr.project_type_id = pt.project_type_id \
                           WHERE pr.entity_number = {entity_number}  ;" 
  
      print(sqlj) 
      cur.execute(sqlj)
      t_output_pr          = cur.fetchall()
      keys                 = ( "uprn","type_id","assessed","project_type")
      output_pr            =  [dict(zip(keys, values)) for values in t_output_pr]
      
  # Convert output_pr from dict to pandas dataframe 
      df1                = pd.DataFrame.from_dict (output_pr)

      npr                = df1.shape[0]

  # Sort the project data dataframe by uprn 
      if npr > 0:
        df1                = df1.sort_values(by=['uprn'])
        df1                = df1.fillna('')

  # Get list of buildings for this entity to build the output table for Excel

      sqlb             = f"SELECT uprn, building_name, building_type FROM raw_estate_data WHERE entity_number = {entity_number};"
      cur.execute(sqlb)
      t_output_bl      = cur.fetchall()
      keys             = ("uprn", "building_name", "building_type")
      output_bl        = [dict(zip(keys, values)) for values in t_output_bl]

  # Convert output_bl from dict to pandas dataframe 
      dfbl               = pd.DataFrame.from_dict (output_bl)

      nb                 = dfbl.shape[0]
      if nb == 0:
        ret_mess['em'] = "****Error - no buildings have been set up for this entity - unable to create a project assessment sheet."
        ret_mess['ef'] = 2
        return ret_mess
        
  # Sort the dataframe by uprn 

      dfbl               = dfbl.sort_values(by=['uprn'])

 # Get the list of project types for column headers
  
      sqlt              = f"SELECT project_type_id, name FROM project_types;"
      cur.execute(sqlt)
      t_output_pt       = cur.fetchall()
      keys              = ("project_type_id","name")
      output_pt         = [dict(zip(keys, values)) for values in t_output_pt]
      project_type_ids, project_type_names = zip(*(d.values() for d in output_pt))

  # Create list of project type names for column headers for output dataframe and append to the column headers for building details

      ptl               = list(project_type_names)
      blh               = ["Building ID", "Building name", "Building type"]
      outh              = blh + ptl

  # Create the output dataframe (dfout) 

      dfout             = pd.DataFrame(columns = outh)

      dfout['Building ID']   = dfbl['uprn']
      dfout['Building name'] = dfbl['building_name']
      dfout['Building type'] = dfbl['building_type']

      # Convert NaNs to Nulls
      
      dfout              = dfout.fillna('')
     # print('dfout with populated building info: -')
     # print(dfout.to_string())

  # Iterate through the list of projects in df1 and populate the appropriate cells in dfout with the value os 'assessed'
      print(f"\n Printing projects df1: - \n")
      for index, row in df1.iterrows():
        uprn              = row['uprn']
        type_id           = row['type_id']
        assessed          = row['assessed']
        project_type      = row['project_type']

       # Find the row (uprn) and column (type_id) in dfout to populate with the assessed value

        r = dfout.index[dfout['Building ID'] == uprn]
        dfoutrow = dfout.iloc[r]
        dfoutrow[project_type] = assessed
        dfout.iloc[r]          = dfoutrow

  # Write dataframe to Excel
      ret                = kc.write_project_initialisation_to_excel(dfout,entity, partner, client )
    
      ret_mess['ef']     = ret['ef']
      ret_mess['em']     = ret['em']
      ret_mess['rmedia'] = ret['abm']
    return ret_mess
  
  except Exception as e:
    ret_mess['ef'] = 1
    msg = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    ret_mess['em'] = msg
    print(msg)
  return ret_mess

@anvil.server.callable
def get_entity_number_v002(entity_code):
  print('=================In get_entity_number_v002 - dbconnection:')

  # Open database connection

  conn = initialise_database_connection(app.branch)

  esql = f"SELECT entity_number FROM entities WHERE entity_id = \'{entity_code}\';"
  print(esql)
  with conn.cursor() as cur:
    cur.execute(esql)
    tenum     = cur.fetchall()
    keys      = ("entity_number")
    outputen  = [dict(zip(keys, values)) for values in tenum]

    if len(outputen) == 0:
      return -1
  
    ed                         = outputen[0]
    en                         = ed['e']
  conn.close()
  return en
@anvil.server.callable
def get_entity_name_v002(entity_code):
  print('=================In get_entity_number_v002 - dbconnection:')

  # Open database connection

  conn = initialise_database_connection(app.branch)

  esql = f"SELECT entity_name FROM entities WHERE entity_id = \'{entity_code}\';"
  print(esql)
  with conn.cursor() as cur:
    cur.execute(esql)
    tenum     = cur.fetchall()
    keys      = ("entity_name")
    outputen  = [dict(zip(keys, values)) for values in tenum]

    if len(outputen) == 0:
      return None
  
    od                         = outputen[0]
    d                          = od['e']

  conn.close()
  return d
@anvil.server.callable 
def get_all_entity_codes_except_reporting_only():
  # Open database connection

  conn = initialise_database_connection(app.branch)

  esql = f"SELECT entity_id FROM entities WHERE reporting_only = 0;"
  
  with conn.cursor() as cur:
    cur.execute(esql)
    tenum     = cur.fetchall()
    print('tenum')
    print(tenum)
    keys      = ("entity_id")
    outputen  = [dict(zip(keys, values)) for values in tenum]
    print('outputen')
    print(outputen)
    if len(outputen) == 0:
      return -1
    entids = []  
    for r in outputen:
      eid = r['e']
      entids.append(eid)

    print('xxxx In get_all_entity_codes_except_reporting_only - entids')
    print(entids)

  return entids 
@anvil.server.callable 
def get_all_partner_codes():
  # Open database connection

  conn = initialise_database_connection(app.branch)

  esql = f"SELECT partner_id FROM partner;"
  
  with conn.cursor() as cur:
    cur.execute(esql)
    tenum     = cur.fetchall()
    print('tenum')
    print(tenum)
    keys      = ("partner_id")
    outputen  = [dict(zip(keys, values)) for values in tenum]
    print('outputen')
    print(outputen)
    if len(outputen) == 0:
      return -1
    parids = []  
    for r in outputen:
      pid = r['p']
      parids.append(pid)

    print('xxxx get_all_partner_codes - parids')
    print(parids)

    return parids 

@anvil.server.callable
def get_partner_name_number_from_id(in_partner_id):
  # Open database connection
  ret = {'name' : '', 'number' : 0} 
  conn = initialise_database_connection(app.branch)

  esql = f"SELECT partner_name, partner_number FROM partner WHERE partner_id = '{in_partner_id}';"
  
  with conn.cursor() as cur:
    cur.execute(esql)
    tenum     = cur.fetchall() 
    keys      = ("partner_name", "partner_number")
    outputen  = [dict(zip(keys, values)) for values in tenum]
    print('outputen')
    print(outputen)
    if len(outputen) == 0:
      return ret
    pd            = outputen[0]
    ret['number'] = pd['partner_number']
    ret['name']   = pd['partner_name']
    return ret
@anvil.server.callable
def get_client_name_number_from_id(in_client_id):
  # Open database connection
  ret = {'name' : '', 'number' : 0} 
  conn = initialise_database_connection(app.branch)

  esql = f"SELECT client_name, client_number FROM client WHERE client_id = '{in_client_id}';"
  
  with conn.cursor() as cur:
    cur.execute(esql)
    tenum     = cur.fetchall() 
    keys      = ("client_name", "client_number")
    outputen  = [dict(zip(keys, values)) for values in tenum]
    print('outputen')
    print(outputen)
    if len(outputen) == 0:
      return ret
    pd            = outputen[0]
    ret['number'] = pd['client_number']
    ret['name']   = pd['client_name']
    return ret
@anvil.server.callable
def get_entity_name_number_from_id(in_entity_id):
  # Open database connection
  ret = {'name' : '', 'number' : 0} 
  conn = initialise_database_connection(app.branch)

  esql = f"SELECT entity_name, entity_number FROM entities WHERE entity_id = '{in_entity_id}';"
  
  with conn.cursor() as cur:
    cur.execute(esql)
    tenum     = cur.fetchall() 
    keys      = ("entity_name", "entity_number")
    outputen  = [dict(zip(keys, values)) for values in tenum]
    print('outputen')
    print(outputen)
    if len(outputen) == 0:
      return ret
    pd            = outputen[0]
    ret['number'] = pd['entity_number']
    ret['name']   = pd['entity_name']
    return ret  
@anvil.server.callable 
def get_all_client_codes(in_partner_number):
  # Open database connection

  conn = initialise_database_connection(app.branch)

  esql = f"SELECT client_id FROM client WHERE partner_number = {in_partner_number};"
  
  with conn.cursor() as cur:
    cur.execute(esql)
    tenum     = cur.fetchall()
    print('tenum')
    print(tenum)
    keys      = ("client_id")
    outputen  = [dict(zip(keys, values)) for values in tenum]
    print('outputen')
    print(outputen)
    if len(outputen) == 0:
      return []
    cliids = []  
    for r in outputen:
      cid = r['c']
      cliids.append(cid)

    print('xxxx get_all_partner_codes - cliids')
    print(cliids)

    return cliids 
@anvil.server.callable 
def get_all_entity_codes(in_client_number):
  # Open database connection

  conn = initialise_database_connection(app.branch)

  esql = f"SELECT entity_id FROM entities WHERE client_number = {in_client_number};"
  
  with conn.cursor() as cur:
    cur.execute(esql)
    tenum     = cur.fetchall()
    print('tenum')
    print(tenum)
    keys      = ("entity_id")
    outputen  = [dict(zip(keys, values)) for values in tenum]
    print('outputen')
    print(outputen)
    if len(outputen) == 0:
      return []
    cliids = []  
    for r in outputen:
      cid = r['e']
      cliids.append(cid)

    print('xxxx get_all_entity_codes - cliids')
    print(cliids)

    return cliids     
@anvil.server.callable
def get_user_partner_details(email):
  print(f"In top get_user_partner_details input email is {email}")
  # Open database connection
  ret  = {'partner_number': 0, 'partner_id' : 'Undefined', 'partner_name' : 'Undefined'}
  conn = initialise_database_connection(app.branch)

  esql = f"SELECT organisation_type, organisation_number FROM [user] WHERE email = '{email}';"
  print(f"In get_user_partner_details - esql - {esql}\n")
  with conn.cursor() as cur:
    cur.execute(esql)
    tenum     = cur.fetchall()

    keys      = ("organisation_type", "organisation_number")
    outputen  = [dict(zip(keys, values)) for values in tenum]
    print('outputen')
    print(outputen)
    if len(outputen) == 0 :
      ret['partner_number'] = -1 #User isn't in the database user table
      return ret
    lpen                    = outputen[0]

    organisation_number     = lpen['organisation_number']
    organisation_type       = lpen['organisation_type']
    
    print('xxxx get_user_partner_organisation and number - ')
    print(organisation_type)
    print(organisation_number)
    
    if organisation_type != 'Partner': # If this user is not owned by a partner then they can't have access to the backend
      ret['partner_number'] = 0
      return ret
    else:
      partner_number = organisation_number

    psql = f"SELECT partner_name, partner_id FROM partner WHERE partner_number = {organisation_number};"
    cur.execute(psql)
    penum     = cur.fetchall()
    keys      = ("partner_name", "partner_id")
    outputen  = [dict(zip(keys, values)) for values in penum]
    if len(outputen) == 0:
      return ret    
    ppen                    = outputen[0]
    partner_name            = ppen['partner_name']
    partner_id              = ppen['partner_id']
    ret['partner_number']   = partner_number
    ret['partner_name']     = partner_name
    ret['partner_id']       = partner_id

    print(f"ret at end get_user_partner_details - {ret}\n")
    return ret      
@anvil.server.callable
def get_programme_start_date_v002(entity_code):
  print('=================In get_entity_number_v002 - dbconnection:')

  # Open database connection

  conn = initialise_database_connection(app.branch)

  esql = f"SELECT programme_start_date FROM entities WHERE entity_id = \'{entity_code}\';"
  print(esql)
  with conn.cursor() as cur:
    cur.execute(esql)
    tenum     = cur.fetchall()
    keys      = ("programme_start_date")
    outputsd  = [dict(zip(keys, values)) for values in tenum]

    if len(outputsd) == 0:
      return None
  
    od                         = outputsd[0]
    d                          = od['p']
    if d == None:
      return d
    sd                         = d.strftime("%d-%b-%Y")

  conn.close()
  return sd
@anvil.server.callable
def save_programme_start_date(ent_number, stdate, published):
  # Save the programme start date, stdate, for entity with entity number ent_number in the entities table.
  # Update the assessed_delivery_date fields for all projects for this entity.
  try:
    ret = {'ef':0, 'em':""}
    # Connect to the appropriate database
    
    conn = initialise_database_connection(published)
    
    with conn.cursor() as cursor:
      
      ssdsql = f"UPDATE entities SET programme_start_date = \'{stdate}\' WHERE entity_number = {ent_number};"
      cursor.execute(ssdsql)

      sqlr = f"SELECT project_id FROM projects WHERE entity_number = {ent_number} ; "
      cursor.execute(sqlr)
      t_projects_in_db   = cursor.fetchall() # A list of tuple project_id
      keys               = ("project_id","dummy_key")
      projects_in_db     = [dict(zip(keys, values)) for values in t_projects_in_db]

      if len(projects_in_db) > 0: # Only if this entity has some projects set up
        #print('====In save_programme_start_date')      
        for n in projects_in_db: # Iterate through all projects in this entity_number
          
          project_id = n['project_id']
            
          sqla = f"SELECT assessed FROM projects WHERE project_id = {project_id};"
          
          cursor.execute(sqla)
          t_asslist = cursor.fetchall()
          keys      = ("assessed","dummy_key")
          asslist   = [dict(zip(keys, values)) for values in t_asslist]
          
          dic = asslist[0]
          assessed = dic['assessed']

          # Calculate the assessed_delivery_date based on start_date and assessed value.
          asdd = kc.calculate_assessed_delivery_date(assessed, stdate)
          # Update the assessed_delivery_date
          sqlup         = f"UPDATE projects SET assessed_delivery_date = \'{asdd}\' WHERE project_id = {project_id};"

          cursor.execute(sqlup)
          conn.commit()    
    
      return ret
  except Exception as e:
    ret['ef'] = 1
    msg = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    ret['em'] = msg
    print(msg)
  return ret
@anvil.server.callable
def get_summary_stats(entity_number, published):
  try:
    summary_stats       = {'nbuild':0, 'nproj':0, 'ngsc':0, 'nesc':0, 'ef':0}

  # Open database connection
    conn                = initialise_database_connection(published)

    with conn.cursor() as cur:

  # Get gas and elec savings flags for this entity
  
      sqlb              = f"SELECT uprn, g_saving_flag, e_saving_flag FROM raw_estate_data WHERE entity_number = {entity_number};"
      cur.execute(sqlb)
      t_output_bl       = cur.fetchall()
      keys              = ("uprn","g_saving_flag","e_saving_flag") 
      output_bl         = [dict(zip(keys, values)) for values in t_output_bl]
      
      if len(output_bl) == 0:
          return summary_stats
      else:
          summary_stats['nbuild'] = len(output_bl)

      # Convert dict to dataframe
    
      dfb                = pd.DataFrame.from_dict (output_bl)

      # Count number of gas saving and electricity saving flags that have been set

      ngf                = dfb['g_saving_flag'].sum()
      nef                = dfb['e_saving_flag'].sum()
      
      summary_stats['ngsc'] = ngf
      summary_stats['nesc'] = nef    

    # Get number of projects for this entity
    
      sqlp              = f"SELECT uprn FROM projects WHERE entity_number = {entity_number};"
      cur.execute(sqlp)
      t_output_pr       = cur.fetchall() 
      keys              = ("uprn","dummy_key")
      output_pr         = [dict(zip(keys, values)) for values in t_output_pr]
      
      if len(output_pr) == 0:
          return summary_stats
      else:
          summary_stats['nproj'] = len(output_pr)      
      return summary_stats
    
  except Exception as e: 
    summary = f"******An exception has occurred in get_summary_stats for entity number - {entity_number}\n"

    msg1 = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    msg2 = f"{summary}{msg1}"
    print(msg2)
    summary_stats['ef'] = 1
    return summary_stats    

@anvil.server.background_task  
def upload_project_details_data_bt(file,entity, partner, client, published, user_name, dt_str, project_types):
  
# This is the background task for uploading project details data from an Excel Project Details Upload Form generated by the Hub and completed by the user. The upload form contains the details of the
# projects already set up for this entity via the project assessment upload. The projects are split into 3 groups - Non solar, Solar PV and Solar Thermal. Each group has it's own worksheet 
# called respectively - project_details, solar_pv and solar_thermal. 

# Each group has the following common fields: -

# uprn	             (unique property reference number within the entity)
# building_name	     (user defined)
# building_type	     (one of the pre-defined types help in the cibse_benchmarks table)
# project_type	     (1 of the 24 pre-defined project types)
# baselined	         (flag set 'YES' if user has baselined project which means no further updates can be made until flag set to 'NO')
# assessed	         (initial assessment made of likelihood and delivery timescales - FIRM, POSSIBLE, POTENTIAL etc. see below)
# status             (project status taken from the team managing the project)	
# utility	           (utility against which saving will be recorded - ELEC, GAS)
# persistence_factor (Expert judgement used to select a value from the Salix PF tables)	
# capex	             (Estimate of the most likely capex cost at project completion)
# delivery_date      (Estimate of the most likely delivery date)
# hp_scop            (Published Seasonal Coefficient of Performance for a Heat Pump)
# hp_elec_add_kwh_pa (Estimate of the annual kWh of electricity consumed to operate the Heat Pump)
#
# The following field is unique to the non solar (project details) sheet:- 
#
# saving_percent     (Expert judgement of the % energy saving achieved by the project on completion. Note: for solar projects this figure is calculated 'behind the scenes'
#                     based on other data collected - see functions calc_solar_pv_percent_savings and calc_solar_thermal_percent_savings)
#
# The following fields are unique to the solar pv sheet: -
#
# solar_roof_type                 (Entered by user - select from predefined list)
# solar_angle                     (Entered by user - select from predefined list)
# solar_pv_area_m2                (At project assessment time this is pre-populated with raw_estate_data.roof_space_m2 but this value can be overridden if user has a better measurement.)
# solar_kw_peak                   (Input by user if they have a figure otherwise the hub will create an estimated value)
# solar_pv_corrected_ann_gen_kwh  (Input by user if they have a figure otherwise the hub will create an estimated value)  
#
# The following fields are unique to the solar thermal sheet: -
#
# solar_thermal_area_m2	              (Entered by user - no value means missing data )
# solar_thermal_corrected_ann_gen_kwh (Input by user if they have a figure otherwise the hub will create an estimated value)

#  uprn, building name, building type, project type (1 of the 24 pre-defined types) and baselined flag - are in locked cells and cannot be changed by the user. 
# Delivery date is only mandatory once the status has moved to, or beyond, 'Procurement'.
# NOTE: If no delivery_date is provided the Hub calculates a rough delivery date from the value in the assessed field and stores this in the 'projects.assessed_delivery_date' field. When it 
# comes to writing results for Power BI the delivery date used will be 'projects.delivery_date' if this has been provided otherwise 'projects.assessed_delivery_date' will be used instead.
# The value of 'assessed_delivery_date' is based on the value in the 'assessed' field and the 'estimated_start_date' (for the programme for this building) which is held in the 'entities' table in Anvil.
# The 'assessed_delivery_date' is calculated when the project is initially setup - see see function 'upload_project_assessment_data' : -
#
# Assessed value    Delivered within     Value used in assessed_delivery_date
# --------------    ----------------     ------------------------------------
# FIRM              within 1 year         6 months from start date
# LIKELY            1 - 2 years          18 months from start date
# POSSIBLE          2 - 3 years          30 months from start date
# POTENTIAL         3 - 5 years          48 months from start date
# IN PLACE          N/A                  Null - status set to 'Completed'
# ASSESSED/NV       N/A                  Null - status set to 'Cancelled'
# FTHR IMPV*        Unknown but assume   48 months from start date
#                   same as POTENTIAL
#
# The following process is followed: -
# 1 - Validates column headers - reject batch if they are not as expected
# 2 - Check uprns are valid for this entity - reject projects with invalid uprns
# 3 - Converts nans to null or zero (depending on data type of column)
# 4 - Removes spurious projects added to the Excel worksheet that are not on the database
# 5 - Validates the dataframe values. Projects failing validation are rejected Note: project still exists in projects table but won't be updated with the information in this batch. 
# 6 - Identifies missing data.Projects with missing data are reported as warnings.
# 7 - Checks elec and gas savings per building do not exceed 100% - if >100% all projects for the building are rejected
# 8 - Projects that get this far are updated by the data in the upload and their energy and carbon savings are calculated. 
  
  try:

#    print('In load project details data uk')
    # Create header for the summary and the log. Initialize upload log and summary messages.
    # dt_str = dd/mm/YY H:M:S
    task_name              = "Upload project details data"
    task_context           = f"/{partner}/{client}/{entity}"
    anvil.server.task_state['pc_complete'] = "0"
    anvil.server.task_state['status'] = f"{task_name} upload starting "    
    
    import datetime as dt
    header                 = f"Project details upload by user - {user_name} run on {dt_str} for Partner: {partner}, Client: {client}, Entity {entity} \n File selected : {file.name}\n Task name - {task_name}\n "
    default_date           = dt.datetime(1900,1,1)
    ####default_date           =   
    up_log                 = header

    # Retrieve the entity number from entities table

    entity_number       = anvil.server.call('get_entity_number_v002',entity)
    # Open database connection
      
    conn                = initialise_database_connection(published)
    
   # Get background task ID and initialise log

    task_id             = anvil.server.context.background_task_id
    task_id             = f"{task_id}{task_context}"
    kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log) 
    
    with anvil.media.TempFile(file) as file_name:
   
      if file == None or entity == None:
        up_log              = up_log  + f"++++++++ No file or entity supplied\n"
        anvil.server.task_state['pc_complete'] = "0"
        anvil.server.task_state['status'] = "****FAILED"
        kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
        return 
      else:

        # Check sheets named 'project_details', 'solar_pv' and 'solar_thermal' are in workbook
        shn                 = ['Non Solar', 'Solar PV', 'Solar Thermal','Auth', 'Key']
        xl                  = pd.ExcelFile(file_name)
        snames              = xl.sheet_names  # see all sheet names
        snfail              = False
        for s in shn:
          if s not in snames:
            snfail            = True
            up_log            = up_log  + f"****Error - cannot find sheet called {s} required for project_details upload\n"
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****FAILED - cannot find sheet called {s} required for estate upload"
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
        if snfail:
          return         
        # Read in the Auth dataframe and check Partner, Client and Entity are the same as encrypted in the Key on the Key sheet.. If they are different then raise an error and exit.
        
#      Authenticate_workbook
        auts                = pd.read_excel(file_name, sheet_name = 'Auth', dtype = object) 
        keys                = pd.read_excel(file_name, sheet_name = 'Key', dtype = object)
        ret                 = kc.authenticate_workbook(auts, keys,  partner, client, entity)
        ef                  = ret['ef']
        msg                 = ret['em']

        if ef == 1:
          up_log            = up_log + f"***Workbook authentication error: -\n{msg}\n"
          anvil.server.task_state['pc_complete'] = "0"
          anvil.server.task_state['status'] = f"****FAILED - Workbook authentication error, see upload log for details"
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
          return          
        if ef == 2:
          if 'cryptography.exceptions.InvalidSignature: Signature did not match digest' in msg:
            up_log            = up_log + f"***Invalid key found in workbook - does not correspond to context detils. Possibly the Auths or Key sheets have been edited."
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****FAILED - Invalid key found in workbook - does not correspond to context detils. Possibly the Auths or Key sheets have been edited."
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            return    
          else: 
            up_log            = up_log + f"***Authentication failure - inconsistency between current context and the key stored in the workbook. Possibly attempting to load the workbook to the wrong entity: -\n{msg}\n"
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****Authentication failure - inconsistency between current context and the key stored in the workbook. Possibly attempting to load the workbook to the wrong entity"
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            return        
                
        # Create dataframes from each sheet
        
        dfns                  = pd.read_excel(file_name, sheet_name = 'Non Solar', dtype = object) #Non-solar dataframe
        dfsp                  = pd.read_excel(file_name, sheet_name = 'Solar PV', dtype = object) #Solar PV dataframe
        dfst                  = pd.read_excel(file_name, sheet_name = 'Solar Thermal', dtype = object) #Solar Thermal dataframe
        
        # Validate non-solar dataframe column headings (keys)
      
        sname               = 'Non Solar'
        
        col_heads_read      = list(dfns.columns.values)

#        valid_col_heads     = [ 'building_name', 'uprn','building_type', 'project_type','baselined','assessed','status','utility','persistence_factor','saving_percent','hp_scop','hp_elec_add_kwh_pa','capex','delivery_date']
        valid_col_heads     =  ['Building ID', 'Building name','Building type', 'Project type','Assessed','Status','Utility','Lifetime (yrs)','Saving %','Heat pump scop','Heat pump elec add kWh pa','CAPEX','Delivery date']
        column_xl           = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z","AA","AB"]
      
        len_expected        = len(valid_col_heads)
        len_read            = len(col_heads_read)
     
        if len_read != len_expected:
          up_log            = up_log + f"****ERROR - Mismatch in number of columns found on input file. Expected {len_expected} but found {len_read}\n"
          anvil.server.task_state['pc_complete'] = "0"
          anvil.server.task_state['status'] = f"****FAILED - Mismatch in number of columns found on input file. Expected {len_expected} but found {len_read}"
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
          return 
      
        check1             = f"\nExpected the following column names on sheet {sname} but did not find them:- \n"                      
        ic                 = -1
        nerr1              = 0
        for c in valid_col_heads:
          if c not in col_heads_read:
            nerr1         = nerr1 + 1
            check1        = check1 + f"{c}, "
            
        if nerr1 > 0:
          up_log          = up_log + f"****Error - missing columns in upload - see upload log for details\n"
          up_log          = up_log + f"{check1}\n"
  
        check2            = f"\nFound the following column names which are not valid:- \n"

        nerr2             = 0
        for c in col_heads_read:
          ic              = ic + 1    
          if c not in valid_col_heads:
            nerr2         = nerr2 + 1
            check2        = check2 + f"{c} in Excel column {column_xl[ic]}\n "
           
        if nerr2 > 0:
          up_log          = up_log + f"****Error - invalid columns found in upload - see upload log for details\n"
          up_log          = up_log + f"{check2}\n"
        
        if nerr1 > 0 or nerr2 > 0:
          anvil.server.task_state['pc_complete'] = "0"
          anvil.server.task_state['status'] = f"****FAILED - invalid columns found in upload"
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
          return 
        
        # Validate solar pv dataframe column headings (keys)
      
        sname               = 'Solar PV'
        
        col_heads_read      = list(dfsp.columns.values)

#        valid_col_heads     = [ 'building_name', 'uprn','building_type', 'project_type','baselined','assessed','status','utility','persistence_factor','capex','delivery_date','solar_roof_type','solar_angle','solar_pv_area_m2','solar_kw_peak','solar_pv_corrected_ann_gen_kwh']
        valid_col_heads     = ['Building ID', 'Building name','Building type', 'Project type','Assessed','Status','Utility','Lifetime (yrs)','CAPEX','Delivery date','Solar roof type','Solar angle','Solar area m2','Solar KW peak','Corrected annual gen kWh']		
        column_xl           = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z","AA","AB"]
      
        len_expected        = len(valid_col_heads)
        len_read            = len(col_heads_read)
      
        if len_read != len_expected:
          up_log            = up_log + f"****ERROR - Mismatch in number of columns found on input file. Expected {len_expected} but found {len_read}\n"
          anvil.server.task_state['pc_complete'] = "0"
          anvil.server.task_state['status'] = f"****FAILED - Mismatch in number of columns found on input file. Expected {len_expected} but found {len_read}"
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
          return 
      
        check1             = f"\nExpected the following column names on sheet {sname} but did not find them:- \n"                      
        ic                 = -1
        nerr1              = 0
        for c in valid_col_heads:
          if c not in col_heads_read:
            nerr1         = nerr1 + 1
            check1        = check1 + f"{c}, "
            
        if nerr1 > 0:
          up_log          = up_log + f"****Error - missing columns in upload - see upload log for details\n"
          up_log          = up_log + f"{check1}\n"
  
        check2            = f"\nFound the following column names which are not valid:- \n"

        nerr2             = 0
        for c in col_heads_read:
          ic              = ic + 1    
          if c not in valid_col_heads:
            nerr2         = nerr2 + 1
            check2        = check2 + f"{c} in Excel column {column_xl[ic]}\n "
           
        if nerr2 > 0:
          up_log          = up_log + f"****Error - invalid columns found in upload - see upload log for details\n"
          up_log          = up_log + f"{check2}\n"
        
        if nerr1 > 0 or nerr2 > 0:
          anvil.server.task_state['pc_complete'] = "0"
          anvil.server.task_state['status'] = f"****FAILED - invalid columns found in upload"
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
          return 
          
        # Validate solar thermal dataframe column headings (keys)
      
        sname               = 'Solar Thermal'
        
        col_heads_read      = list(dfst.columns.values)

#        valid_col_heads     = [ 'building_name', 'uprn','building_type', 'project_type','baselined','assessed','status','utility','persistence_factor','capex','delivery_date','solar_thermal_area_m2','solar_thermal_corrected_ann_gen_kwh']
        valid_col_heads     =  ['Building ID', 'Building name','Building type', 'Project type','Assessed','Status','Utility','Lifetime (yrs)','CAPEX','Delivery date','Solar area m2','Corrected annual gen kWh']		
        column_xl           = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z","AA","AB"]
      
        len_expected        = len(valid_col_heads)
        len_read            = len(col_heads_read)
      
        if len_read != len_expected:
          up_log            = up_log + f"****ERROR - Mismatch in number of columns found on input file. Expected {len_expected} but found {len_read}\n"
          anvil.server.task_state['pc_complete'] = "0"
          anvil.server.task_state['status'] = f"****FAILED - Mismatch in number of columns found on input file. Expected {len_expected} but found {len_read}"
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
          return 
      
        check1             = f"\nExpected the following column names on sheet {sname} but did not find them:- \n"                      
        ic                 = -1
        nerr1              = 0
        for c in valid_col_heads:
          if c not in col_heads_read:
            nerr1         = nerr1 + 1
            check1        = check1 + f"{c}, "
            
        if nerr1 > 0:
          up_log          = up_log + f"****Error - missing columns in upload - see upload log for details\n"
          up_log          = up_log + f"{check1}\n"
  
        check2            = f"\nFound the following column names which are not valid:- \n"

        nerr2             = 0
        for c in col_heads_read:
          ic              = ic + 1    
          if c not in valid_col_heads:
            nerr2         = nerr2 + 1
            check2        = check2 + f"{c} in Excel column {column_xl[ic]}\n "
           
        if nerr2 > 0:
          up_log          = up_log + f"****Error - invalid columns found in upload - see upload log for details\n"
          up_log          = up_log + f"{check2}\n"
        
        if nerr1 > 0 or nerr2 > 0:
          anvil.server.task_state['pc_complete'] = "0"
          anvil.server.task_state['status'] = f"****FAILED - invalid columns found in upload"
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
          return 
        
        with conn.cursor() as cursor:
        # Get the list of project type ids and names

          sqlt                = f"SELECT project_type_id, name FROM project_types;"
          cursor.execute(sqlt)
          t_output_pt         = cursor.fetchall()
          keys                = ("project_type_id","name")
          output_pt           = [dict(zip(keys, values)) for values in t_output_pt]
          
        # Cleansing and validation of input dataframes
        #
        #XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
        #
        # ==== Non solar dataframe ====
        #
        #XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
        #
        # Replace nans in text columns by Null, in numeric columns replace by zero.
          print('dfns+++++++++')
          print(dfns.to_string())
          dfns['Building ID']         = dfns['Building ID'].fillna(0)
          dfns['Building name']       = dfns['Building name'].fillna('')
          dfns['Building type']       = dfns['Building type'].fillna('')
          dfns['Project type']        = dfns['Project type'].fillna('')
          dfns['Assessed']            = dfns['Assessed'].fillna('')
          dfns['Status']              = dfns['Status'].fillna('')
          dfns['Utility']             = dfns['Utility'].fillna('')
          dfns['Lifetime (yrs)']      = dfns['Lifetime (yrs)'].fillna(0)
          dfns['Saving %']            = dfns['Saving %'].fillna(0)
          dfns['Heat pump scop']      = dfns['Heat pump scop'].fillna(0)
          dfns['Heat pump elec add kWh pa']  = dfns['Heat pump elec add kWh pa'].fillna(0)
          dfns['CAPEX']               = dfns['CAPEX'].fillna(0)
          dfns['Delivery date']       = dfns['Delivery date'].fillna(default_date)#Default date stored on database when no date specified on upload
       
          #  Insert column holding row numbers as seen by user in Excel
      
          dfns.insert(loc=0,column    ='excel_row_num',value = dfns.reset_index().index + 2)
          num_rows_read               = dfns.shape[0]
          up_log                      = up_log + f"Number of records read from project_details (non-solar) sheet - {num_rows_read}\n"

          # Remove spurious projects in the upload dataframe (projects in upload not on database for each building)

          ret                 = kc.remove_spurious_projects(conn, dfns, project_types)

          ef                  = ret['ef']
          em                  = ret['em']
          log                 = ret['log']
          nspur               = ret['nspur']
          dfns                = ret['df_nospur']

          if ef == 2:
            up_log            = up_log + f"**** Error -  removing spurious projects from non-solar sheet: -\n{em}\n"
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****FAILED - Error removing spurious projects from non-solar sheet, see upload log for details"
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            return            
          
          up_log              = up_log + log
          
          # Validate the dataframe
          #{'ef':0,'em':'Validation completed successfully','validated_dfns':'','validation_messages':'','nvw':0,'nve':0}
          validation          = kc.validate_non_solar_projects_details_upload(conn, entity, entity_number, dfns)
#          print('####Exited from validate non solar')

          ef                  = validation['ef']
          em                  = validation['em']
          d4                  = validation['validated_df']
          vm                  = validation['validation_messages']
          mdm                 = validation['missing_data_messages']
          nvw                 = validation['nvw']  # Number of validation warnings
          nve                 = validation['nve']  # Number of validation errors
          mdw                 = validation['mdw']  # Number of missing data warnings

#          print(f"{ef}\n {em} \n {d4.to_string()} \n {mdm} \n {nvw} \n {nve} \n {mdw}")
          if ef == 2:
            up_log            = up_log + f"**** Error -  Non solar projects validation stopped with error: -\n{em}\n"
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****FAILED - Error occurred validating upload file, see upload log for details"
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            return
          if ef == 0:
            up_log            = up_log + f"----- Non solar projects validation completed successfully -------\n"
            anvil.server.task_state['pc_complete'] = "10"
            anvil.server.task_state['status'] = f"Non solar projects validation completed successfully"            
          if ef == 1:
            up_log            = up_log + f"++++ Non solar projects validation completed with application warning: -\n{em}\n"  
            anvil.server.task_state['pc_complete'] = "10"
            anvil.server.task_state['status'] = f"Non solar projects validation completed with warning - see upload log for details"
            
          up_log            = up_log + f"Number of spurious projects removed:     {nspur}\n"
          up_log            = up_log + f"Number of validation errors:             {nve}\n"
          up_log            = up_log + f"Number of validation warnings:           {nvw}\n"
          up_log            = up_log + f"Number of missing data warnings:         {mdw}\n"
          up_log            = up_log + f"\n Validation messages: - \n{vm}\n"
          up_log            = up_log + f"\n Missing data messages: - \n{mdm}\n"

          if nve == 0: # No validation errors so ok to update
                      #..Update non solar projects  
#            print('###Before update non solar projects')
            ret_m             = kc.update_non_solar_project_details(conn, dfns, entity_number, output_pt)
            
            up_log            = up_log + ret_m['up_log']
            
            if ret_m['ef'] > 1: # A serious error has occured so return and don't do any further processing
              anvil.server.task_state['pc_complete'] = "0"
              anvil.server.task_state['status'] = f"****FAILED - Error occurred during update of non solar projects, see upload log for details"
              kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
              return
            else:
              anvil.server.task_state['pc_complete'] = "20"
              anvil.server.task_state['status'] = f"Non solar projects update completed successfully"           
          #
          #XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
          #
          # ==== Solar pv dataframe ====
          #
          #XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
          #
          # Replace nans in text columns by Null, in numeric columns replace by zero.
          
          dfsp['Building ID']         = dfsp['Building ID'].fillna(0)
          dfsp['Building name']       = dfsp['Building name'].fillna('')
          dfsp['Building type']       = dfsp['Building type'].fillna('')
          dfsp['Project type']        = dfsp['Project type'].fillna('')
          dfsp['Assessed']            = dfsp['Assessed'].fillna('')
          dfsp['Status']              = dfsp['Status'].fillna('')
          dfsp['Utility']             = dfsp['Utility'].fillna('')
          dfsp['Lifetime (yrs)']      = dfsp['Lifetime (yrs)'].fillna(0)
  #        dfsp['saving_percent']      = dfsp['saving_percent'].fillna(0)  This is calculated separately usin other fields and inserted into database
          dfsp['CAPEX']               = dfsp['CAPEX'].fillna(0)
          dfsp['Delivery date']       = dfsp['Delivery date'].fillna(default_date)#Default date stored on database when no date specified on upload
          dfsp['Solar roof type']     = dfsp['Solar roof type'].fillna('')
          dfsp['Solar angle']         = dfsp['Solar angle'].fillna('')
          dfsp['Solar area m2']       = dfsp['Solar area m2'].fillna(0)
          dfsp['Solar KW peaf']       = dfsp['Solar KW peak'].fillna(0)
          dfsp['Corrected annual gen kWh'] = dfsp['Corrected annual gen kWh'].fillna(0)
        
          #  Insert column holding row numbers as seen by user in Excel
        
          dfsp.insert(loc=0,column    ='excel_row_num',value = dfsp.reset_index().index + 2)
          num_rows_read               = dfsp.shape[0]
          up_log                      = up_log + f"Number of records read from solar pv sheet - {num_rows_read}\n"
          
          # Remove spurious projects in the upload dataframe (projects in upload not on database for each building)

          ret                 = kc.remove_spurious_projects(conn, dfsp, project_types)

          ef                  = ret['ef']
          em                  = ret['em']
          log                 = ret['log']
          nspur               = ret['nspur']
          #print(f"In upload project details after remove_spurious_projects - log : \n {log} \n")
          dfsp                = ret['df_nospur']

          if ef == 2:
            up_log            = up_log + f"**** Error -  removing spurious projects from solar pv sheet: -\n{em}\n"
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****FAILED - Error removing spurious projects from solar pv sheet, see upload log for details"
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            return            
          
          up_log              = up_log + log
          #print(f"In upload project details after remove_spurious_projects -  up_log : \n {up_log} \n")          
          # Validate the dataframe
          #{'ef':0,'em':'Validation completed successfully','validated_dfsp':'','validation_messages':'','nvw':0,'nve':0}
          validation          = kc.validate_solar_pv_projects_details_upload(conn, entity, entity_number, dfsp)
          print('####Exited from validate solar_pv')
  
          ef                  = validation['ef']
          em                  = validation['em']
          d4                  = validation['validated_df']
          vm                  = validation['validation_messages']
          mdm                 = validation['missing_data_messages']
          nvw                 = validation['nvw']  # Number of validation warnings
          nve                 = validation['nve']  # Number of validation errors
          mdw                 = validation['mdw']  # Number of missing data warnings

          #print(f"{ef}\n {em} \n {d4.to_string()} \n {mdm} \n {nvw} \n {nve} \n {mdw}")
          if ef == 2:
            up_log            = up_log + f"**** Error -  Solar PV projects validation stopped with error: -\n{em}\n {vm} \n {mdm}"
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****FAILED - Error occurred validating upload file, see upload log for details"
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            return
          if ef == 0:
            up_log            = up_log + f"----- Solar PV projects validation completed successfully -------\n"
            anvil.server.task_state['pc_complete'] = "30"
            anvil.server.task_state['status'] = f"Solar PV projects validation completed successfully"            
          if ef == 1:
            up_log            = up_log + f"++++ Solar PV projects validation completed with application warning: -\n{em}\n"  
            anvil.server.task_state['pc_complete'] = "30"
            anvil.server.task_state['status'] = f"Solar PV projects validation completed with warning - see upload log for details"

          up_log            = up_log + f"Number of spurious projects removed:     {nspur}\n"
          up_log            = up_log + f"Number of validation errors:             {nve}\n"
          up_log            = up_log + f"Number of validation warnings:           {nvw}\n"
          up_log            = up_log + f"Number of missing data warnings:         {mdw}\n"
          up_log            = up_log + f"\n Validation messages: - \n{vm}\n"
          up_log            = up_log + f"\n Missing data messages: - \n{mdm}\n"

          if nve == 0: # No validation errors so ok to update
                      #..Update Solar PV projects  
            print('###Before update Solar PV projects')
            ret_m             = kc.update_solar_pv_project_details(conn, dfsp, entity_number, output_pt)
            print('####After call to update solar pv projects - ret_m is:')

            print(ret_m)
            
            up_log            = up_log + ret_m['up_log']
            
            if ret_m['ef'] > 1: # A serious error has occured so return and don't do any further processing
              anvil.server.task_state['pc_complete'] = "0"
              anvil.server.task_state['status'] = f"****FAILED - Error occurred during update of Solar PV projects, see upload log for details"
              kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
              return
            else:
              anvil.server.task_state['pc_complete'] = "40"
              anvil.server.task_state['status'] = f"Solar PV projects update completed successfully"   
          #
          #XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
          #
          # ==== Solar thermal dataframe ====
          #
          #XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
          #
          # Replace nans in text columns by Null, in numeric columns replace by zero.
          
          dfst['Building ID']         = dfst['Building ID'].fillna(0)
          dfst['Building name']       = dfst['Building name'].fillna('')
          dfst['Building type']       = dfst['Building type'].fillna('')
          dfst['Project type']        = dfst['Project type'].fillna('')
          dfst['Assessed']            = dfst['Assessed'].fillna('')
          dfst['Status']              = dfst['Status'].fillna('')
          dfst['Utility']             = dfst['Utility'].fillna('')
          dfst['Lifetime (yrs)']      = dfst['Lifetime (yrs)'].fillna(0)
  #        dfst['saving_percent']      = dfst['saving_percent'].fillna(0)  This is calculated separately from other fields and inserted into database
          dfst['CAPEX']               = dfst['CAPEX'].fillna(0)
          dfst['Delivery date']       = dfst['Delivery date'].fillna(default_date)#Default date stored on database when no date specified on upload
          dfst['Solar area m2']       = dfst['Solar area m2'].fillna(0)
          dfst['Corrected annual gen kWh'] = dfst['Corrected annual gen kWh'].fillna(0)
        
          #  Insert column holding row numbers as seen by user in Excel
          #  Insert column holding row numbers as seen by user in Excel
        
          dfst.insert(loc=0,column    ='excel_row_num',value = dfst.reset_index().index + 2)
          num_rows_read               = dfst.shape[0]
          up_log                      = up_log + f"Number of records read from solar thermal sheet - {num_rows_read}\n"

       # Remove spurious projects in the upload dataframe (projects in upload not on database for each building)

          ret                 = kc.remove_spurious_projects(conn, dfst, project_types)

          ef                  = ret['ef']
          em                  = ret['em']
          log                 = ret['log']
          nspur               = ret['nspur']
          dfst                = ret['df_nospur']

          if ef == 2:
            up_log            = up_log + f"**** Error -  removing spurious projects from solar thermal sheet: -\n{em}\n"
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****FAILED - Error removing spurious projects from solar thermal sheet, see upload log for details"
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            return            
          
          up_log              = up_log + log
                    
          
          # Validate the dataframe
          #{'ef':0,'em':'Validation completed successfully','validated_dfsp':'','validation_messages':'','nvw':0,'nve':0}
          validation          = kc.validate_solar_thermal_projects_details_upload(conn, entity, entity_number, dfst)
#          print('####Exited from validate solar_thermal')
  
          ef                  = validation['ef']
          em                  = validation['em']
          d4                  = validation['validated_df']
          vm                  = validation['validation_messages']
          mdm                 = validation['missing_data_messages']
          nvw                 = validation['nvw']  # Number of validation warnings
          nve                 = validation['nve']  # Number of validation errors
          mdw                 = validation['mdw']  # Number of missing data warnings

#          print(f"{ef}\n {em} \n {d4.to_string()} \n {mdm} \n {nvw} \n {nve} \n {mdw}")
          if ef == 2:
            up_log            = up_log + f"**** Error -  Solar thermal projects validation stopped with error: -\n{em}\n"
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****FAILED - Error occurred validating upload file, see upload log for details"
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            return
          if ef == 0:
            up_log            = up_log + f"----- Solar thermal projects validation completed successfully -------\n"
            anvil.server.task_state['pc_complete'] = "50"
            anvil.server.task_state['status'] = f"Solar thermal projects validation completed successfully"            
          if ef == 1:
            up_log            = up_log + f"++++ Solar thermal projects validation completed with application warning: -\n{em}\n"  
            anvil.server.task_state['pc_complete'] = "50"
            anvil.server.task_state['status'] = f"Solar thermal projects validation completed with warning - see upload log for details"
         
          up_log            = up_log + f"Number of spurious projects removed:     {nspur}\n"  
          up_log            = up_log + f"Number of validation errors:             {nve}\n"
          up_log            = up_log + f"Number of validation warnings:           {nvw}\n"
          up_log            = up_log + f"Number of missing data warnings:         {mdw}\n"
          up_log            = up_log + f"\n Validation messages: - \n{vm}\n"
          up_log            = up_log + f"\n Missing data messages: - \n{mdm}\n"

          if nve == 0: # No validation errors so ok to update
                      #..Update Solar thermal projects  
#            print('###Before update Solar thermal projects')
            ret_m             = kc.update_solar_thermal_project_details(conn, dfst, entity_number, output_pt)
            
            up_log            = up_log + ret_m['up_log']
            
            if ret_m['ef'] > 1: # A serious error has occured so return and don't do any further processing
              anvil.server.task_state['pc_complete'] = "0"
              anvil.server.task_state['status'] = f"****FAILED - Error occurred during update of Solar thermal projects, see upload log for details"
              kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
              return
            else:
              anvil.server.task_state['pc_complete'] = "60"
              anvil.server.task_state['status'] = f"Solar thermal projects update completed successfully"   
          #
            # Gas and elec saving % checks for each building.
            
            checks           = kc.gaselec_savings_check(conn,entity_number)
            
            ef               = checks['ef']
            em               = checks['em']
            
            if ef == 2:
              up_log = up_log + f"****Error occurred during gas and elec savings checks \n {em}\n"              
              anvil.server.task_state['pc_complete'] = "0"
              anvil.server.task_state['status'] = f"****FAILED - Error occurred during gas and elec savings checks - see upload log for details"
              kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
              return                
            else:
              anvil.server.task_state['pc_complete'] = "70"
              anvil.server.task_state['status'] = f"Gas and elec savings checks completed successfully"    
              up_log = up_log + f"Gas and elec savings checks completed successfully \n"  

            gas_uprn_list    = checks['gas_uprn_list']
            elec_uprn_list   = checks['elec_uprn_list']
            gas_build_list   = checks['gas_build_list']
            elec_build_list  = checks['elec_build_list']            
            ngasfails        = checks['ngasfails']
            nelecfails       = checks['nelecfails']
            
            print('Info gas elec savings --------------')
            print(f"gas uprn list \n {gas_uprn_list} \n\n")
            print(f"gas build list \n {gas_build_list} \n\n")
            print(f"elec uprn list \n {elec_uprn_list} \n\n")
            print(f"elec build list \n {elec_build_list} \n\n")
            print(f"Num gas fails : {ngasfails}\n")
            print(f"Num elec fails : {nelecfails}\n")
            if ngasfails > 0:
              up_log         = up_log + f"****Processing cannot continue as {ngasfails} buildings have gas savings >100%. Please correct and resubmit. \n"
              n = 0
              up_log         = up_log + f"***** Gas saving checks have failed for the following buildings: -\n"
              for l in gas_uprn_list:
                gef          = f"Building id : {gas_uprn_list[n]} Building name: {gas_build_list[n]}\n"
                n            = n + 1
                up_log       = up_log + gef
            if nelecfails > 0:
              up_log         = up_log + f"****Processing cannot continue as {nelecfails} buildings have electricity savings >100%. Please correct and resubmit. \n"
              n = 0
              up_log         = up_log + f"***** Electricity saving checks have failed for the following buildings: -\n"
              for l in elec_uprn_list:
                gef          = f"Building id : {elec_uprn_list[n]} Building name: {elec_build_list[n]}\n"
                n            = n + 1
                up_log       = up_log + gef                 

            if nelecfails > 0 or ngasfails > 0:
              up_log = up_log + f"****Error occurred during gas and elec savings checks \n {em}\n"              
              anvil.server.task_state['pc_complete'] = "0"
              anvil.server.task_state['status'] = f"****FAILED - gas and elec savings check have failed - see upload log for details"
              kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
              return

            up_log            = up_log  + f"\n ---Validation and updating of project details complete.\n\n >>>Calculation of energy and carbon savings starting.\n"
        
        # Calculate and save the project energy and carbon savings
 
            ret            = kc.calc_project_energy_carbon_savings_v5_PC01(conn, entity_number)
            ef                = ret['ef']
            em                = ret['em']
            #print(f"++++++++In project details upload after calc_project - \n ef = {ef} \n em = {em}")
            if ef == 2:
              up_log = up_log + f"****Error occurred calculating project energy and carbon savings \n {em}\n"              
              anvil.server.task_state['pc_complete'] = "0"
              anvil.server.task_state['status'] = f"****FAILED - Error updating project energy and carbon savings - see upload log for details"
              kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
              return                
            else:
              anvil.server.task_state['pc_complete'] = "75"
              anvil.server.task_state['status'] = f"Calculating project energy and carbon savings completed successfully"    
              up_log = up_log + f"Calculating project energy and carbon savings completed successfully \n"  
            
        # Calculate and save the solar estate summary
            
            ret               = kc.calc_solar_summary(conn, entity_number)
            ef                = ret['ef']
            em                = ret['em']
            if ef == 2:
              up_log = up_log + f"****Error occurred calculating solar summary \n {em}\n"              
              anvil.server.task_state['pc_complete'] = "0"
              anvil.server.task_state['status'] = f"****FAILED - Error calculating solar summary - see upload log for details"
              kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
              return                
            if ef == 1:
              anvil.server.task_state['pc_complete'] = "80"
              anvil.server.task_state['status'] = f"Calculating solar summary completed with warnings"    
              up_log = up_log + f"Calculating solar summary completed with warnings \n {em}" 
            else:
              anvil.server.task_state['pc_complete'] = "80"
              anvil.server.task_state['status'] = f"Calculating solar summary completed successfully"    
              up_log = up_log + f"Calculating solar summary completed successfully \n"              
 
            ret           =   kc.create_pbi_tables_v3( conn, entity_number) 
            ef            = ret['ef']
            em            = ret['em']            
            if ef == 2:
              up_log = up_log + f"****Error occurred creating Power BI tables \n {em}\n"              
              anvil.server.task_state['pc_complete'] = "0"
              anvil.server.task_state['status'] = f"****FAILED - Error creating Power BI tables - see upload log for details"
              kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
              return                
            else:
              anvil.server.task_state['pc_complete'] = "100"
              anvil.server.task_state['status'] = f"Upload of project details data completed successfully" 
              up_log = up_log + f"Creation of Power BI tables completed successfully \n Upload of project details data completed successfully"
              kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)        
  except Exception as e: 
    msg = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    print(msg)
    up_log  = f"Exception exit 1 \n {msg}"
    anvil.server.task_state['pc_complete'] = "0"
    anvil.server.task_state['status'] = f"****FAILED - Exception occured during upload of project details data - see upload log for details"
    kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
    return 

@anvil.server.callable
def export_all_results( export_to, entity_number, entity_name, published, user_name, dt_str):

# This subroutine assembles the dataframes containing the data that is required to be written to either the results Excel workbook or the database
# results tables for Power BI depending on the value of 'export_to' which can be either 'Excel' or 'DB'. 
# Estate, Estate_summary, Project_savings, Solar_summary, and other tables bespoke to certain Power BI visualisations.
# The subroutine write_all_results_to_excel writes the assembled dataframes to an excel file for downloading by the user. The following Excel tables are created: -
#    Estate, Estate_summary, Project_savings, Solar_summary, Build_energy_cost and Waterfall_project_savings
#
# The subroutine write_all_results_to_db writes the assembled dataframes to the database for access by Power BI. The following tables are updated: -
#    pbi_estate, pbi_build_energy_cost, pbi_waterfall_project_savings.
#    Power BI can use the estate_summary and solar_summary tables without modification.

  ret_mess  = {'ef':0, 'em':0, 'rmedia':''}
  summary   = ''
  up_log    = ''
  
  if export_to != 'Excel' and export_to != 'DB':
    ret_mess['ef']  = 2
    ret_mess['em']  = '****ERROR - value of export_to must be Excel or DB'
    return ret_mess
  
  conn                 = initialise_database_connection(app.branch)
  try:
    with conn.cursor() as cur:
    
    #====================================================================================================
    #
    # Assemble project savings results 
    #
    #=====================================================================================================
      
    # Get uprn, building name, building type and elec and gas saving flags for this entity
    
      sqlb              = f"SELECT uprn, building_name, building_type, g_saving_flag, e_saving_flag FROM raw_estate_data WHERE entity_number = {entity_number};"
      cur.execute(sqlb)
      t_output_bl       = cur.fetchall()
      keys              = ("uprn", "building_name", "building_type", "g_saving_flag", "e_saving_flag")
      output_bl         = [dict(zip(keys, values)) for values in t_output_bl]
    
      if len(output_bl) == 0:
        ret_mess['em'] = "+++Warning - no buildings have been set up for this entity"
        ret_mess['ef'] = 1
        return ret_mess
      
      # Convert dict to dataframe
      
      dfb                = pd.DataFrame.from_dict (output_bl)
      
      # Count number of gas saving and electricity saving flags that have been set
      
      em                 = ''
      ngf                = dfb['g_saving_flag'].sum()
      nef                = dfb['e_saving_flag'].sum()
      
      if ngf > 0:
        em               = f"Number of buildings failing gas savings checks : {ngf}\n"
      if nef > 0:
        em               =  em + f"Number of buildings failing electricity savings checks : {ngf}"
      if ngf > 0 or nef > 0:
        ret_mess['em'] = em
        ret_mess['ef'] = 1
        return ret_mess        
        
    # Get data for all projects for this entity_number from projects and project_results tables. Filter out projects where assessed is 'IN PLACE' or 'ASSESSED/NV'
    
      sqlj               = f"SELECT  projects.uprn, projects.project_type_id, projects.baselined, projects.assessed, projects.assessed_delivery_date, projects.project_status, projects.utility, \
                                      projects.cost_capex_mode, projects.delivery_date_mode, project_results.energy_savings, project_results.gas_savings, \
                                      project_results.electric_savings, project_results.carbon_savings, project_results.tonne_co2_lifetime_cost \
                                      FROM projects \
                                      INNER JOIN project_results ON projects.project_id = project_results.project_id \
                                      WHERE entity_number = {entity_number} AND assessed != 'IN PLACE' AND assessed != 'ASSESSED/NV';"\
    
      dfprojects         = pd.read_sql_query(sqlj, conn)

    # Get the list of project types
    
      sqlt              = f"SELECT project_type_id, name FROM project_types;"
      cur.execute(sqlt)
      t_output_pt       = cur.fetchall()
      keys              = ("project_type_id","name")
      output_pt         = [dict(zip(keys, values)) for values in t_output_pt]

      nr                 = dfprojects.shape[0]    
    
    # Insert columns to hold building_name, building_type and project_type
    
      dfprojects                = dfprojects.assign(building_name = [''] * nr,
                                      building_type = [''] * nr,
                                      project_type  = [''] * nr)
    
    # Insert building_name, building_type and project_type (name) 
    
      for index, row in dfprojects.iterrows():
        uprn             = row['uprn']
        ptypeid          = row['project_type_id']
        
        for n in output_bl:
          if n['uprn'] == uprn:
            row['building_name']  = n['building_name']
            row['building_type']  = n['building_type']
            break
        for n in output_pt:
          if n['project_type_id'] == ptypeid:
            row['project_type']   = n['name']
            break
        dfprojects.iloc[index]       = row
        
    # Sort the dataframe by uprn to get all projects for buildings together
    
      dfprojects                = dfprojects.sort_values(by='uprn')
      
    # If delivery_date_mode has not been explicitly set (i.e. it is still the default 2022-01-01) then set it to the assessed_delivery_date
    
      tdate = pd.to_datetime('2022-01-01',format = "%Y-%m-%d" )
      dfprojects.loc[(dfprojects.delivery_date_mode == tdate), 'delivery_date_mode'] = dfprojects.assessed_delivery_date
    
    # Put the columns in the order we want them to appear in Excel 
    
      dfprojects                = dfprojects[['uprn','building_name','building_type','project_type', 'baselined', 'assessed', 'project_status', 'utility',  
                                'energy_savings', 'gas_savings', 'electric_savings', 'carbon_savings', 'tonne_co2_lifetime_cost','cost_capex_mode', 'delivery_date_mode' ]]
    
    #====================================================================================================
    #
    # Assemble estate and controlled estate summary results 
    #
    #=====================================================================================================    

      sqle                         = f"SELECT ra.uprn, ra.building_name, ra.latitude_dd, ra.longitude_dd, ra.building_type, ra.under_control, ra.listed, ra.annual_elec_kwh,ra.annual_gas_kwh,ra.annual_oil_kwh,ra.annual_lpg_kwh, ra.dec_score, ra.epc, \
                                      rr.elec_co2, rr.gas_co2, rr.oil_co2, rr.lpg_co2, rr.gas_wtt_scope_3, rr.elec_t_d_scope_3, rr.elec_wtt_t_d_scope_3, rr.elec_wtt_gen_scope_3, rr.oil_wtt_scope_3, rr.lpg_wtt_scope_3,\
                                      rr.total_scope_1, rr.total_scope_2, rr.total_scope_3, rr.total_co2_tco2e, rr.annual_elec_cost, rr.annual_gas_cost, rr.annual_oil_cost, rr.annual_lpg_cost, rr.annual_energy_cost, \
                                      rr.total_kwh, rr.elec_kwh_m2, rr.gas_kwh_m2, rr.bmark_elec_kwh_m2b, rr.bmark_gas_kwh_m2b, rr.elec_2b_saved_2_typical, rr.gas_2b_saved_2_typical, rr.baseline_flag \
                                      FROM raw_estate_data AS ra \
                                      LEFT JOIN results_raw_estate_data AS rr \
                                      ON (ra.uprn = rr.uprn) AND (ra.entity_number = rr.entity_number) \
                                      WHERE ra.entity_number = {entity_number};"
#      cur.execute(sqle)
#      t_output_e         = cur.fetchall()
#      keys               = ("uprn","building_name","latitude_dd","longitude_dd","building_type","under_control","listed","annual_elec_kwh","annual_gas_kwh","annual_oil_kwh","annual_lpg_kwh","dec_score","epc","elec_co2",\
#                            "gas_co2","oil_co2","lpg_co2","gas_wtt_scope_3","elec_t_d_scope_3","elec_wtt_t_d_scope_3","elec_wtt_gen_scope_3","oil_wtt_scope_3","lpg_wtt_scope_3", \
#                            "total_scope_1","total_scope_2","total_scope_3","total_co2_tco2e","annual_elec_cost","annual_gas_cost","annual_oil_cost","annual_lpg_cost","annual_energy_cost", \
#                            "total_kwh","elec_kwh_m2","gas_kwh_m2","bmark_elec_kwh_m2b","bmark_gas_kwh_m2b","elec_2b_saved_2_typical","gas_2b_saved_2_typical","baseline_flag")
#      output_e           = [dict(zip(keys, values)) for values in t_output_e] 
      
    # Convert output_e from dict to pandas dataframe 
#      dfestate           = pd.DataFrame.from_dict (output_e)  
      dfestate            = pd.read_sql_query(sqle, conn) 

    # Add a column to hold DEC rating

      dfestate.insert(loc=12, column='dec_rating', value='', allow_duplicates=True)

    # Calculate DEC rating letters and insert in dec_rating column
      
      for index,row in dfestate.iterrows():
        ds                       = row['dec_score']
        row['dec_rating']        = kc.get_dec_letter(ds)
 #       dfestate.iloc[index]     = row  
        dfestate.iloc[index]     = row 
        
      sqles                      = f"SELECT * FROM controlled_estate_summary WHERE entity_number = {entity_number};"
      cur.execute(sqles)
      keys                 = [column[0] for column in cur.description]
      t_output_es          = cur.fetchall() 
      output_es            = [dict(zip(keys, values)) for values in t_output_es]
      
      dfestatesummary    = pd.DataFrame.from_dict (output_es) 
      
    # Extract estate totals for energy and energy types
    
      tot_estate_energy_cost          = dfestate['annual_energy_cost'].sum()
      tot_estate_energy_kwh           = dfestatesummary.at[0,'total_energy_kwh']
      tot_estate_elec_kwh             = dfestatesummary.at[0,'total_elec_kwh']
      tot_estate_gas_kwh              = dfestatesummary.at[0,'total_gas_kwh']
      tot_estate_zero_carbon_elec     = dfestatesummary.at[0,'total_solar_pv_kwh']
      tot_estate_zero_carbon_heat     = dfestatesummary.at[0,'total_solar_thermal_kwh']
    
    #====================================================================================================
    #
    # Assemble solar summary results 
    #
    #=====================================================================================================  
      sqlses              = f"SELECT * FROM solar_estate_summary WHERE entity_number = {entity_number};"
      cur.execute(sqlses)
      keys                = [column[0] for column in cur.description]
      t_output_ses        = cur.fetchall() 
      output_ses          = [dict(zip(keys, values)) for values in t_output_ses]
      dfsolarsummary      = pd.DataFrame.from_dict (output_ses) 
    
    #====================================================================================================
    #
    # Assemble build_energy_cost table for Treemaps 
    #
    #=====================================================================================================      
    
    # Create empty dataframe (dfbec) to hold the completed table.
    
      dfbec = pd.DataFrame(columns = ['uprn', 'building_name', 'building_type', 'parameter', 'value', 'pc_of_total'])
    #                         index   = [0, 1, 2, 3, 4, 5, 6]) 
      
    # Get the solar pv and solar thermal thermal corrected annual generation kWh for all solar pv and solar thermal projects respectively that were assessed as 'IN PLACE'.
    # NOTE: - build_energy_cost table will have Zero carbon elec and Zero carbon heat for all projects irrespective of assessed status 
      sqlsolpv            = f"SELECT uprn, solar_pv_corrected_ann_gen_kwh FROM projects WHERE entity_number = {entity_number} \
                            AND project_type_id = 20 ;"
      cur.execute(sqlsolpv)
      t_output_solpv      = cur.fetchall()
      keys                = ("uprn","solar_pv_corrected_ann_gen_kwh")
      output_solpv        = [dict(zip(keys, values)) for values in t_output_solpv]
      
      sqlsolth            = f"SELECT uprn, solar_thermal_corrected_ann_gen_kwh FROM projects WHERE entity_number = {entity_number} \
                            AND project_type_id = 21 ;" 
      cur.execute(sqlsolth)
      t_output_solth      = cur.fetchall() 
      keys                = ("uprn","solar_thermal_corrected_ann_gen_kwh")
      output_solth        = [dict(zip(keys, values)) for values in t_output_solth]
      

    # Loop down the rows in the Estate dataframe. For each building (uprn) assemble the required set of parameters in a table fit for Power Bi slicing
      
      sind                = 0
      for index, row in dfestate.iterrows():
        
        uprn              = row['uprn']
        building_name     = row['building_name']
        building_type     = row['building_type']
        zero_carbon_elec_kwh_pa         = 0
        zero_carbon_heat_kwh_pa         = 0
        energy_cost_pa    = decimal.Decimal(row['annual_energy_cost'])
        energy_kwh_pa     = decimal.Decimal(row['total_kwh'])
        gas_kwh_pa        = decimal.Decimal(row['annual_gas_kwh'])
        elec_kwh_pa       = decimal.Decimal(row['annual_elec_kwh'])
        
        for g in output_solpv:
          if g['uprn'] == uprn:
            zero_carbon_elec_kwh_pa     = g['solar_pv_corrected_ann_gen_kwh']
            break
            
        for g in output_solth:
          if g['uprn'] == uprn:
            zero_carbon_heat_kwh_pa     = g['solar_thermal_corrected_ann_gen_kwh']
            break            
            
        # Temporary dataframe dft
        dft = pd.DataFrame(columns=['uprn', 'building_name', 'building_type', 'parameter', 'value', 'pc_of_total'],
                   index = [0, 1, 2, 3, 4, 5])
        dft.loc[[0],['uprn']]                  = uprn
        dft.loc[[0],['building_name']]         = building_name
        dft.loc[[0],['building_type']]         = building_type
        dft.loc[[0],['parameter']]             = 'Total Energy Cost pa'
        dft.loc[[0],['value']]                 = round(energy_cost_pa)    
        dft.loc[[0],['pc_of_total']]           = round((energy_cost_pa / decimal.Decimal(tot_estate_energy_cost)) * 100,2)
        
        dft.loc[[1],['uprn']]                  = uprn
        dft.loc[[1],['building_name']]         = building_name
        dft.loc[[1],['building_type']]         = building_type
        dft.loc[[1],['parameter']]             = 'Total Energy kWh pa'
        dft.loc[[1],['value']]                 = round(energy_kwh_pa)    
        dft.loc[[1],['pc_of_total']]           = round((energy_kwh_pa / decimal.Decimal(tot_estate_energy_kwh)) * 100,2)
        
        dft.loc[[2],['uprn']]                  = uprn
        dft.loc[[2],['building_name']]         = building_name
        dft.loc[[2],['building_type']]         = building_type
        dft.loc[[2],['parameter']]             = 'Gas kWh pa'
        dft.loc[[2],['value']]                 = round(gas_kwh_pa)   
        dft.loc[[2],['pc_of_total']]           = round((gas_kwh_pa / decimal.Decimal(tot_estate_gas_kwh)) * 100,2)
        
        dft.loc[[3],['uprn']]                = uprn
        dft.loc[[3],['building_name']]       = building_name
        dft.loc[[3],['building_type']]       = building_type
        dft.loc[[3],['parameter']]           = 'Electricity kWh pa'
        dft.loc[[3],['value']]               = round(elec_kwh_pa) 
        dft.loc[[3],['pc_of_total']]         = round((elec_kwh_pa / decimal.Decimal(tot_estate_elec_kwh)) * 100,2)
        
        dft.loc[[4],['uprn']]                = uprn
        dft.loc[[4],['building_name']]       = building_name
        dft.loc[[4],['building_type']]       = building_type
        dft.loc[[4],['parameter']]           = 'Zero carbon electricity kWh pa'
        dft.loc[[4],['value']]               = round(zero_carbon_elec_kwh_pa)
        dft.loc[[4],['pc_of_total']]         = round((zero_carbon_elec_kwh_pa / decimal.Decimal(tot_estate_zero_carbon_elec)) * 100,2)
        
        dft.loc[[5],['uprn']]                = uprn
        dft.loc[[5],['building_name']]       = building_name
        dft.loc[[5],['building_type']]       = building_type
        dft.loc[[5],['parameter']]           = 'Zero carbon heat kWh pa'
        dft.loc[[5],['value']]               = round(zero_carbon_heat_kwh_pa)
        dft.loc[[5],['pc_of_total']]         = round((zero_carbon_heat_kwh_pa / decimal.Decimal(tot_estate_zero_carbon_heat)) * 100,2)
        
        dfbec                                = dfbec.append(dft)
        
#     # Rename columns for better display in Power BI

      dfbec.rename(columns={"parameter":"Parameter"}, inplace = True)
      dfbec.rename(columns={"pc_of_total":"Percentage of total"}, inplace = True)
      dfbec.rename(columns={"value":"Value"}, inplace = True)
      dfbec.rename(columns={"building_type":"Building type"}, inplace = True)

    #====================================================================================================
    #
    # Assemble waterfalls table for waterfall visualisations of project savings
    #
    #=====================================================================================================

    # Copy the project savings dataframe
    
      dfwf                       = dfprojects.copy()
    
    # Add a delivery_year column and populate it with the year of delivery date. 
    
      dfwf['delivery_date_mode'] = pd.to_datetime(dfwf['delivery_date_mode'])
    #  dfwf['year']               = dfwf['delivery_date_mode'].dt.year

    # Extract just the columns we need
    
      dfwf1                      = dfwf[['assessed','delivery_date_mode','energy_savings', 'gas_savings', 'electric_savings', 'carbon_savings', 'tonne_co2_lifetime_cost']].copy()
      dfwfall                    = pd.DataFrame(columns=[ 'assessed', 'order','delivery_date', 'parameter', 'value', 'title'])
      
      total_energy_savings       = dfwf['energy_savings'].sum()
      total_electric_savings     = dfwf['electric_savings'].sum()
      total_gas_savings          = dfwf['gas_savings'].sum()
      total_carbon_savings       = dfwf['carbon_savings'].sum()
      
      baseline_energy            = dfestatesummary.at[0,'total_energy_kwh']
      baseline_electric          = dfestatesummary.at[0,'total_elec_kwh']
      baseline_gas               = dfestatesummary.at[0,'total_gas_kwh']
      baseline_carbon            = dfestatesummary.at[0,'co2_total']

      remaining_energy_kwh       = baseline_energy         -       decimal.Decimal(total_energy_savings)
      remaining_electric_kwh     = baseline_electric       -       decimal.Decimal(total_electric_savings)
      remaining_gas_kwh          = baseline_gas            -       decimal.Decimal(total_gas_savings)
      remaining_carbon_tonnes    = baseline_carbon         -       decimal.Decimal(total_carbon_savings)
      
      for index, row in dfwf1.iterrows():
        order                                 = 0
        assessed                              = ''
        ass                                   = row['assessed']
        if ass == 'FIRM':
          assessed       = 'FIRM (0-1yr)'
          order          = 2
        if ass == 'LIKELY':
          assessed       = 'LIKELY (1-2yrs)'
          order          = 3          
        if ass == 'POSSIBLE':
          assessed       = 'POSSIBLE (2-3yrs)'
          order          = 4                   
        if ass == 'POTENTIAL':
          assessed       = 'POTENTIAL (3-5yrs)'
          order          = 5  
        if ass == 'FTHR IMPV':
          assessed       = 'FURTHER IMPROVEMENT (3-5yrs)'
          order          = 6           
         
        delivery_date                         = row['delivery_date_mode']
        energy_savings                        = row['energy_savings']
        gas_savings                           = row['gas_savings']        
        electric_savings                      = row['electric_savings']        
        carbon_savings                        = row['carbon_savings']
        tonne_co2_lifetime_cost               = row['tonne_co2_lifetime_cost']
          
        # Temporary dataframe dft
        dft = pd.DataFrame(columns=[ 'assessed', 'order', 'delivery_date', 'parameter', 'value', 'title'],
                   index = [0, 1, 2, 3, 4])

        dft.loc[[0],['assessed']]              = assessed
        dft.loc[[0],['order']]                 = order
        dft.loc[[0],['delivery_date']]         = delivery_date
        dft.loc[[0],['parameter']]             = 'Energy savings kWh pa'
        dft.loc[[0],['value']]                 = round(-energy_savings)
        dft.loc[[0],['title']]                 = 'Interventions - energy savings - kWh pa'
        
        dft.loc[[1],['assessed']]              = assessed
        dft.loc[[1],['order']]                 = order
        dft.loc[[1],['delivery_date']]         = delivery_date
        dft.loc[[1],['parameter']]             = 'Gas savings kWh pa'
        dft.loc[[1],['value']]                 = round(-gas_savings)
        dft.loc[[1],['title']]                 = 'Interventions - gas savings - kWh pa'
      
        dft.loc[[2],['assessed']]              = assessed
        dft.loc[[2],['order']]                 = order
        dft.loc[[2],['delivery_date']]         = delivery_date
        dft.loc[[2],['parameter']]             = 'Electricity savings kWh pa'
        dft.loc[[2],['value']]                 = round(-electric_savings)    
        dft.loc[[2],['title']]                 = 'Interventions - electricity savings - kWh pa'
        
        dft.loc[[3],['assessed']]              = assessed
        dft.loc[[3],['order']]                 = order
        dft.loc[[3],['delivery_date']]         = delivery_date
        dft.loc[[3],['parameter']]             = 'Carbon savings Tonnes pa'
        dft.loc[[3],['value']]                 = round(-carbon_savings)    
        dft.loc[[3],['title']]                 = 'Interventions - carbon savings - tonnes pa'
        
        dft.loc[[4],['assessed']]              = assessed
        dft.loc[[4],['order']]                 = order
        dft.loc[[4],['delivery_date']]         = delivery_date
        dft.loc[[4],['parameter']]             = '£ CO2 tonnes lifetime cost savings'
        dft.loc[[4],['value']]                 = round(-tonne_co2_lifetime_cost)    
        dft.loc[[4],['title']]                 = 'Interventions - carbon savings - £ CO2 tonnes lifetime '
        
        dfwfall                                = dfwfall.append(dft)
        
    # Insert the baseline and remaining figures
      dft = pd.DataFrame(columns=[ 'assessed', 'order', 'delivery_date', 'parameter', 'value', 'title'],
                  index = [0, 1, 2, 3, 4, 5, 6, 7])    
      
      dft.loc[[0],['assessed']]              = 'BASELINE'
      dft.loc[[0],['order']]                 = 1
      dft.loc[[0],['delivery_date']]         = delivery_date
      dft.loc[[0],['parameter']]             = 'Energy savings kWh pa'
      dft.loc[[0],['value']]                 = round(baseline_energy)
      dft.loc[[0],['title']]                 = 'Intervention energy savings - kWh pa'
      
      dft.loc[[1],['assessed']]              = 'BASELINE'
      dft.loc[[1],['order']]                 = 1
      dft.loc[[1],['delivery_date']]         = delivery_date
      dft.loc[[1],['parameter']]             = 'Gas savings kWh pa'
      dft.loc[[1],['value']]                 = round(baseline_gas)
      dft.loc[[1],['title']]                 = 'Intervention gas savings - kWh pa'
    
      dft.loc[[2],['assessed']]              = 'BASELINE'
      dft.loc[[2],['order']]                 = 1
      dft.loc[[2],['delivery_date']]         = delivery_date
      dft.loc[[2],['parameter']]             = 'Electricity savings kWh pa'
      dft.loc[[2],['value']]                 = round(baseline_electric)    
      dft.loc[[2],['title']]                 = 'Intervention electricity savings - kWh pa'
      
      dft.loc[[3],['assessed']]              = 'BASELINE'
      dft.loc[[3],['order']]                 = 1
      dft.loc[[3],['delivery_date']]         = delivery_date
      dft.loc[[3],['parameter']]             = 'Carbon savings Tonnes pa'
      dft.loc[[3],['value']]                 = round(baseline_carbon)    
      dft.loc[[3],['title']]                 = 'Intervention carbon savings - tonnes pa' 
      
      dft.loc[[4],['assessed']]              = 'REMAINING'
      dft.loc[[4],['order']]                 = 7
      dft.loc[[4],['delivery_date']]         = delivery_date
      dft.loc[[4],['parameter']]             = 'Energy savings kWh pa'
      dft.loc[[4],['value']]                 = round(-remaining_energy_kwh)
      dft.loc[[4],['title']]                 = 'Intervention energy savings - kWh pa'
      
      dft.loc[[5],['assessed']]              = 'REMAINING'
      dft.loc[[5],['order']]                 = 7
      dft.loc[[5],['delivery_date']]         = delivery_date
      dft.loc[[5],['parameter']]             = 'Gas savings kWh pa'
      dft.loc[[5],['value']]                 = round(-remaining_gas_kwh)
      dft.loc[[5],['title']]                 = 'Intervention gas savings - kWh pa'
    
      dft.loc[[6],['assessed']]              = 'REMAINING'
      dft.loc[[6],['order']]                 = 7
      dft.loc[[6],['delivery_date']]         = delivery_date
      dft.loc[[6],['parameter']]             = 'Electricity savings kWh pa'
      dft.loc[[6],['value']]                 = round(-remaining_electric_kwh)    
      dft.loc[[6],['title']]                 = 'Intervention electricity savings - kWh pa'
      
      dft.loc[[7],['assessed']]              = 'REMAINING'
      dft.loc[[7],['order']]                 = 7
      dft.loc[[7],['delivery_date']]         = delivery_date
      dft.loc[[7],['parameter']]             = 'Carbon savings Tonnes pa'
      dft.loc[[7],['value']]                 = round(-remaining_carbon_tonnes)    
      dft.loc[[7],['title']]                 = 'Intervention carbon savings - tonnes pa' 
      
      dfwfall                                = dfwfall.append(dft)
    
    # Rename the 'assessed' column to 'Likelihood' and the 'value' column to 'Value'. These column names are displayed by Power Bi on the axes of visualisations
    # and renamimg them makes the visualisation more readable.
    
      dfwfall.rename(columns={"assessed":"Likelihood"}, inplace = True)
      dfwfall.rename(columns={"value":"Value"}, inplace = True)
      
      # Write dataframes to Excel
      if export_to =="Excel":
        ret                = kc.write_all_results_to_excel(dfprojects, dfestate, dfestatesummary, dfsolarsummary, dfbec, dfwfall, entity_name, user_name, dt_str)
        ret_mess['ef']     = ret['ef']
        ret_mess['em']     = ret['em']
        ret_mess['rmedia'] = ret['abm']
        return ret_mess
      if export_to =="DB":
        ret                = kc.write_all_results_to_db(dfprojects, dfestate, dfbec, dfwfall, entity_number, user_name, dt_str)
        ret_mess['ef']     = ret['ef']
        ret_mess['em']     = ret['em']
        ret_mess['rmedia'] = ret['abm']
        return ret_mess      
  except Exception as e: 
    summary =   summary + f"******An exception has occurred in 'export_all_results_to_excel'- please see upload log for details"

    msg = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    print(msg)
    up_log  = up_log + f"****Exception - in export_all_results_to excel: \n {msg}"
    ret_mess['summary'] = summary
    ret_mess['up_log']  = up_log
    ret_mess['ef']      = 2
    return ret_mess
  

@anvil.server.callable
def get_buildings_with_gaselec_savings_fail(entity_number):
  
# Returns lists of dicts of uprn and building names of buildings in the estate identified by entity_number where
# the gas and electricity savings check flag has been set to 1 (i.e.have failed the check). Returns 1 list for gas
# and 1 for electricity.
#
# entity_number - entity number identifying the estate in question
  ret_mess         = {'ef':0, 'em':'', 'gsub':'', 'esub':''}
  
  gsub             = []
  esub             = []
  
  try:
    
    conn           = initialise_database_connection(app.branch)
    
    tf = 'g_saving_flag'
   
    sqlubg         = f"SELECT uprn, building_name FROM raw_estate_data WHERE (entity_number = {entity_number}) AND ({tf} = 1);"
    
    tf = 'e_saving_flag'    
  
    sqlube         = f"SELECT uprn, building_name FROM raw_estate_data WHERE (entity_number = {entity_number}) AND ({tf} = 1);"
   
  # Convert uprn to string and add boolean 'select' for the buildings table
    
    with conn.cursor() as cursor:
      cursor.execute(sqlubg)
      t_output_ubg       = cursor.fetchall()
      keys               = ("uprn","building_name")
      output_ubg         = [dict(zip(keys, values)) for values in t_output_ubg]
      
      gsub               = []

      for r in output_ubg:
        new_b                  = {'uprn':'', 'building_name':'', 'select':False}
        suprn                  = str(r['uprn'])
        new_b['uprn']          = suprn
        new_b['building_name'] = r['building_name']
        gsub.append(new_b)
      ret_mess['gsub']   = gsub
      
      cursor.execute(sqlube)
      t_output_ube       = cursor.fetchall()
      keys               = ("uprn","building_name")
      output_ube         = [dict(zip(keys, values)) for values in t_output_ube]      

      esub               = []

      for r in output_ube:
        new_b                  = {'uprn':'', 'building_name':'', 'select':False}
        suprn                  = str(r['uprn'])
        new_b['uprn']          = suprn
        new_b['building_name'] = r['building_name']
        esub.append(new_b)      
      ret_mess['esub']   = esub      
      return ret_mess
      
  except Exception as e: 
    msg1 = f"******An exception has occurred in 'get_buildings_with_gaselec_savings_fail' see below:\n"
    msg2 = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    print(f"{msg1}{msg2}\n")
    ret_mess['em']  = "****Error - an exception occured - please see your support team"
    ret_mess['ef']  = 2
    return ret_mess
  return ret_mess

@anvil.server.callable
def get_projects_for_building_with_gaselec_savings_fail( entity_number,buprn, stype):
  
# Returns a list of gas or electricity saving projects, depending on the setting of stype argument - 'Gas' for gas, 'Elec' for electricity.
# Returns project identifiers, project names and percent savings for a building identified by buprn
                                                        
  ret_mess         = {'ef':0, 'em':'', 'prlist':''}
  stype            = stype.upper()
  try:
    gsql = f"SELECT projects.project_id,projects.project_type_id, projects.saving_percent, project_types.name FROM projects \
            INNER JOIN project_types ON projects.project_type_id = project_types.project_type_id \
            WHERE (uprn = {buprn}) AND (entity_number = {entity_number}) AND (utility = \'{stype}\');"
    
    conn           = initialise_database_connection(app.branch)
    
    with conn.cursor() as cursor:
      cursor.execute(gsql)
      t_prlist  = cursor.fetchall()
      keys      = ("project_id","project_type_id","saving_percent","name")
      prlist    = [dict(zip(keys, values)) for values in t_prlist]
      
      # Add a key for 'revised_savings' to each dict and initialise the value to be equal to 'saving_percent'
      for i in range(len(prlist)):
    
        r                   = prlist[i]
        r['revised_saving'] = r['saving_percent']
        prlist[i]           = r
     
      ret_mess['prlist'] = prlist

  except Exception as e: 
    msg1 = f"******An exception has occurred in 'get_projects_for_building_with_gaselec_savings_fail' see below:\n"
    msg2 = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    print(f"{msg1}{msg2}\n")
    ret_mess['em']  = "****Error - an exception occured - please see your support team"
    ret_mess['ef']  = 2
    return ret_mess
  return ret_mess 

@anvil.server.callable
def get_project_types():

# Returns a list of dicts of project_type_id and names
  ret_mess = {'ef':0, 'em':'', 'ptlist':''}
  try:

  # Retrieve the list of project types (name) and their associated id (id)
  # Store list of dicts defining project type names and project type ids e.g. {'name': 'Bioenergy', 'project_type_id': 2}, in Project_types.ptlist
          
    sql1           = f"SELECT name,project_type_id FROM project_types;"

    conn           = initialise_database_connection(app.branch)
    keys           = ('name','project_type_id')
    with conn.cursor() as cursor:
      cursor.execute(sql1)
      t_name_id = cursor.fetchall()
      name_id = [dict(zip(keys, values)) for values in t_name_id]
      if len(name_id)==0:
        ret_mess['ef']  = 1
        ret_mess['em']  = "****Error - no project types found on database"
        return ret_mess
      else:
        ret_mess['ptlist'] = name_id
        # Note: pt is import of module Project_types
        pt.ptlist = ret_mess['ptlist'] 

  except Exception as e: 
    msg1 = f"******An exception has occurred in 'get_project_types' see app log:\n"
    msg2 = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    print(f"{msg1}{msg2}\n")
    ret_mess['ef']  = 2
    ret_mess['em']  = msg1
    return ret_mess
  return ret_mess

@anvil.server.background_task
def upload_forecast_actual_energy_usage_bt(file,entity,partner, client, published, user_name, dt_str, option):
  
# This is the background task for uploading forecast or actual energy usage from an Excel file (.xlsx) depending on the value of the option argument. These are the forecasts made assuming none of the intervention projects take place. 
# If actuals then they are measured historical actuals. 
# Forecasts should include events that would change the baseline energy consumption, such as planned changes in building occupancy or use, building works not related to the zero carbon 
# programme etc.
# The rows of the input Excel worksheet are buildings identified by uprn and building name. Columns are years (YYYY - e.g. 2023).
#
# If the Forecast option is specified each upload workbook should contain the following sheets and the upload will fail if any of these are not present: -
#
# Buildings - each row contains the uprn and name of a building. These are pulled through to the usage sheets from the buildings sheet to ensure all forecasts relate to the same buildings.
#             The header contains the years for all the forecast sheets from Column C onwards. These are pulled through to the usage sheets.
# Forecast Elec - Forecasts for imported electricity, gas , oil, and lpg respectively.
# Forecast Gas  - "    "  
# Forecast Oil  - "    "  
# Forecast LPG  - "    " 
# Forecast Solar PV - Forecast for on-site generation of solar pv
# Forecast Solar Thermal - Forecast for on-site generation of solar thermal
#
# If the Actuals option is specified each upload workbook should contain the following sheets and the upload will fail if any of these are not present: -
#
# Buildings - each row contains the uprn and name of a building. These are pulled through to the usage sheets from the buildings sheet to ensure all actuals relate to the same buildings.
#             The header contains the years for all the actuals sheets from Column C onwards. These are pulled through to the usage sheets.
# Actual Elec - Actuals for imported electricity, gas , oil, and lpg respectively.
# Actual Gas  - "    "  
# Actual Oil  - "    "  
# Actual LPG  - "    " 
# Actual Solar PV - Actuals for on-site generation of solar pv
# Actual Solar Thermal - Actuals for on-site generation of solar thermal 
  # The upload works in REPLACE mode: -
# Any existing records for the entity are deleted and replaced by the records in the upload.
  import math
  try:
    print('Forecast/actuals usage upload start **************************')
    # Open database connection
    conn                = initialise_database_connection(published) 

    capoption           = option.capitalize()
    task_name           = f" {capoption} energy usage" 

    # Create header for the summary and the log. Initialize upload log.
    # dt_str = dd/mm/YY H:M:S    
    header              = f"{task_name} upload by user - {user_name} run on {dt_str} for Partner: {partner}, Client: {client}, Entity {entity} \n File selected : {file.name}\n Task name - {task_name}\n "
    etypes              = ['Electricity', 'Gas', 'Oil','LPG', 'Solar PV','Solar Thermal']
    up_log              = header
    
    # Retrieve the entity number from entities table
        
    entity_number       = anvil.server.call('get_entity_number_v002',entity) 
    
    # Get background task ID and initialise log

    task_id             = anvil.server.context.background_task_id

    kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log) 
    
    if option != 'actual' and option != 'forecast':
      up_log               = up_log + f"***Error invalid option {option} supplied"
      anvil.server.task_state['pc_complete'] = "0"
      anvil.server.task_state['status'] = "****FAILED - see upload log for details"
      kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
      return 

    anvil.server.task_state['pc_complete'] = "0"
    anvil.server.task_state['status'] = f"{task_name} upload starting "

    with anvil.media.TempFile(file) as file_name:
   
      if file == None or entity == None:
        up_log              = up_log  + f"++++++++ No file or entity supplied\n"
        anvil.server.task_state['pc_complete'] = "0"
        anvil.server.task_state['status'] = "****FAILED - No file or entity supplied"
        kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
        return 
      else:
        # Placeholder for the entity column as selected by the user
        dent                = {'entity':entity,'entity_number':entity_number}

        # Check sheets required are in workbook
        if option == 'forecast':
          shn                 = ['Buildings', 'Forecast Elec', 'Forecast Gas', 'Forecast Oil', 'Forecast LPG', 'Forecast Solar PV', 'Forecast Solar Thermal']
        else:
          shn                 = ['Buildings', 'Actual Elec', 'Actual Gas', 'Actual Oil', 'Actual LPG', 'Actual Solar PV', 'Actual Solar Thermal']
        xl                  = pd.ExcelFile(file_name)
        snames              = xl.sheet_names  # see all sheet names
        snfail              = False
        
        # Calculate rough percentage complete for each sheet validated and each energy type calculated
        nsh      = len(shn)
        net      = len(etypes)
        pcpers   = 100/(nsh + net)
        spcpers  = str(round(pcpers))

        for s in shn:
          if s not in snames:
            snfail            = True
            up_log            = up_log  + f"****Error - cannot find sheet called {s} required for {task_name}\n"
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****FAILED - cannot find sheet called {s} required for {task_name}"
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
        if snfail:
          return       
        
        # Create dataframes from each sheet
#        t_sta_read = time.perf_counter()
#        print('Timing before reads of excel  ****************************')
#        print(t_sta_read - tic)              
        dfbu                  = pd.read_excel(file_name, sheet_name = 'Buildings', dtype = object) #Buildings dataframe
#        t_aft_read = time.perf_counter()
#        print('Timing after reads of buildings  ****************************')
#        print(t_aft_read - tic)     
        
        dfel                  = pd.read_excel(file_name, sheet_name = option.capitalize() + ' Elec', dtype = object) # Elec dataframe
        
        dfga                  = pd.read_excel(file_name, sheet_name = option.capitalize() + ' Gas', dtype = object) # Gas dataframe 
        
        dfoi                  = pd.read_excel(file_name, sheet_name = option.capitalize() + ' Oil', dtype = object) # Oil dataframe

        dflp                  = pd.read_excel(file_name, sheet_name = option.capitalize() + ' LPG', dtype = object) # LPG dataframe
        
        dfsp                  = pd.read_excel(file_name, sheet_name = option.capitalize() + ' Solar PV', dtype = object) #Forecast Solar PV dataframe
        
        dfst                  = pd.read_excel(file_name, sheet_name = option.capitalize() + ' Solar Thermal', dtype = object) #Forecast Solar Thermal dataframe
        
        # Select just the uprn and building_name columns in the Buildings dataframe

        dfbu[['uprn','building_name']]
        
#------------------------------------------------------------------------------------------------------------------------------------
#
#      Clean and validate building and forecast dataframes
#
#------------------------------------------------------------------------------------------------------------------------------------
#      Remove all rows in building and forecast dataframes where uprn is zero

        dfbu1 = dfbu[dfbu.uprn != 0].copy()
        dfel1 = dfel[dfel.uprn != 0].copy()
        dfga1 = dfga[dfga.uprn != 0].copy()
        dfoi1 = dfoi[dfoi.uprn != 0].copy()
        dflp1 = dflp[dflp.uprn != 0].copy() 
        dfsp1 = dfsp[dfsp.uprn != 0].copy()
        dfst1 = dfst[dfst.uprn != 0].copy()         

#      Convert Nans to zeros

        dfbu1 = dfbu1.fillna(0)
        dfel1 = dfel1.fillna(0)
        dfga1 = dfga1.fillna(0)
        dfoi1 = dfoi1.fillna(0)
        dflp1 = dflp1.fillna(0)
        dfsp1 = dfsp1.fillna(0)
        dfst1 = dfst1.fillna(0)

#      Validate the buildings uprns

        validation          = kc.validate_forecast_actuals_buildings(conn, entity, entity_number, dfbu1)
  
        ef                  = validation['ef']
        em                  = validation['em']
        df                  = validation['validated_df']

        if ef > 0:
          up_log            = up_log + f"***Error occurred validating {capoption} upload file - buildings: -\n{em}\n"
          anvil.server.task_state['pc_complete'] = "0"
          anvil.server.task_state['status'] = f"****FAILED - Error occurred validating upload file, see upload log for details"
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
          return

        vm                  = validation['validation_messages']

        nvw                 = validation['nvw']
        nve                 = validation['nve']
        up_log              = up_log + f"Validation messages:\n {vm}\n"
        up_log              = up_log + f"Results of buildings validation: - \n    Number of warnings - {nvw}\n    Number of errors  - {nve}\n" 

        snum                = str(round(pcpers*1))
        if nve > 0:
          anvil.server.task_state['pc_complete'] = "0"
          anvil.server.task_state['status'] = f"****FAILED - Buildings validation has failed, see upload log for details."
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
          return
        if nvw > 0:
          anvil.server.task_state['pc_complete'] = snum
          anvil.server.task_state['status'] = f"****Warning - Buildings validation has generated warnings, see upload log for details."
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)      
        else:
          anvil.server.task_state['pc_complete'] = snum
          anvil.server.task_state['status'] = f"Buildings validation has completed successfully"
#------------------------------------------------------------------
#
#       Validate each usage type forecast sheet in turn
#
#------------------------------------------------------------------
        print('++++++BEFORE validate forecast actuals usage')
 
        with conn.cursor() as cursor:
           # Read in energy types and associated codes         

          try:
            cursor.execute("SELECT energy_code, energy_type FROM energy_type_codes")
            t_output_en     = cursor.fetchall()

            keys = ("energy_code","energy_type")
            output_en = [dict(zip(keys, values)) for values in t_output_en]

          except (pyodbc.Error) as e:
            # Rolling back in case of error
            exnum             = 1
            conn.rollback()
            up_log            = up_log + f"Exception number {exnum} - error on DELETE record for building {row.uprn}. Database returned: - \n{e}\n"      
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****Exception {exnum} retrieving energy codes from database. See upload log for details"
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            return               
                    
          nvw     = 0
          nve     = 0          
          for en_type in etypes:

            if en_type == 'Electricity':
              dfin  = dfel1.copy()
            if en_type == 'Gas':
              dfin  = dfga1.copy()
            if en_type == 'Oil':
              dfin  = dfoi1.copy()
            if en_type == 'LPG':
              dfin  = dflp1.copy()
            if en_type == 'Solar PV':
              dfin  = dfsp1.copy()
            if en_type == 'Solar Thermal':
              dfin  = dfst1.copy()
  
            #  Remove the building_name column

            dfin                = dfin.drop(['building_name'],axis=1)
    
            validation          = kc.validate_forecast_actuals_usage_v2(conn, entity, entity_number, dfin, en_type)
      
            ef                  = validation['ef']
            em                  = validation['em']
            
            if ef > 0:
              up_log            = up_log + f"***Exception occurred validating {en_type} usage {option} sheet: -\n{em}\n"
              anvil.server.task_state['pc_complete'] = "0"
              anvil.server.task_state['status'] = f"****Error occurred validating {en_type} usage {option} sheet. See upload log for details"
              kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
              return     
  
            vm                  = validation['validation_messages']

            nvw                 = nvw + validation['nvw']
            nve                 = nve + validation['nve']
            
            up_log              = up_log + f"\n Validation messages for {en_type} :\n {vm}\n"
            up_log              = up_log + f"\n Results of validation for {en_type}: - \n    Number of warnings - {validation['nvw']}\n    Number of errors  - {validation['nve']}\n" 

          snum                  = str(round(pcpers*net))
          if nve > 0:
            up_log = up_log + f"\n ****FAILED - Upload file has failed validation"
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****FAILED - Upload file has failed validation, see upload log for details."
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            return
          if nvw > 0:
            up_log = up_log + f"\n ++++Warning - Upload file validation has generated warnings"
            anvil.server.task_state['pc_complete'] = snum
            anvil.server.task_state['status'] = f"++++Warning - Upload file validation has generated warnings, see upload log for details. Please review."
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
          else:
            up_log = up_log + f"\n Upload file validation completed successfully."            
            anvil.server.task_state['pc_complete'] = snum
            anvil.server.task_state['status'] = f"Upload file validation completed successfully."
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
        
  # We have now validated all 7 dataframes (1 - buildings, 6 -usage) with nothing worse than errors. Loop through the original usage dataframes (with building and uprn columns) and add columns for:
            # entity_number
            # energy_type
  # Then unpivot the dataframe and rename the 'date' column as 'year' containing integer year (e.g. 2022).
  # Finally append the resulting dataframes together and write to the forecasts or actuals table in the database, having first deleted any previous forecast or actuals records for this entity. 
          option_df = pd.DataFrame()

          for en_type in etypes:

            for n in output_en:
              if n['energy_type'] == en_type:
                energy_code = n['energy_code']
                break            
  
            if en_type == 'Electricity':
              dfin  = dfel1.copy()

            if en_type == 'Gas':
              dfin  = dfga1.copy()

            if en_type == 'Oil':
              dfin  = dfoi1.copy()

            if en_type == 'LPG':
              dfin  = dflp1.copy()

            if en_type == 'Solar PV':
              dfin  = dfsp1.copy()

            if en_type == 'Solar Thermal':
              dfin  = dfst1.copy()

            # Add entity_number and energy_type columns
            dfin.insert(0,'entity_number', entity_number)
            dfin.insert(2,'energy_code',energy_code)
  
            # Unpivot
  
            gcol_names = list(dfin.columns)
            gncols     = len(gcol_names)
            gcdates    = gcol_names[4:gncols]
  
            tfor_unp   = pd.melt(dfin, id_vars = ['entity_number', 'uprn', 'energy_code'], value_vars = gcdates, var_name = 'date',value_name = 'kwh') 
  
            option_df = option_df.append(tfor_unp,True)

          col_dates           = option_df['date']
    
          # Replace the 'dates' column with integer years 
          #forecast_df['years'] = pd.to_numeric(forecast_df['years'])
        # Rename date column as year
          option_df.rename(columns = {'date':'year'}, inplace = True)
          sqld  = f"DELETE FROM {option}_energy_usage WHERE (entity_number = {entity_number}) ;" 
        # print('sqld - forecast record delete')
        # print(sqld)
          try:
            cursor.execute(sqld)
            conn.commit()
          
          except (pyodbc.Error) as e:
        # Rolling back in case of error
            exnum             = 1
            conn.rollback()
            up_log            = up_log + f"Exception number {exnum} - error on DELETE records for entity number {entity_number}. Database returned: - \n{e}\n"      
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****FAILED - deleting existing records on database. Please see log file for details"
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            return  
          try:
            odbc_str = Connections.connection_string    
      
            connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(odbc_str)

            engine      = salch.create_engine(connect_str)
            with engine.connect().execution_options(autocommit=False) as conn2:
              txn = conn2.begin()
              option_df.to_sql(f"{option}_energy_usage", con=conn2, if_exists='append', index= False)
      #        print(dir(conn2))
              txn.commit()            
                
          except (pyodbc.Error) as e:
        # Rolling back in case of error
            exnum             = 2
            conn.rollback()
            up_log            = up_log + f"Exception number {exnum} - error on INSERT {option}s for entity number {entity_number}. Database returned: - \n{e}\n"      
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****FAILED - inserting records into database. Please see log file for details"
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            return            
          up_log               = up_log + f"\n ==== {task_name} upload completed successfully."
          anvil.server.task_state['pc_complete'] = "100"
          anvil.server.task_state['status'] = f" {task_name} upload completed successfully."
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
          return 
                    
  except Exception as e: 
    exnum = 3
    msg1 = f"******An exception has occurred in {task_name} see app log:\n"
    msg2 = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    print(f"{msg1}{msg2}\n")
    msg  = msg1 + msg2
    up_log            = up_log + f"Exception number {exnum} - ****ERROR - occurred during {task_name}: - \n{msg}\n"
    print ('In exception at end')
    print(up_log)
    anvil.server.task_state['pc_complete'] = "0"
    anvil.server.task_state['status'] = f"****FAILED - Exception occured during {task_name} - see upload log for details"
    kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
    return 

@anvil.server.background_task
def upload_forecast_actual_energy_cost_bt(file,entity, partner, client, published, user_name, dt_str, option):
  
# This is the background task for uploading forecast or actual energy costs from an Excel file (.xlsx) depending on the value of the option argument.  
#  
# The rows of the input Excel worksheet are buildings identified by uprn and building name. Columns are years (YYYY - e.g. 2023).
#
# If the Forecast option is specified each upload workbook should contain the following sheets and the upload will fail if any of these are not present: -
#
# Buildings - each row contains the uprn and name of a building. These are pulled through to the usage sheets from the buildings sheet to ensure all forecasts relate to the same buildings.
#             The header contains the years for all the usage sheets from Column C onwards. These are pulled through to the usage sheets.
# Forecast Elec cost- Forecast cost per kWh for imported electricity, gas , oil, and lpg respectively.
# Forecast Gas  - "    "  
# Forecast Oil  - "    "  
# Forecast LPG  - "    " 
# Forecast Solar PV - Forecast cost per kWh for on-site generation of solar pv
# Forecast Solar Thermal - Forecast cost per kWh for on-site generation of solar thermal
#
#
# If the Actuals option is specified each upload workbook should contain the following sheets and the upload will fail if any of these are not present: -
#
# Buildings - each row contains the uprn and name of a building. These are pulled through to the usage sheets from the buildings sheet to ensure all actuals relate to the same buildings.
#             The header contains the years for all the usage sheets from Column C onwards. These are pulled through to the usage sheets.
# Actuals Elec - Actuals cost per kWh for imported electricity, gas , oil, and lpg respectively.
# Actuals Gas  - "    "  
# Actuals Oil  - "    "  
# Actuals LPG  - "    " 
# Actuals Solar PV - Actuals for on-site generation of solar pv
# Actuals Solar Thermal - Actuals for on-site generation of solar thermal 
  # The upload works in REPLACE mode: -
# Any existing records for the entity are deleted and replaced by the records in the upload.
  ret_mess ={'up_log': '', 'summary': '', 'ef':0, 'em':''} 
  import math
  try:
    print('Forecast/Actual cost upload start **************************')

    # Open database connection
    conn                = initialise_database_connection(published) 

    capoption           = option.capitalize()
    task_name           = f" {capoption} energy cost" 

    # Create header for the summary and the log. Initialize upload log.
    # dt_str = dd/mm/YY H:M:S    
    header              = f"{task_name} upload by user - {user_name} run on {dt_str} for Partner: {partner}, Client: {client}, Entity {entity} \n File selected : {file.name}\n Task name - {task_name}\n "
    etypes              = ['Electricity', 'Gas', 'Oil','LPG', 'Solar PV','Solar Thermal']
    up_log              = header
    
    # Retrieve the entity number from entities table
        
    entity_number       = anvil.server.call('get_entity_number_v002',entity) 
    
    # Get background task ID and initialise log

    task_id             = anvil.server.context.background_task_id

    kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log) 
    
    if option != 'actual' and option != 'forecast':
      up_log               = up_log + f"***Error invalid option {option} supplied"
      anvil.server.task_state['pc_complete'] = "0"
      anvil.server.task_state['status'] = "****FAILED - see upload log for details"
      kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
      return 

    anvil.server.task_state['pc_complete'] = "0"
    anvil.server.task_state['status'] = f"{task_name} upload starting "
  
    with anvil.media.TempFile(file) as file_name:
   
      if file == None or entity == None:
        up_log              = up_log  + f"++++++++ No file or entity supplied\n"
        anvil.server.task_state['pc_complete'] = "0"
        anvil.server.task_state['status'] = "****FAILED - No file or entity supplied"
        kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
        return 
      else:
      
        # Placeholder for the entity column as selected by the user
        dent                = {'entity':entity,'entity_number':entity_number}
        
        # Open database connection
        conn                = initialise_database_connection(published)
        
        # Check sheets required are in workbook
        if option == 'forecast':
          shn                 = ['Buildings', 'Forecast Elec Cost', 'Forecast Gas Cost', 'Forecast Oil Cost', 'Forecast LPG Cost', 'Forecast Solar PV Cost', 'Forecast Solar Thermal Cost']
        else:
          shn                 = ['Buildings', 'Actual Elec Cost', 'Actual Gas Cost', 'Actual Oil Cost', 'Actual LPG Cost', 'Actual Solar PV Cost', 'Actual Solar Thermal Cost']

        xl                  = pd.ExcelFile(file_name)
        snames              = xl.sheet_names  # see all sheet names
        snfail              = False

        # Calculate rough percentage complete for each sheet validated and each energy type calculated
        nsh      = len(shn)
        net      = len(etypes)
        pcpers   = 100/(nsh + net)
        spcpers  = str(round(pcpers))
        
        for s in shn:
          if s not in snames:
            snfail            = True
            up_log            = up_log  + f"****Error - cannot find sheet called {s} required for {task_name}\n"
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****FAILED - cannot find sheet called {s} required for {task_name}"
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
        if snfail:
          return         
        
        # Create dataframes from each sheet
           
        dfbu                  = pd.read_excel(file_name, sheet_name = 'Buildings', dtype = object) #Buildings dataframe

        dfel                  = pd.read_excel(file_name, sheet_name = option.capitalize() + ' Elec Cost', dtype = object) # Elec dataframe

        dfga                  = pd.read_excel(file_name, sheet_name = option.capitalize() + ' Gas Cost', dtype = object) # Gas dataframe 
        
        dfoi                  = pd.read_excel(file_name, sheet_name = option.capitalize() + ' Oil Cost', dtype = object) # Oil dataframe

        dflp                  = pd.read_excel(file_name, sheet_name = option.capitalize() + ' LPG Cost', dtype = object) # LPG dataframe

        dfsp                  = pd.read_excel(file_name, sheet_name = option.capitalize() + ' Solar PV Cost', dtype = object) #Forecast Solar PV dataframe
        
        dfst                  = pd.read_excel(file_name, sheet_name = option.capitalize() + ' Solar Thermal Cost', dtype = object) #Forecast Solar Thermal dataframe

        # Select just the uprn and building_name columns in the Buildings dataframe

        dfbu[['uprn','building_name']]
        
#------------------------------------------------------------------------------------------------------------------------------------
#
#      Clean and validate building and forecast dataframes
#
#------------------------------------------------------------------------------------------------------------------------------------
#      Remove all rows in building and forecast dataframes where uprn is zero

        dfbu1 = dfbu[dfbu.uprn != 0].copy()
        dfel1 = dfel[dfel.uprn != 0].copy()
        dfga1 = dfga[dfga.uprn != 0].copy()
        dfoi1 = dfoi[dfoi.uprn != 0].copy()
        dflp1 = dflp[dflp.uprn != 0].copy() 
        dfsp1 = dfsp[dfsp.uprn != 0].copy()
        dfst1 = dfst[dfst.uprn != 0].copy()         

#      Convert Nans to zeros

        dfbu1 = dfbu1.fillna(0)
        dfel1 = dfel1.fillna(0)
        dfga1 = dfga1.fillna(0)
        dfoi1 = dfoi1.fillna(0)
        dflp1 = dflp1.fillna(0)
        dfsp1 = dfsp1.fillna(0)
        dfst1 = dfst1.fillna(0)
        
#      Validate the buildings uprns

        validation          = kc.validate_forecast_actuals_buildings(conn, entity, entity_number, dfbu1)
  
        ef                  = validation['ef']
        em                  = validation['em']
        df                  = validation['validated_df']

        if ef > 0:
          up_log            = up_log + f"***Error occurred validating {capoption} upload file - buildings: -\n{em}\n"
          anvil.server.task_state['pc_complete'] = "0"
          anvil.server.task_state['status'] = f"****FAILED - Error occurred validating upload file, see upload log for details"
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
          return 

        vm                  = validation['validation_messages']

        nvw                 = validation['nvw']
        nve                 = validation['nve']
        up_log              = up_log + f"Validation messages:\n {vm}\n"
        up_log              = up_log + f"Results of buildings validation: - \n    Number of warnings - {nvw}\n    Number of errors  - {nve}\n" 

        snum                = str(round(pcpers*1))
        if nve > 0:
          anvil.server.task_state['pc_complete'] = "0"
          anvil.server.task_state['status'] = f"****FAILED - Buildings validation has failed, see upload log for details."
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
          return
        if nvw > 0:
          anvil.server.task_state['pc_complete'] = snum
          anvil.server.task_state['status'] = f"****Warning - Buildings validation has generated warnings, see upload log for details."
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)      
        else:
          anvil.server.task_state['pc_complete'] = snum
          anvil.server.task_state['status'] = f"Buildings validation has completed successfully"

#------------------------------------------------------------------
#
#       Validate each usage type forecast sheet in turn
#
#------------------------------------------------------------------

        with conn.cursor() as cursor:
           # Read in energy types and associated codes         

          try:
            cursor.execute("SELECT energy_code, energy_type FROM energy_type_codes")
            t_output_en     = cursor.fetchall()

            keys = ("energy_code","energy_type")
            output_en = [dict(zip(keys, values)) for values in t_output_en]

          except (pyodbc.Error) as e:
            # Rolling back in case of error
            exnum             = 1
            conn.rollback()
            up_log            = up_log + f"Exception number {exnum} - error on DELETE record for building {row.uprn}. Database returned: - \n{e}\n"      
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****Exception {exnum} retrieving energy codes from database. See upload log for details"
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            return               
                    
          nvw     = 0
          nve     = 0          
          for en_type in etypes:

            if en_type == 'Electricity':
              dfin  = dfel1.copy()
            if en_type == 'Gas':
              dfin  = dfga1.copy()
            if en_type == 'Oil':
              dfin  = dfoi1.copy()
            if en_type == 'LPG':
              dfin  = dflp1.copy()
            if en_type == 'Solar PV':
              dfin  = dfsp1.copy()
            if en_type == 'Solar Thermal':
              dfin  = dfst1.copy()
  
            #  Remove building_name column

            dfin                = dfin.drop(['building_name'],axis=1)
    
            validation          = kc.validate_forecast_actuals_cost_v2(conn, entity, entity_number, dfin, en_type)
      
            ef                  = validation['ef']
            em                  = validation['em']
            
            if ef > 0:
              up_log            = up_log + f"***Exception occurred validating {en_type} usage {option} sheet: -\n{em}\n"
              anvil.server.task_state['pc_complete'] = "0"
              anvil.server.task_state['status'] = f"****Error occurred validating {en_type} usage {option} sheet. See upload log for details"
              kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
              return    

            vm                  = validation['validation_messages']

            nvw                 = nvw + validation['nvw']
            nve                 = nve + validation['nve']
            
            up_log              = up_log + f"\n Validation messages for {en_type} :\n {vm}\n"
            up_log              = up_log + f"\n Results of validation for {en_type}: - \n    Number of warnings - {validation['nvw']}\n    Number of errors  - {validation['nve']}\n" 

          snum                  = str(round(pcpers*net))
          if nve > 0:
            up_log = up_log + f"\n ****FAILED - Upload file has failed validation"
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****FAILED - Upload file has failed validation, see upload log for details."
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            return
          if nvw > 0:
            up_log = up_log + f"\n ++++Warning - Upload file validation has generated warnings"
            anvil.server.task_state['pc_complete'] = snum
            anvil.server.task_state['status'] = f"++++Warning - Upload file validation has generated warnings, see upload log for details. Please review."
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
          else:
            up_log = up_log + f"\n Upload file validation completed successfully."            
            anvil.server.task_state['pc_complete'] = snum
            anvil.server.task_state['status'] = f"Upload file validation completed successfully."
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
        
  # We have now validated all 7 dataframes (1 - buildings, 6 -cost) with nothing worse than errors. Loop through the original cost dataframes (with building and uprn columns) and add columns for:
            # entity_number
            # energy_type
  # Then unpivot the dataframe and rename the 'date' column as 'year' containing integer year (e.g. 2022).
  # Finally append the resulting dataframes together and write to the forecasts or actuals table in the database, having first deleted any previous forecast or actuals records for this entity. 

          option_df = pd.DataFrame()

          for en_type in etypes:

            for n in output_en:
              if n['energy_type'] == en_type:
                energy_code = n['energy_code']
                break            
  
            if en_type == 'Electricity':
              dfin  = dfel1.copy()

            if en_type == 'Gas':
              dfin  = dfga1.copy()

            if en_type == 'Oil':
              dfin  = dfoi1.copy()

            if en_type == 'LPG':
              dfin  = dflp1.copy()

            if en_type == 'Solar PV':
              dfin  = dfsp1.copy()

            if en_type == 'Solar Thermal':
              dfin  = dfst1.copy()

  
            # Add entity_number and energy_type columns
            dfin.insert(0,'entity_number', entity_number)
            dfin.insert(2,'energy_code',energy_code)
  
            # Unpivot
  
            gcol_names = list(dfin.columns)
            gncols     = len(gcol_names)
            gcdates    = gcol_names[4:gncols]
  
            tfor_unp   = pd.melt(dfin, id_vars = ['entity_number', 'uprn', 'energy_code'], value_vars = gcdates, var_name = 'date',value_name = 'cost_per_kwh') 
  
            option_df = option_df.append(tfor_unp,True)
    
          col_dates           = option_df['date']
    
          # Replace the 'dates' column with integer years 
          #forecast_df['years'] = pd.to_numeric(forecast_df['years'])
        # Rename date column as year
          option_df.rename(columns = {'date':'year'}, inplace = True)
          sqld  = f"DELETE FROM {option}_energy_costs WHERE (entity_number = {entity_number}) ;" 
        # print('sqld - forecast record delete')
        # print(sqld)
          try:
            cursor.execute(sqld)
            conn.commit()
          
          except (pyodbc.Error) as e:
        # Rolling back in case of error
            exnum             = 1
            conn.rollback()
            up_log            = up_log + f"Exception number {exnum} - error on DELETE records for entity number {entity_number}. Database returned: - \n{e}\n"      
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****FAILED - deleting existing records on database. Please see log file for details"
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            return  

          try:
            odbc_str = Connections.connection_string    
      
            connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(odbc_str)

            engine      = salch.create_engine(connect_str)
            with engine.connect().execution_options(autocommit=False) as conn2:
              txn = conn2.begin()
              option_df.to_sql(f"{option}_energy_costs", con=conn2, if_exists='append', index= False)
      #        print(dir(conn2))
              txn.commit()            
                
          except (pyodbc.Error) as e:
        # Rolling back in case of error
            exnum             = 2
            conn.rollback()
            up_log            = up_log + f"Exception number {exnum} - error on INSERT {option}s for entity number {entity_number}. Database returned: - \n{e}\n"      
            anvil.server.task_state['pc_complete'] = "0"
            anvil.server.task_state['status'] = f"****FAILED - inserting records into database. Please see log file for details"
            kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
            return                     
          up_log               = up_log + f"\n ==== {task_name} upload completed successfully."
          anvil.server.task_state['pc_complete'] = "100"
          anvil.server.task_state['status'] = f" {task_name} upload completed successfully."
          kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
          return 
                    
  except Exception as e: 
    exnum = 3
    msg1 = f"******An exception has occurred in {task_name} see app log:\n"
    msg2 = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    print(f"{msg1}{msg2}\n")
    msg  = msg1 + msg2
    up_log            = up_log + f"Exception number {exnum} - ****ERROR - occurred during {task_name}: - \n{msg}\n"
    print ('In exception at end')
    print(up_log)
    anvil.server.task_state['pc_complete'] = "0"
    anvil.server.task_state['status'] = f"****FAILED - Exception occured during {task_name} - see upload log for details"
    kc.write_upload_log_2_db(conn, entity_number, user_name, dt_str, task_id, task_name, up_log)
    return 

@anvil.server.callable
def launch_upload_estate_partner_PC_01(file, entity_id, partner_id, client_id, published, user_name, dt_str):
  print('In launch_upload_estate_partner_PC_01_v002=======')
  task = anvil.server.launch_background_task('upload_estate_H4_PC_001_bt',file, entity_id, partner_id, client_id, published, user_name, dt_str)
  return task

@anvil.server.callable
def launch_upload_project_initialisation_data_v001(file, entity_id, partner_id, client_id, published, user_name, dt_str, start_date):
  print('In launch_upload_project_initialisation_data_v001=======')
  print(start_date)
  task = anvil.server.launch_background_task('upload_project_initialisation_data_bt',file, entity_id, partner_id, client_id, published, user_name, dt_str, start_date)
  return task
  
@anvil.server.callable
def launch_upload_project_details_data_v001(file, entity_id, partner_id, client_id, published, user_name, dt_str, project_types):
  print('In launch_upload_project_details_data_v001=======')
  task = anvil.server.launch_background_task('upload_project_details_data_bt',file, entity_id, partner_id, client_id, published, user_name, dt_str, project_types)
  return task
  
@anvil.server.callable 
def launch_upload_forecast_actual_energy_usage_v001(file, entity_id, partner_id, client_id, published, user_name, dt_str):
  task = anvil.server.launch_background_task('upload_forecast_actual_energy_usage_bt',file, entity_id, partner_id, client_id, published, user_name, dt_str)
  return task

@anvil.server.callable 
def launch_upload_forecast_actual_energy_cost_v001(file, entity_id, partner_id, client_id, published, user_name, dt_str):
  task = anvil.server.launch_background_task('upload_forecast_actual_energy_cost_bt',file,entity_id, partner_id, client_id, published, user_name, dt_str)
  return task

# Specials in here==========================================================================
#===========================================================================================
@anvil.server.callable
def special_populate_hp_elec_add_kwh_pa():
  # New column hp_elec_add_kwh_pa has been added to project_results. Populate this with values from projects.hp_elec_add_kwh_pa
    
  ret_mess = {'ef':0, 'em':''}
  conn                 = initialise_database_connection(app.branch)
  print('======Starting special_populate_hp_elec_add_kwh_pa')
  try:
    with conn.cursor() as cursor:
      
      # Get building level details for all controlled buildings in entity
      sqlrd = f"SELECT project_type_id, project_id, hp_elec_add_kwh_pa FROM projects ;"
      cursor.execute(sqlrd)
      t_output_rd      = cursor.fetchall()
      keys             = ("project_type_id", "project_id", "hp_elec_add_kwh_pa")
      output_rd        = [dict(zip(keys, values)) for values in t_output_rd]
      
      # Loop through each building in estate and get required building data and emission factors
      num_proj_pr      = len(output_rd)
      num_proj_prr     = 0
      
      for pr in output_rd:
        project_type_id    = pr['project_type_id']
        project_id         = pr['project_id']
        hp_elec_add_kwh_pa = pr['hp_elec_add_kwh_pa']

        if project_type_id == 14:
          num_proj_prr     = num_proj_prr + 1
          sqlrr = f"UPDATE project_results SET hp_elec_add_kwh_pa = {hp_elec_add_kwh_pa} WHERE project_id = {project_id}"
          cursor.execute(sqlrr)

      print('***Special special_populate_hp_elec_add_kwh_pa completed')
      print(f"Number of projects read = {num_proj_pr}")
      print(f"Number of HEat Pump projects updated in project_results = {num_proj_prr}")
  except Exception as e:
    msg = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    ret_mess['ef'] = 2
    ret_mess['em'] = msg
  return ret_mess

@anvil.server.callable
def launch_correct_task_log_dt_str(published):
  print('In launch_correct_task_log_dt_str=======')
  task = anvil.server.launch_background_task('correct_task_log_dt_str', published)
  return task
  
@anvil.server.callable
def  test_date_rounding(input = '1'):
#  print('AT test of dateutil start ----')
  #day_of_year = datetime.now().timetuple().tm_yday
  #print(day_of_year)
  decnum = kc.get_dec_letter(34)
#  print('decnum')
#  print(decnum)
  stdate = '2024-11-01'
  dtdate = dt.strptime(stdate,'%Y-%m-%d')
#  print(dtdate)
#  print(dtdate.year)
  day_of_year = dtdate.timetuple().tm_yday
#  print(day_of_year)
  #import calendar
#  print(calendar.isleap(dtdate.year))
  life_time = 8.5
  ly = np.floor(life_time)
  lf = life_time - ly
  #ld = np.floor(lf * 365)
#  print('lf')
#  print(lf)
#  print('lf * 365')
#  print(lf*365)
  ld = kc.round_half_up(lf * 365)
#  print('ld')
#  print(ld)
  end_date = dtdate + relativedelta(years = ly, days = ld)
#  print('end date')
#  print(end_date)
#  print('AT test of dateutil end ----')
  return input

@anvil.server.callable
def test_calc_projects(published,entity_number_in):
  print('In test_calc_projects ******')
  # Open database connection
  conn                = initialise_database_connection(published)
  ret_mess            = {'ef':0, 'em':''}
  # Test01
  entity_number       = entity_number_in
  print(f"Entity_number = {entity_number}")
  with conn.cursor() as cursor:
    ret_mess = kc.calc_project_energy_carbon_savings_v5_PC01(conn, entity_number)
    print('Return from calc_project_energy_carbon_savings_v5_PC01')
    
@anvil.server.callable
def test_get_partner_client_from_entity_number(published, entity_number_in):
  print('In test_calc_projects ******')
  # Open database connection
  conn                = initialise_database_connection(published)
  #ret_mess            = {'ef':0, 'em':''}
  # Test01
  entity_number       = entity_number_in
  print(f"Entity_number = {entity_number}")
  with conn.cursor() as cursor:
    ret_mess = kc.get_partner_client_from_entity_number(conn, entity_number)
    print('Return from get_partner_client_from_entity_number')
    return ret_mess
    
@anvil.server.callable
def launch_migration_to_HUB4(published):
  print('In launch_correct_task_log_dt_str=======')
  task = anvil.server.launch_background_task('migration_to_HUB4', published)
  return task

@anvil.server.background_task 
def correct_task_log_dt_str(published):
 # Corrects the format of date time strings in db table task_logs.dtstr from format '%d/%m/%Y %H:%M:%S' to a sortable format '%Y/%m/%d %H:%M:%S'. It extracts the 
 # the correct date and time from the upload log because in some cases the date time string written in column dtstr is set to 'dt_str' by virtue of an old bug
 # which has noew been corrected.
  try:
    print('In correct_task_log_dt_str')
    # Open database connection
    conn                = initialise_database_connection(published)
    ret_mess            = {'ef':0, 'em':''}
    num_recs            = 0

    with conn.cursor() as cursor:
      sqlr                = f"SELECT task_id, dtstr, up_log FROM task_logs;"
      cursor.execute(sqlr)
      npdb     = cursor.fetchall()
      print('****Number of task_logs records: -')
      print(len(npdb))
      for t in npdb: #t is a list of tuples in which 1st element is task_id, 2nd element is dtstr and 3rd element is up_log
        num_recs      = num_recs + 1
        task_id       = t[0]
        log           = t[2]
        new_dtr_str   = kc.extract_dt_str_from_upload_log(log)
        if new_dtr_str == 'Date re-format error':
          ret_mess['ef'] = 2
          ret_mess['em'] = f"Date re-format error on record number: {num_recs} task id = {task_id}\n"
          print(ret_mess['em'])
          #return ret_mess
          
        sqlu          = f"UPDATE task_logs SET dtstr = \'{new_dtr_str}\' WHERE task_id = \'{task_id}\';"
        cursor.execute(sqlu)
        conn.commit()
        
    return ret_mess
  except Exception as e:
    msg = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    print('In Special correct dt_str : Exception occured')
    print(msg)
    ret_mess['ef']    = 2
    ret_mess['em']    = msg
    return ret_mess           

@anvil.server.background_task 
def migration_to_HUB4(published):
 # Copies data from legacy HUB database to HUB 4 database that cannot be copied using the download job/upload job method.
 # These data are: -
 # 1 - Actual usage and actual costs for entity numbers 1200, 1300, 1108
 # 2 - Entries in the entities table for the following entity_id's: -
 #     CLEARWOR	 Clear World
 #     UNI-SALF	 University of Salford
 #     TEST04	   For full end2end test


  try:
    print('In correct_task_log_dt_str')
    # Open database connection
    conn                = initialise_database_connection(published)
    ret_mess            = {'ef':0, 'em':''}
    num_recs            = 0

    with conn.cursor() as cursor:
      sqlr                = f"SELECT task_id, dtstr, up_log FROM task_logs;"
      cursor.execute(sqlr)
      npdb     = cursor.fetchall()
      print('****Number of task_logs records: -')
      print(len(npdb))
      for t in npdb: #t is a list of tuples in which 1st element is task_id, 2nd element is dtstr and 3rd element is up_log
        num_recs      = num_recs + 1
        task_id       = t[0]
        log           = t[2]
        new_dtr_str   = kc.extract_dt_str_from_upload_log(log)
        if new_dtr_str == 'Date re-format error':
          ret_mess['ef'] = 2
          ret_mess['em'] = f"Date re-format error on record number: {num_recs} task id = {task_id}\n"
          print(ret_mess['em'])
          #return ret_mess
          
        sqlu          = f"UPDATE task_logs SET dtstr = \'{new_dtr_str}\' WHERE task_id = \'{task_id}\';"
        cursor.execute(sqlu)
        conn.commit()
        
    return ret_mess
  except Exception as e:
    msg = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    print('In Special correct dt_str : Exception occured')
    print(msg)
    ret_mess['ef']    = 2
    ret_mess['em']    = msg
    return ret_mess           

@anvil.server.callable
def launch_latlong_migration(file,entity,published, user_name, dt_str):
  print('In launch_latlong_migration=======')
  task = anvil.server.launch_background_task('upload_latlong_migration_v001',file, entity, published, user_name, dt_str)
  return task

@anvil.server.background_task 
def upload_latlong_migration_v001(file,entity,published, user_name, dt_str):

# This is for uploading the lat long data prepared off-line to go into raw_estate_data which is copied to new lat and long columns in raw_estate_data
# Source file is 'raw_estate_data lats and longs populated from uprn_lists HUB_4 20230825'
  print('+++++In upload_latlong_migration_v001')
  print(f"---Filename is {file}")
  header                 = f"upload_latlong_migration_v001 - {user_name} run on {dt_str} \n File selected : {file}\n"
  print(header)
  print(' ')  
  up_log                 = header
  summary                = header
  ret_mess               = {'up_log': up_log, 'summary': summary, 'ef': 0, 'em': ''} 
  try:	
    #with anvil.media.TempFile(file) as file_name:
    
    if file == None or entity == None:
      summary             = summary + f"++++++++ No file or entity supplied\n"
      up_log              = up_log  + f"++++++++ No file or entity supplied\n"
      ret_mess            = {'up_log': up_log, 'summary': summary, 'ef':2,'em':f"++++++++ No file or entity supplied\n"}
      return ret_mess
    else:
      
      # Open database connection
      conn                = initialise_database_connection(published)

      with anvil.media.TempFile(file) as file_name:
        
        xl = pd.ExcelFile(file_name)
  
        print(xl.sheet_names) # see all sheet namesRead in the dataframe
        
        dfuprns             = pd.read_excel(file_name, sheet_name = 'raw_estate_data_latlong', dtype = object)
        print('-----At 2 dfuprns')
        print(dfuprns.to_string())  
        # Validate entity_uprn_lists for migration dataframe column headings (keys)
      
        col_heads_read      = list(dfuprns.columns.values)
  
        valid_col_heads     = ['uprn', 'entity_number', 'latitude_dd', 'longitude_dd']
        column_xl           = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z","AA","AB","AC","AD","AE","AF","AG","AH","AI","AJ","AK"]
      
        len_expected        = len(valid_col_heads)
        len_read            = len(col_heads_read)
      
        if len_read != len_expected:
          summary           = summary + f"****ERROR - Mismatch in number of columns found on entity_uprn_lists for migration sheet. Expected {len_expected} but found {len_read}"
          up_log            = up_log + f"****ERROR - Mismatch in number of columns found on entity_uprn_lists for migration sheet. Expected {len_expected} but found {len_read}\n"
          ret_mess['summary'] = summary
          ret_mess['up_log']  = up_log
          ret_mess['ef']      = 2
          ret_mess['em']      = f"****ERROR - Mismatch in number of columns found on entity_uprn_lists for migration sheet. Expected {len_expected} but found {len_read}\n"         
          return ret_mess
      
        check1             = f"\nExpected the following column names but did not find them:- \n"                      
        ic                 = -1
        nerr1              = 0
        for c in valid_col_heads:
          if c not in col_heads_read:
            nerr1         = nerr1 + 1
            check1        = check1 + f"{c}, "
            
        if nerr1 > 0:
          summary         = summary + f"****Error - missing columns in upload entity_uprn_lists for migration sheet - see upload log for details\n"
          up_log          = up_log + f"{check1}\n"
  
        check2            = f"\nFound the following column names which are not valid for the entity_uprn_lists for migration sheet:- \n"
  
        nerr2             = 0
        for c in col_heads_read:
          ic              = ic + 1    
          if c not in valid_col_heads:
            nerr2         = nerr2 + 1
            check2        = check2 + f"{c} in Excel column {column_xl[ic]}\n "
            
        if nerr2 > 0:
          summary         = summary + f"****Error - invalid columns found in upload entity_uprn_lists for migration sheet - see upload log for details\n"
          up_log          = up_log + f"{check2}\n"
        
        if nerr1 > 0 or nerr2 > 0:
          ret_mess['summary'] = summary + f"*** - Upload has been abandonded - see upload log for details, resolve issues and re-submit\n"
          ret_mess['up_log']  = up_log
          ret_mess['ef']      = 2
          ret_mess['em']      = f"*** - Upload has been abandonded - see upload log for details, resolve issues and re-submit\n"
          return ret_mess
                
        # Cleansing and validation of input uprn dataframe
        # Replace nans with 0 in numeric columns and null in text columns
          
        dfuprns['uprn']                           = dfuprns['uprn'].fillna(0)
        dfuprns['entity_number']                  = dfuprns['entity_number'].fillna(0)
        dfuprns['latitude_dd']                    = dfuprns['latitude_dd'].fillna(0)
        dfuprns['longitude_dd']                   = dfuprns['longitude_dd'].fillna(0)
        
  
          # Validate the dataframe
        
        rec_num           = 1  # Record (row) number as seen by user in Excel
        vm                = ''
        nve               = 0
        nupdates          = 0
        ninserts          = 0
        nupdrawer         = 0
        nupdrawwa         = 0
  
        with conn.cursor() as cursor:
          print('-----At 3 with')
          
          for d in dfuprns.to_dict(orient="records"):
            # d is now a dict of {columnname -> value} for this row
  
            vflag         = False
            rec_num       = rec_num + 1
            uprn          = d['uprn']
            entity_number = d['entity_number']
            latitude      = d['latitude_dd']
            longitude     = d['longitude_dd']
  
            if uprn < 1 or uprn > 999999999999999:
              print('In uprn check range')
              print(uprn)
              nve         = nve + 1
              vflag       = True
              vm          = f"UPRN on input record {rec_num} is invalid - it must be a positive integer not greater than 15 digits\n"
              print(vm)
            if not kc.vcl_check_int(uprn):
              nve         = nve + 1
              vflag       = True
              vm          = f"UPRN on input record {rec_num} is invalid - it must be an integer number\n" 
              print(vm)
            if entity_number <= 0:
              nve         = nve + 1
              vflag       = True
              vm          = f"entity_number on input record {rec_num} is invalid - it must be a positive integer \n"            
              print(vm)
            
            if latitude < -90 or latitude > 90:
              nve         = nve + 1
              vflag       = True
              vm          = f"Latitude decimal degrees on input record {rec_num} is out of range -90 to +90\n"
              print(vm)
            if longitude < -180 or longitude > 180:
              nve         = nve + 1
              vflag       = True
              vm          = f"Longitude decimal degrees on input record {rec_num} is out of range -180 to +180\n"              
              print(vm)

            sql_ie          = f"UPDATE raw_estate_data SET latitude_dd = {latitude}, longitude_dd = {longitude} WHERE ((entity_number = {entity_number}) AND (uprn = {uprn}));"
            cursor.execute(sql_ie)
            conn.commit()

            try:
              cursor.execute(sql_ie)
              conn.commit()
#             
            except (pyodbc.Error) as e:
              nupdrawer         = nupdrawer + 1
              conn.rollback()
              exnum             = 3
              up_log            = up_log + f"Exception number {exnum} - UPDATE raw_estate_data - ****ERROR on record number {rec_num}. DB returned: - \n{e}\n"
              summary           = summary + f"****ERROR - a database error has occured during UPDATE raw data. See upload log for details\n"
              ret_mess['ef']       = 2
              ret_mess['em']       = f"****ERROR - a database error has occured during UPDATE raw data. See upload log for details\n"
              ret_mess['summary']  = summary
              ret_mess['up_log']   = up_log
              return ret_mess

        if nve > 0:
          up_log            = up_log + f"***There were {nve} validation errors in the upload file. Failing records have not been processed. Please resolve and resubmit: -\n{vm}\n"
          summary           = summary + f"***There were {nve} validation errors in the upload file. Failing records have not been processed. See upload log for details. Please resolve and resubmit.\n"
          ret_mess          = {'up_log': up_log, 'summary': summary,'ef':2,'em': "****Validation errors - failing records have not been processed. Please resolve and resubmit"}
          return ret_mess 
        else:
          up_log            = up_log + f"++++ Upload completed successfully \n Number of updates performed - {nupdates}\n Number of inserts performed - {ninserts} \n"
          summary           = summary + f"++++ Upload completed successfully \n Number of updates performed - {nupdates}\n Number of inserts performed - {ninserts} \n"
          ret_mess          = {'up_log': up_log, 'summary': summary, 'ef':0, 'em':''}
          return ret_mess
        
  except Exception as e:
    msg = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    print('In Special1 : Exception occured')
    print(msg)
    ret_mess['ef']    = 2
    ret_mess['em']    = msg
    return ret_mess           
# Specials above here===============================    
# End of specials ==========================================================================
#===========================================================================================
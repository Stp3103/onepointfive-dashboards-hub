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

@anvil.server.callable
def call_get_MACC_data(entity_number):
  ret       = {'ef':0, 'em':'', 'x':0, 'y':0, 'pt': 0}
  ret_mess  = get_MACC_data(entity_number)
  ef        = ret_mess['ef']
  em        = ret_mess['em']
  dfb       = ret_mess['data']
  print('In call get MACC')
  print(dfb.to_string())
  x         = dfb['lifetime_tonnes_CO2e'].tolist()
  y         = dfb['annual_abatement_cost_tCO2e'].tolist()
  pt        = dfb['project_type_id'].tolist()

  return ef,em,x,y,pt
  
  
def get_MACC_data(entity_number):
  # Gets the data required to plot a MACC chart from project_results table for entity specified by entity_number.
  
  ret_mess = {'ef':0, 'em':'', 'data':''}
  
  try:
    sqlmaccdata    = f"SELECT entity_number, project_type_id, lifetime_tonnes_CO2e, annual_abatement_cost_tCO2e FROM project_results WHERE entity_number ={entity_number};"

    conn           = initialise_database_connection(app.branch)
    keys           = ('name','project_type_id')
    with conn.cursor() as cursor:
      cursor.execute(sqlmaccdata)
      t_output_bl  = cursor.fetchall()
      keys         = ("entity_number", "project_type_id", "lifetime_tonnes_CO2e", "annual_abatement_cost_tCO2e")
      output_bl    = [dict(zip(keys, values)) for values in t_output_bl]
    
      if len(output_bl) == 0:
        ret_mess['em'] = "+++Warning - no project results have been found for this entity"
        ret_mess['ef'] = 1
        return ret_mess
      
      # Convert dict to dataframe
      
      dfb                = pd.DataFrame.from_dict (output_bl) 
      print(dfb.to_string())
      ret_mess['data']   = dfb
      return ret_mess
  except Exception as e: 
    msg1 = f"******An exception has occurred in 'get_MACC_data'. Please contact OPF support at support@onepointfive.uk:\n"
    msg2 = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    print(f"{msg1}{msg2}\n")
    ret_mess['ef']  = 2
    ret_mess['em']  = msg1
    return ret_mess
  return ret_mess  
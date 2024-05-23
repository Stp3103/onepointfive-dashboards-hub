# Copyright(C) Val-Cloud Ltd 2023. All rights reserved
import anvil.server
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
# This is a module.
# You can define variables and functions here, and use them from any form. For example, in a top-level form:
#

entity_select_dropdown_items = None
client_select_dropdown_items = None
partner_select_dropdown_items = None

selected_entity_item = None
selected_entity_number = 0
selected_entity_name = ''
selected_entity_start_date = None

context_Partner_number    = 0
context_Partner_id        = ''
context_Partner_name      = ''

context_Client_number     = 0
context_Client_id         = ''
context_Client_name       = ''

context_Entity_number     = 0
context_Entity_id         = ''
context_Entity_name       = ''

published = False
cibse_data = None
selected_data_type_4_upload = ''
selected_form_4_download = ''
selected_form_selected_option = ''
upload_user_select_dropdown_items = ['Select type of data here: -','Estate load','Project initialisation','Project details','Actual energy usage', 'Actual energy cost'] 
upload_sysadmin_select_dropdown_items = ['Select type of data here: -','Estate load','Project initialisation','Project details','Actual energy usage', 'Actual energy cost']
logged_in_user_role = ''
environment = None
project_types = None
number_of_buildings = 0
number_of_projects = 0
number_of_gas_savings_check_fails = 0
number_of_elec_savings_check_fails = 0
gas_buildings_savings_fail = []
elec_buildings_savings_fail = []
building_savings_uprn_checked = ''
selected_savings = ''
repeating_panel_1 = ''
repeating_panel_2 = ''         # This is the gaselecsavings adjust_savings_table. The items property is a copy of prlist but with revised % savings formatted as text to 1 decimal place for display
current_total_savings     = 0
revised_total_savings     = 0
prlist                    = [] # List of project details for selected building in gaselecsavings form. Current and revised % savings as numeric
dprlist                   = [] # List of project details for selected building in gaselecsavings form. Current and revised % savings as strings. Used to hold the last valid values as point of restore if a change made by user fails validation
gesav_table2_row          = 0
upload_bt_task_id         = ''
upload_bt_task_name       = ''

release                   = 'H4.PC.003.32'
country_codes             = [{'code' :'AE','name': 'United Arab Emirates'},{'code' :'AL','name': 'Albania'},{'code' :'AT','name': 'Austria'},{'code' :'AU','name': 'Australia'},{'code' :'BA','name': 'Bosnia and Herzegovina'},{'code' :'BB','name': 'Barbados'},\
                             {'code' :'AD','name': 'Andorra'},{'code' :'BG','name': 'Bulgaria'},{'code' :'BM','name': 'Bermuda'},{'code' :'CA','name': 'Canada'},{'code' :'CH','name': 'Switzerland'},{'code' :'CY','name': 'Cyprus'},{'code' :'CZ','name': 'Czech Republic'},\
                             {'code' :'DE','name': 'Germany'},{'code' :'DK','name': 'Denmark'},{'code' :'EE','name': 'Estonia'},{'code' :'EG','name': 'Egypt'},{'code' :'ES','name': 'Spain'},{'code' :'FI','name': 'Finland'},{'code' :'FK','name': 'Falkland Islands (Malvinas)'},\
                             {'code' :'FO','name': 'Faroe Islands'},{'code' :'FR','name': 'France'},{'code' :'GB','name': 'United Kingdom of Great Britain and Northern Ireland'},{'code' :'GG','name': 'Guernsey'},{'code' :'GI','name': 'Gibraltar'},{'code' :'GR','name': 'Greece'},\
                             {'code' :'HK','name': 'Hong Kong'},{'code' :'HR','name': 'Croatia'},{'code' :'HU','name': 'Hungary'},{'code' :'IE','name': 'Ireland'},{'code' :'IL','name': 'Israel'},{'code' :'IM','name': 'Isle of Man'},{'code' :'IS','name': 'Iceland'},\
                             {'code' :'IT','name': 'Italy'},{'code' :'JE','name': 'Jersey'},{'code' :'JP','name': 'Japan'},{'code' :'KR','name': 'Korea (Republic of)'},{'code' :'KW','name': 'Kuwait'},{'code' :'KY','name': 'Cayman Islands'},{'code' :'LI','name': 'Liechtenstein'},\
                             {'code' :'LT','name': 'Lithuania'},{'code' :'LU','name': 'Luxembourg'},{'code' :'LV','name': 'Latvia'},{'code' :'MD','name': 'Moldova (Republic of)'},{'code' :'ME','name': 'Montenegro'},{'code' :'MK','name': 'Macedonia (the former Yugoslav Republic of)'},\
                             {'code' :'MT','name': 'Malta'},{'code' :'NL','name': 'Netherlands'},{'code' :'NO','name': 'Norway'},{'code' :'NZ','name': 'New Zealand'},{'code' :'PL','name': 'Poland'},{'code' :'PT','name': 'Portugal'},{'code' :'RO','name': 'Romania'},\
                             {'code' :'RS','name': 'Serbia'},{'code' :'SE','name': 'Sweden'},{'code' :'SI','name': 'Slovenia'},{'code' :'SK','name': 'Slovakia'},{'code' :'SM','name': 'San Marino'},{'code' :'TC','name': 'Turks and Caicos Islands'},{'code' :'TR','name': 'Turkey'},\
                             {'code' :'UA','name': 'Ukraine'},{'code' :'US','name': 'United States of America'}]


#gas_elec_savings_checks_link = None # Points to the link component in main menu for gas/elec savings checks
#download_results_to_excel_link = None # Points to the download results link in main menu

def is_str_numeric(x):
  # Test if a string is numeric.
  # is_str_numeric("5.9") returns True
  # is_str_numeric("5") returns True
  # is_str_numeric("xyz") returns False
  try:
    float(x)
    return True
  except ValueError:
    return False

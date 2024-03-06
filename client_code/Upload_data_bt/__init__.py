# Copyright(C) Val-Cloud Ltd 2023. All rights reserved
from ._anvil_designer import Upload_data_btTemplate
from anvil import *
import anvil.server
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
from .. import Globals
from datetime import datetime
#from ..Main_menu_form import Main_menu_form

class Upload_data_bt(Upload_data_btTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)
    
    if Globals.logged_in_user_role == 'sysadmin':
      self.upload_data_select_type_of_data.items = Globals.upload_sysadmin_select_dropdown_items
    else:
      self.upload_data_select_type_of_data.items = Globals.upload_user_select_dropdown_items

    # Any code you write here will run when the form opens.
    self.file_loader_1.enabled            = False
    #alert("In upload data form")
  def select_upload_type_of_data(self, **event_args):
    """This method is called when an item is selected"""
    Globals.selected_data_type_4_upload   = self.upload_data_select_type_of_data.selected_value
    self.file_loader_1.enabled            = True

    print('In upload_data_select_type_of_data')
    print(Globals.selected_data_type_4_upload)
    pass

  def file_loader_1_change(self, file, **event_args):
    """This method is called when the button is clicked"""
    # Set the timer to 0.5 secs and make progress Flowpanel visible
    self.timer_1.interval     = 0.5
    self.flow_panel_1.visible = True
    self.pc_complete_label.text = '' 
    self.status_label.text      = ''

    # Call the appropriate server function
    user_row       = anvil.users.get_user()
    user_name      = user_row["email"]
    now            = datetime.now()
    dt_str         = now.strftime("%Y/%m/%d %H:%M:%S")
    type_of_upload = Globals.selected_data_type_4_upload
    if type_of_upload == '' or type_of_upload == 'Select type of data here: -':
      return
    if type_of_upload == 'Estate load':

      self.task = anvil.server.call('launch_upload_estate_partner_PC_01',file, Globals.context_Entity_id, Globals.context_Partner_id, Globals.context_Client_id, Globals.published, user_name, dt_str)     
    
    elif type_of_upload == 'Project initialisation':
    
      self.task = anvil.server.call('launch_upload_project_initialisation_data_v001',file, Globals.context_Entity_id, Globals.context_Partner_id, Globals.context_Client_id,Globals.published, user_name, dt_str, Globals.selected_entity_start_date)

    elif type_of_upload == 'Project details':
    
      self.task = anvil.server.call('launch_upload_project_details_data_v001',file, Globals.context_Entity_id, Globals.context_Partner_id, Globals.context_Client_id, Globals.published, user_name, dt_str, Globals.project_types)

    elif type_of_upload == 'Actual energy usage':

     self.task = anvil.server.call('launch_upload_forecast_actual_energy_usage_v001',file,Globals.context_Entity_id, Globals.context_Partner_id, Globals.context_Client_id, Globals.published, user_name, dt_str)

    elif type_of_upload == 'Actual energy cost':

      self.task = anvil.server.call('launch_upload_forecast_actual_energy_cost_v001',file, Globals.context_Entity_id, Globals.context_Partner_id, Globals.context_Client_id,Globals.published, user_name, dt_str)

    Globals.upload_bt_task_id    = self.task.get_id()
    Globals.upload_bt_task_name  = type_of_upload
    self.file_loader_1.enabled = False

    pass
  def info_button_click(self, **event_args):
    upload_option = Globals.selected_data_type_4_upload
    print('In info_button_click. upload option: -')
    print(upload_option)
    if upload_option == 'Select type of data here: -' or upload_option == '':
      txt = f"Use the dropdown to select the type of data you want to upload.Only upload data from a data upload form created for you by the Hub. These forms can be downloaded from the Get Data Capture Forms menu.\n"
      alert(txt)
    """This method is called when the button is clicked"""
    pass

  def bt_monitor(self, **event_args):
    """This method is called Every [interval] seconds. Does not trigger if [interval] is 0."""
    # Show progress
    try:
      state       = self.task.get_state()
      pc_complete = state.get('pc_complete')
      status      = state.get('status')
  
      self.flow_panel_1.visible   = True
      self.pc_complete_label.text = pc_complete 
      self.status_label.text      = status
  
  # Switch Timer off and enable upload button if process is not running 
      if not self.task.is_running():
        self.timer_1.interval = 0
        self.file_loader_1.enabled = True
        alert(f"{Globals.upload_bt_task_name} upload has finished")
  
  
      pass# Hide the loading spinner so the user is not interrup with anvil.server.no_loading_indicator:
    except Exception as e: #Task object may take a few seconds to be created
      pass

  def view_upload_log(self, **event_args):
    """This method is called when the button is clicked"""
    log  = anvil.server.call('get_upload_log', Globals.published, Globals.upload_bt_task_id)
    alert(log,title = f"Log for {Globals.upload_bt_task_name} upload",large = True)
    pass




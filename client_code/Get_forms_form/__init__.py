# Copyright(C) Val-Cloud Ltd 2023. All rights reserved
from ._anvil_designer import Get_forms_formTemplate
from anvil import *
import anvil.server
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
from .. import Globals
from datetime import datetime

class Get_forms_form(Get_forms_formTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Any code you write here will run when the form opens.
    self.download_form_button.enabled = False
    
  def select_form_for_download(self, **event_args):
    Globals.selected_form_4_download   = self.select_type_of_form.selected_value
    print('In select_form_4_downloada')
    print(Globals.selected_form_4_download)
    """This method is called when an item is selected"""
    self.download_form_button.enabled = True
    pass

  def download_forms_click(self, **event_args):
    """This method is called when the button is clicked"""
    print('In top download_forms_click')
    form_option    = Globals.selected_form_selected_option
  
    user_row       = anvil.users.get_user()
    user_name      = user_row["email"]
    now            = datetime.now()
    dt_str         = now.strftime("%d/%m/%Y %H:%M:%S")
   
    try:
      form_type      = Globals.selected_form_4_download
  
      if form_type == 'Select type of form here: -' or form_type == '':
        return
    
      form_option    = Globals.selected_form_selected_option
  
      user_row       = anvil.users.get_user()
      user_name      = user_row["email"]
      now            = datetime.now()
      dt_str         = now.strftime("%d/%m/%Y %H:%M:%S")


      if form_type == 'Partner Estate Data':
#        ret_mess       = anvil.server.call("export_estate_upload_form_to_excel", Globals.selected_entity_number,Globals.selected_entity_item, Globals.published, user_name, dt_str, form_option)
# export_estate_upload_form_to_excel_PC_01
        ret_mess       = anvil.server.call("export_estate_upload_form_to_excel_PC_01", Globals.context_Entity_number, Globals.context_Entity_id, Globals.context_Partner_id, Globals.context_Client_id, Globals.published, user_name, dt_str, form_option)

      elif form_type == 'Project initialisation':
        ret_mess       = anvil.server.call("export_project_initialisation_form_to_excel", Globals.context_Entity_number,Globals.context_Entity_id, Globals.context_Partner_id, Globals.context_Client_id, Globals.published, user_name, dt_str, form_option)
    
      elif form_type == 'Project details':
        ret_mess       = anvil.server.call("export_project_details_form_to_excel", Globals.context_Entity_number,Globals.context_Entity_id, Globals.context_Partner_id, Globals.context_Client_id, Globals.published, user_name, dt_str, form_option)

      elif form_type == 'Specials':
        ret_mess       = anvil.server.call("special_populate_hp_elec_add_kwh_pa")
      else:
        alert(f"Form for {form_type} not available yet")
        self.download_form_button.enabled = False
        return    

      ef             = ret_mess['ef']
      if ef > 0:
	    em             = ret_mess['em']
	    alert(em)
      else:
	    rmedia         = ret_mess['rmedia']
	    anvil.media.download(rmedia)
        
      self.download_form_button.enabled = False
    except Exception as e:
      print(f"****ERROR - downloading {form_type} form \n {e} \n User name: {user_name} date time {dt_str}. Entity is: {Globals.selected_entity_item}")
      alert(f"****ERROR - downloading {form_type} form - please contact support team")
      self.download_form_button.enabled = False
    pass

  def text_box_1_pressed_enter(self, **event_args):
    """This method is called when the user presses Enter in this text box"""
    pass




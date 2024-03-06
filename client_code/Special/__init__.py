# Copyright(C) Val-Cloud Ltd 2023. All rights reserved
from ._anvil_designer import SpecialTemplate
from anvil import *
import anvil.server
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
from datetime import datetime
from .. import Globals

class Special(SpecialTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)
    # Check an entity has been selected otherwise disable file download button
    if Globals.selected_entity_item == None:
      self.download_results_form_display_entity_message.content = "No entity selected. Please select an entity from the Home form"
      self.file_loader_1.enabled = True
      pass
    else:

      self.file_loader_1.enabled = True
      pass

  def download_results_click(self, **event_args):
    print('In top download_results_click')
    print(Globals.selected_entity_name)
    try:
      user_row       = anvil.users.get_user()
      user_name      = user_row["email"]
      print('Inside try in download_results_click - user_name')
      print(user_name)
      now            = datetime.now()
      dt_str         = now.strftime("%d/%m/%Y %H:%M:%S")
      ret_mess       = anvil.server.call("export_all_results", "Excel",Globals.selected_entity_number,Globals.selected_entity_name, Globals.published, user_name, dt_str)
      print('In download_results_click after server call')

      ef             = ret_mess['ef']
      em             = ret_mess['em']
      print(' After call to export_all_results - ef and em')
      print(ef)
      print(em)
      if ef == 0:
        alert("++++Results file successfully created and will download shortly.")
        rmedia         = ret_mess['rmedia']
        anvil.media.download(rmedia)
      elif ef > 0:
        alert(em)

    except Exception as e:
      print(f"****Error in download_results button: - \n User name: {user_name}, entity: {Globals.selected_entity_name}, datetime: {dt_str}")
      print(e)
      alert("****ERROR - downloading results to Excel - please contact support team")

  def button_1_click(self, **event_args):
    user_row       = anvil.users.get_user()
    user_name      = user_row["email"]
    now            = datetime.now()
    dt_str         = now.strftime("%d/%m/%Y %H:%M:%S")
    file           = "C:\Data\VCL projects\Dev\OnePointFive\HUB 4 development lifecycle\4. Build\Lats and Longs in xl.xlsx"

    #"""This method is called when the button is clicked"""
    pass

  def file_loader_1_change(self, file, **event_args):
    """This method is called when a new file is loaded into this FileLoader"""
   # Call the appropriate server function
    user_row       = anvil.users.get_user()
    user_name      = user_row["email"]
    now            = datetime.now()
    dt_str         = now.strftime("%d/%m/%Y %H:%M:%S")
    ret_mess = anvil.server.call('launch_latlong_migration',file, Globals.selected_entity_item,Globals.published, user_name, dt_str)
    alert('Exiting upload_latlong')
    print(ret_mess['up_log'])
    pass

  def text_box_1_pressed_enter(self, **event_args):
    """This method is called when the user presses Enter in this text box"""
    pass


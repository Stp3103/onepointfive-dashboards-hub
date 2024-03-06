# Copyright(C) Val-Cloud Ltd 2023. All rights reserved
from ._anvil_designer import Login_formTemplate
from anvil import *
import anvil.server
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import custom_signup.login_flow
from .. import Globals
from .. import Main_menu_form as mmf
class Login_form(Login_formTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Any code you write here will run when the form opens.
    self.update_login_status()

    # Determine whether app is published (ie - production)
    print(type(app.branch))
    if app.branch == "published":
      Globals.published = True
    Globals.environment = anvil.app.environment.name  
    print('Environment detected at login : -')
    print(Globals.environment)
    print(Globals.release)
    self.Release.text = f"Release " + Globals.release
         
    
  def update_login_status(self):
    # Get the currently logged in user (if any)
    user = anvil.users.get_user()

    if user is None:
      self.login_status.text = "You are not logged in"
      self.enter_button.enabled = False
    else:
      self.login_status.text = f"You are logged in as {user['email']}"
      self.enter_button.enabled = True
      self.login_button.enabled = False
      Globals.logged_in_user_role = user['role']
      print(user['role'])
      print(user['email'])
#      self.user_entities = user['entity']
      
#      if user['role'] != 'sysadmin' and user['entity'] != 'ANY':  #Changed to enable user role with entity ANY to access all entities
#       print(self.user_entities.split(','))
#       Globals.entity_select_dropdown_items =  self.user_entities.split(',')
#      else:
#       #Note: exclude reporting only entities
#       self.entnames = anvil.server.call('get_all_entity_codes_except_reporting_only') 
#      Globals.entity_select_dropdown_items = self.entnames
      if user['role'] == 'sysadmin':
        Globals.partner_select_dropdown_items = anvil.server.call('get_all_partner_codes')
        print(f"In update_login_status - parids = {Globals.partner_select_dropdown_items} \n")
        
      else:
        user_email                           = user['email']
        ret                                  = anvil.server.call('get_user_partner_details', user_email)
        Globals.context_Partner_number       = ret['partner_number']
        Globals.context_Partner_id           = ret['partner_id']
        Globals.context_Partner_name         = ret['partner_name']
        print(f"At end of assigns from ret =====\n")
        Globals.client_select_dropdown_items = anvil.server.call('get_all_client_codes', Globals.context_Partner_number)

        print('****AT END update_login_status')
        print(f"Globals.partner_select_dropdown_items = {Globals.partner_select_dropdown_items}\n")
        print(f"Globals.context_Partner_number = {Globals.context_Partner_number}\n")
        print(f"Globals.context_Partner_id = {Globals.context_Partner_id}\n")
        print(f"Globals.context_Partner_name = {Globals.context_Partner_name}\n")
        print(f" Globals.client_select_dropdown_items = { Globals.client_select_dropdown_items }\n")
      pass
  def login_button_click(self, **event_args):
    custom_signup.login_flow.login_with_form()
    self.update_login_status() # add this line

  def enter_button_click(self, **event_args):
    """This method is called when the button is clicked"""
    open_form('Main_menu_form')
    pass



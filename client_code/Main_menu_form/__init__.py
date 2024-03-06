# Copyright(C) Val-Cloud Ltd 2023. All rights reserved
from ._anvil_designer import Main_menu_formTemplate
from anvil import *
import anvil.server
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
from .. import Globals
from anvil_extras import routing

class Main_menu_form(Main_menu_formTemplate):
  def __init__(self, **properties):
    # Any code you write here will run when the form opens.
    # Set Form properties and Data Bindings.
    self.init_components(**properties)
    #alert("Alert 1")
    # Get the currently logged in user (if any)
    user = anvil.users.get_user() 
#~~~~~
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
    #alert("Alert 2")
    # Determine whether app is published (ie - production)
    print(type(app.branch))
    if app.branch == "published":
      Globals.published = True
    Globals.environment = anvil.app.environment.name  
    print('Environment detected at login : -')
    print(Globals.environment)
    print(Globals.release)
    self.Release.text = f"Release " + Globals.release

    # If user has role sysadmin enable and make visible the Select Partner label and drop down and initialise the partner dropdown with
    # a list of all Partners in the system

    self.set_menu_off() #Turn off menu options that need a context set. Setting the context is the job of the Home screen.
    self.reports_link.visible =False
    self.reports_link.enable = False
    if Globals.logged_in_user_role == 'sysadmin' :
      
      self.text_select_a_partner.visible = True
      self.drop_down_partner.visible = True
      self.drop_down_partner.enabled = True
      self.drop_down_partner.items = Globals.partner_select_dropdown_items
      self.no_partner_selected_message.visible = True
    else:
      self.text_select_a_partner.visible = False
      self.no_partner_selected_message.visible = False
      self.drop_down_partner.visible = False
      self.drop_down_partner.enabled = False
      self.drop_down_partner.items = []
      #self.sys_admin_link.visible = False
      #self.sys_admin_link.enable = False
      welc_text = f"Welcome to the {Globals.context_Partner_name} estate decarbonisation HUB."
      self.label_welcome.text = welc_text
      self.selected_partner.text = 'Partner: ' + Globals.context_Partner_name
      self.drop_down_client.items = anvil.server.call('get_all_client_codes',Globals.context_Partner_number)
      if Globals.logged_in_user_role == 'partneradmin' :
        self.reports_link.visible =True
        self.reports_link.enable = True

#    if Globals.logged_in_user_role == 'sysadmin' or Globals.logged_in_user_role == 'partneradmin':
#      self.administration_link.visible = True
#      self.administration_link.enable = True
#    else:
     
 # Load project types into Project_types module
    ret = anvil.server.call('get_project_types')

    ef  = ret['ef']
    if ef > 0:
      em = ret['em']
      alert(f"***Error - problem retrieving project types - {em}")
      return
    else:
      Globals.project_types = ret['ptlist']
      #print('Globals.project_types xxxxxxxxx')
     # print(Globals.project_types)
      
    self.date_picker_1.format="%d - %B - %Y"
#    self.entity_select_dropdown.selected_value = Globals.selected_entity_item
    user = anvil.users.get_user()
#    self.entity_select_dropdown.items = Globals.entity_select_dropdown_items
#    self.check_entity_start_date_provided()
#    self.check_entity_has_been_selected()
#    self.home_link.role = 'selected'
    
#    if Globals.selected_entity_item == None:
#      self.selected_entity.text = 'No entity selected'
#    else:
#      self.selected_entity.text = 'Selected Entity: ' + Globals.selected_entity_item
#      stats1  = f"Number of buildings in estate : {Globals.number_of_buildings}. Number of projects : {Globals.number_of_projects}"
#      stats2  = f"Numbers of buildings failing savings checks - for gas: {Globals.number_of_gas_savings_check_fails} and for electricity: { Globals.number_of_elec_savings_check_fails}"
#      self.summary_stats_1.text = stats1
#      self.summary_stats_2.text = stats2
      
#      print('Globals.number_of_gas_savings_check_fails')
#      print(Globals.number_of_gas_savings_check_fails)
#      print('Globals.number_of_elec_savings_check_fails')
#      print(Globals.number_of_elec_savings_check_fails)

#      if Globals.number_of_gas_savings_check_fails > 0 or Globals.number_of_elec_savings_check_fails > 0:
#        self.gas_elec_savings_checks_link.visible = True
#        self.gas_elec_savings_checks_link.foreground = '#ca0707'
#        self.gas_elec_savings_checks_link.text = 'Gas/Elec savings checks**'
#        self.download_results_to_excel_link.visible = False
#      else:
#        self.gas_elec_savings_checks_link.visible = False
#        self.gas_elec_savings_checks_link.foreground = 'theme:Black'
#        self.gas_elec_savings_checks_link.text = 'Gas/Elec savings checks'
 #       self.download_results_to_excel_link.visible = True
    pass

  def menu_logout_click(self, **event_args):
    user = anvil.users.get_user()
    if user != None:
        # Clear down Globals
       Globals.entity_select_dropdown_items = None
       Globals.selected_entity_item = None
       Globals.published = False
        # Close the database connection
       anvil.server.call("close_database_connection")
        #Logout the current user
       anvil.users.logout()
        #Present login form
       open_form('Login_form')
    pass
  
  def check_entity_start_date_provided(self, **event_args):
    sd = anvil.server.call('get_programme_start_date_v002',Globals.context_Entity_id)
    print('In check_entity_start_date_provided')
    print('sd')
    print(sd)
    if sd == None:
      Globals.selected_entity_start_date = None
      self.date_picker_1.date            = None
      self.no_start_date_specified.text  = "No start date specified"
    else:
      Globals.selected_entity_start_date = sd
      self.date_picker_1.date            = sd
      self.no_start_date_specified.text  = None
    pass
  def check_entity_has_been_selected(self, **event_args): 
    if Globals.selected_entity_item == None:
      self.no_entity_selected_message.text = "No estate selected"
      self.no_start_date_specified.text = "No start date specified"
      self.date_picker_1.date = None
      self.date_picker_1.enabled = False
      self.get_data_capture_forms_link.visible = False
      self.upload_data_link.visible = False
      self.gas_elec_savings_checks_link.visible = False
      self.download_results_to_excel_link.visible = False
      self.scenarios_link.visible = False
      self.progress_projects_link.visible = False
        
    else:
      self.no_entity_selected_message.text = ""
      self.date_picker_1.enabled = True
      self.get_data_capture_forms_link.visible = True
      self.upload_data_link.visible = True
      #self.gas_elec_savings_checks_link.visible = True
      #self.download_results_to_excel_link.visible = True
      self.scenarios_link.visible = True
      self.progress_projects_link.visible = True      

    print("In change")
    print(Globals.selected_entity_item) 
    pass
  
  def menu_home_click(self, **event_args):
    self.reset_links()
    self.home_link.role = 'selected'
    #alert("In menu_home_click")
    """This method is called when the link is clicked"""
    open_form('Main_menu_form')
    pass

  def menu_change_password_click(self, **event_args):
    self.reset_links()
    self.change_password_link.role = 'selected'
    """This method is called when the link is clicked"""
    anvil.users.change_password_with_form() 
    pass

  def kv_programme_start_date_change(self, **event_args):
    """This method is called when the selected date changes"""

    # Check to see if date selected is different to the programme start date already stored for this entity
    
    dpicked = self.date_picker_1.date
    dstored = Globals.selected_entity_start_date
    published = Globals.published
    if dstored == dpicked:
      return
    else:
      c = confirm("Do you wish to save the new date?")
    # c will be True if the user clicked 'Yes'
      if c:
        ret = anvil.server.call('save_programme_start_date',Globals.context_Entity_number,dpicked, published)
        ef = ret['ef']
        em = ret['em']
        if ef > 0:
          mess = f"****ERROR - saving programme start date for entity {Globals.selected_entity_number} \n {em}\n"
          alert("Error saving programme start date - see your support team")
        else:
          self.no_start_date_specified.text = None
      else:
        self.no_start_date_specified.text = "No start date specified"
        self.date_picker_1.date = None
      return
    pass
  
  def menu_scenarios_click(self, **event_args):
    self.reset_links()
    self.scenarios_link.role = 'selected'
    """This method is called when the link is clicked"""
    pass

  def menu_upload_data_click(self, **event_args):
    self.reset_links()
    self.upload_data_link.role = 'selected'
    """This method is called when the link is clicked"""
    from ..Upload_data_bt import Upload_data_bt
    new_panel = Upload_data_bt()
    get_open_form().content_panel.clear()
    get_open_form().content_panel.add_component(new_panel)
    pass

  def menu_download_results_click(self, **event_args):
    """This method is called when the link is clicked"""
    self.reset_links()
    self.download_results_to_excel_link.role = 'selected'
    from ..Download_results_form import Download_results_form
    new_panel = Download_results_form()
    get_open_form().content_panel.clear()
    get_open_form().content_panel.add_component(new_panel)    
    pass

  def menu_get_data_capture_forms_click(self, **event_args):
    """This method is called when the link is clicked"""
    self.reset_links()
    self.get_data_capture_forms_link.role = 'selected'
    from ..Get_forms_form import Get_forms_form
    new_panel = Get_forms_form()
    get_open_form().content_panel.clear()
    get_open_form().content_panel.add_component(new_panel)    
    pass

  def menu_progress_projects_click(self, **event_args):
    self.reset_links()
    self.progress_projects_link.role = 'selected'
    """This method is called when the link is clicked"""
    pass
  def reset_links(self, **event_args):	
	self.home_link.role = ''
	self.change_password_link.role = ''
	self.get_data_capture_forms_link.role = ''
	self.upload_data_link.role = ''
	self.progress_projects_link.role = ''
	self.logout_link.role = ''
  pass

  def text_box_4_pressed_enter(self, **event_args):
    """This method is called when the user presses Enter in this text box"""
    pass

  def text_select_an_entity_pressed_enter(self, **event_args):
    """This method is called when the user presses Enter in this text box"""
    pass
  def drop_down_entity_change(self, **event_args):
    """This method is called when an entity item is selected"""
    selected_value = self.drop_down_entity.selected_value
    if selected_value == None:
      self.no_entity_selected_message.text = 'No Estate selected'
      self.selected_entity.text = ''
      Globals.context_Entity_id = ''
      Globals.context_Entity_name = ''
      Globals.context_Entity_number = 0
      self.no_start_date_specified.text = "No start date specified"
      self.date_picker_1.date = None
      self.date_picker_1.enabled = False 
      self.summary_stats_1.text = None
      self.set_menu_off()

    else:
      self.selected_entity.text = 'Estate: ' + selected_value 
      self.no_entity_selected_message.text = ''
      Globals.context_Entity_id = selected_value
      ret = anvil.server.call('get_entity_name_number_from_id',selected_value)
      Globals.context_Entity_name    = ret['name']
      Globals.context_Entity_number  = ret['number']
      summary_stats                  = anvil.server.call('get_summary_stats', Globals.context_Entity_number,Globals.published)
    
      Globals.number_of_buildings    = summary_stats['nbuild']
      Globals.number_of_projects     = summary_stats['nproj']
    
      ef                             = summary_stats['ef']
      if ef > 0:
        alert("***Error occurred obtaining summary stats - please see your support team")
        return
    
      stats1  = f"Number of buildings in estate : {Globals.number_of_buildings}. Number of projects : {Globals.number_of_projects}"
      self.summary_stats_1.text = stats1
      self.date_picker_1.enabled = True
      self.check_entity_start_date_provided()
      self.set_menu_on()
      return    

  def drop_down_client_change(self, **event_args):
    """This method is called when an item is selected"""
    selected_value = self.drop_down_client.selected_value
    self.no_start_date_specified.text = "No start date specified"
    self.date_picker_1.date = None
    self.date_picker_1.enabled = False 
    self.summary_stats_1.text = None
    if selected_value == None:
      self.no_client_selected_message.text = 'No Client selected'
      self.selected_client.text = ''
      Globals.context_Client_id = ''
      Globals.context_Client_name = ''
      Globals.context_Client_number = 0
      #self.reset_client_selections()
      self.reset_entity_selections()
    else:
      self.selected_client.text = 'Client: ' + selected_value 
      self.no_client_selected_message.text = ''
      Globals.context_Client_id = selected_value
      ret = anvil.server.call('get_client_name_number_from_id',selected_value)
      Globals.context_Client_name = ret['name']
      Globals.context_Client_number = ret['number']
      self.drop_down_entity.items = anvil.server.call('get_all_entity_codes',Globals.context_Client_number)
      self.no_entity_selected_message.text = 'No Estate selected'
      self.selected_entity.text = ''
      Globals.context_Entity_id = ''
      Globals.context_Entity_name = ''
      Globals.context_Entity_number = 0      
    return

  def drop_down_partner_change(self, **event_args):
    """This method is called when an item is selected"""
    selected_value = self.drop_down_partner.selected_value
    self.reset_client_selections()
    self.reset_entity_selections()     
    if Globals.logged_in_user_role == 'sysadmin' or Globals.logged_in_user_role == 'partneradmin':
      #self.administration_link.visible = True
     # self.administration_link.enable = True
      self.reports_link.visible = True
      self.reports_link.enable = True
    
    self.no_start_date_specified.text = "No start date specified"
    self.date_picker_1.date = None
    self.date_picker_1.enabled = False
    self.summary_stats_1.text = None
    if selected_value == None:
      self.no_partner_selected_message.text = 'No Partner selected'
      self.selected_partner.text = ''
      Globals.context_Partner_id = ''
      Globals.context_Partner_name = ''
      Globals.context_Partner_number = 0
      #self.administration_link.visible = False
      #self.administration_link.enable = False 
      self.reports_link.visible = False
      self.reports_link.enable = False
    else:
      self.selected_partner.text = 'Partner: ' + selected_value 
      self.no_partner_selected_message.text = ''
      Globals.context_Partner_id = selected_value
      ret = anvil.server.call('get_partner_name_number_from_id',selected_value)
      Globals.context_Partner_name = ret['name']
      Globals.context_Partner_number = ret['number']
      self.drop_down_client.items = anvil.server.call('get_all_client_codes',Globals.context_Partner_number)
    return

  def reset_client_selections(self, **event_args):
    self.drop_down_client.items = []
    self.no_client_selected_message.text = 'No Client selected'
    self.selected_client.text = ''
    Globals.context_Client_id = ''
    Globals.context_Client_name = ''
    Globals.context_Client_number = 0
    return

  def reset_entity_selections(self, **event_args):
    self.drop_down_entity.items = []
    self.no_entity_selected_message.text = 'No Estate selected'
    self.selected_entity.text = ''
    Globals.context_Entity_id = ''
    Globals.context_Entity_name = ''
    Globals.context_Entity_number = 0
    self.set_menu_off()
    return
  
  def set_menu_off(self, **event_args):
    print('*******>>>>>> set_menu_off called')
    self.get_data_capture_forms_link.visible = False
    self.upload_data_link.visible = False
    self.progress_projects_link.visible = False 
    self.view_job_logs_link.visible = False
    return
    
  def set_menu_on(self, **event_args):  
    print('*******>>>>>> set_menu_on called')
    self.get_data_capture_forms_link.visible = True
    self.upload_data_link.visible = True
    self.progress_projects_link.visible = True       
    self.view_job_logs_link.visible = True
    return
  
  def text_box_3_pressed_enter(self, **event_args):
    """This method is called when the user presses Enter in this text box"""
    pass

  def menu_administration_click(self, **event_args):
    self.reset_links()
    #self.administration_link.role = 'selected'
    """This method is called when the link is clicked"""
    from ..Administration import Administration
    new_panel = Administration()
    get_open_form().content_panel.clear()
    get_open_form().content_panel.add_component(new_panel)
    pass	

  def menu_view_job_logs_click(self, **event_args):
    self.reset_links()
    self.view_job_logs_link.role = 'selected'
    """This method is called when the link is clicked"""
    from ..View_upload_logs import View_upload_logs
    new_panel = View_upload_logs()
    get_open_form().content_panel.clear()
    get_open_form().content_panel.add_component(new_panel)
    pass	

  def menu_reports_click(self, **event_args):
    """This method is called when the link is clicked"""
    pass

  def menu_sysadmin_click(self, **event_args):
    """This method is called when the link is clicked"""
    self.reset_links()
    #self.sys_admin_link.role = 'selected'
    """This method is called when the link is clicked"""
    from ..Sys_Admin import Sys_Admin
    new_panel = Sys_Admin()
    get_open_form().content_panel.clear()
    get_open_form().content_panel.add_component(new_panel)
    pass	
  
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
  




























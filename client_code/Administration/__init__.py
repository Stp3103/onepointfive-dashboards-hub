from ._anvil_designer import AdministrationTemplate
from anvil import *
import anvil.server
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables

class Administration(AdministrationTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Any code you write here will run before the form opens.

  def tabs_1_hide(self, **event_args):
    """This method is called when the tabs are hidden"""
    pass

  def tabs_1_tab_click(self, tab_index, tab_title, **event_args):
    """This method is called when a tab is clicked"""
#####User Admin>>>>>>>>>  
    
    if tab_title == 'Add User':
      self.Add_user_panel.visible = True
      self.Amend_user_panel.visible = False
      self.Delete_user_panel.visible = False

      self.Add_client_panel.visible = False
      self.Delete_client_panel.visible = False
      #self.Amend_client_panel.visible = False
      
    if tab_title == 'Amend User':
      self.Add_user_panel.visible = False
      self.Amend_user_panel.visible = True
      self.Delete_user_panel.visible = False
      
      self.Add_client_panel.visible = False
      self.Delete_client_panel.visible = False
      #self.Amend_client_panel.visible = False
      
    if tab_title == 'Delete User':
      self.Add_user_panel.visible = False
      self.Amend_user_panel.visible = False
      self.Delete_user_panel.visible = True 

      self.Add_client_panel.visible = False
      self.Delete_client_panel.visible = False
      #self.Amend_client_panel.visible = False
    
#####Client Admin>>>>>>>>>
    
    if tab_title == 'Add Client':
      self.Add_user_panel.visible = False
      self.Amend_user_panel.visible = False
      self.Delete_user_panel.visible = False

      self.Add_client_panel.visible = True
      self.Delete_client_panel.visible = False
      #self.Amend_client_panel.visible = False 

    if tab_title == 'Delete Client':
      self.Add_user_panel.visible = False
      self.Amend_user_panel.visible = False
      self.Delete_user_panel.visible = False

      self.Add_client_panel.visible = False
      self.Delete_client_panel.visible = True
      #self.Amend_client_panel.visible = False 

    if tab_title == 'Amend Client':
      self.Add_user_panel.visible = False
      self.Amend_user_panel.visible = False
      self.Delete_user_panel.visible = False

      self.Add_client_panel.visible = False
      self.Delete_client_panel.visible = False
      #self.Amend_client_panel.visible = True    
    pass

  def save_button_click(self, **event_args):
    """This method is called when the button is clicked"""
    pass

  def cancel_button_click(self, **event_args):
    """This method is called when the button is clicked"""
    pass

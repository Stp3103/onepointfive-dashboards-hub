from ._anvil_designer import Gas_elec_savings_formTemplate
from anvil import *
import anvil.server
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
from .. import Globals
from datetime import datetime

class Gas_elec_savings_form(Gas_elec_savings_formTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Any code you write here will run when the form opens.
    
    summes = f"Number of buildings with savings check fails - Gas: {Globals.number_of_gas_savings_check_fails},  Electricity: {Globals.number_of_elec_savings_check_fails}"
    self.summary.text = summes

    upf = anvil.server.call('get_buildings_with_gaselec_savings_fail', Globals.selected_entity_number)
    
    ef  = upf['ef']
    if ef == 2:
      em = upf['em']
      alert(title = '****ERROR',content = em)
      return
    Globals.gas_buildings_savings_fail  = upf['gsub']
    Globals.elec_buildings_savings_fail = upf['esub']
    print('In gas elec savings form====')
    print(f"upf : {upf}")
    print(f"gsub : {Globals.gas_buildings_savings_fail}")
    print(f"esub : {Globals.elec_buildings_savings_fail}")
    if Globals.number_of_gas_savings_check_fails == 0 and Globals.number_of_elec_savings_check_fails == 0:
      dditems = ['No savings check fails found']
    if Globals.number_of_gas_savings_check_fails > 0 and Globals.number_of_elec_savings_check_fails == 0:
      dditems = ['Gas saving']
      
    if Globals.number_of_gas_savings_check_fails == 0 and Globals.number_of_elec_savings_check_fails > 0:
      dditems = ['Electricity saving']
      
    if Globals.number_of_gas_savings_check_fails > 0 and Globals.number_of_elec_savings_check_fails > 0:
      dditems = ['Select type of check: -','Gas saving','Electricity saving']
    self.drop_down_1.items = dditems
    pass

    

  def button_1_click(self, **event_args):
    """This method is called when the button is clicked"""
    pass
  
   
  def button_1_click(self, **event_args):
    """This method is called when the button is clicked"""
    pass

  def select_type_of_check(self, **event_args):
    """This method is called when an item is selected"""
    selected_value = self.drop_down_1.selected_value

    # Clear the project adjust savings table and it's totals and the building select table
    
    if selected_value == 'Gas saving':
      log = f"Buildings that have failed savings checks for Gas: \n"
      Globals.selected_savings = 'Gas'
      for b in Globals.gas_buildings_savings_fail:
        log   = log + f"UPRN : {b['uprn']}, Building name : {b['building_name']} \n"
    elif selected_value == 'Electricity saving':
      log = f"Buildings that have failed savings checks for Electricity: \n"      
      Globals.selected_savings = 'Elec'
      for b in Globals.elec_buildings_savings_fail:
        log   = log + f"UPRN : {b['uprn']}, Building name : {b['building_name']} \n"
        
    else:
      log = ''
      Globals.selected_savings = ''
    self.buildings_list.text = log
    pass

  def info_gas_elec_savings(self, **event_args):
    """This method is called when the info button is clicked"""
    ttext = f"Resolve situations where the total gas or electricity savings exceed 100% when summed across all projects for a building.\n \
Select whether gas or electricity savings are to be adjusted using the dropdown.\n The building selection table will be populated with the UPRNs and building names of all buildings failing the selected checks.\n \
Select the building you wish to work on and the project savings table will be populated with the projects for that building and \
the current % savings that have been previously entered for each project ('Current %' column).\n \
Enter new values in the 'New %' column. A running total of 'New %' values is shown. \n When complete save the new values by clicking the 'Save' button. If the total exceeds 100% an alert will be produced and new values will not be saved"
    alert(ttext, title="How to use this form",large=True,)
    

    
    
    



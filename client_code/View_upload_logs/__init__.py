from ._anvil_designer import View_upload_logsTemplate
from anvil import *
import anvil.server
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables

class View_upload_logs(View_upload_logsTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Any code you write here will run when the form opens.

  def tabs_1_tab_click(self, tab_index, tab_title, **event_args):
    """This method is called when a tab is clicked"""
    print('In tab click event. Title is: -')
    print(tab_title)
    print(f"tab indexe passed in = {tab_index}")
    pass

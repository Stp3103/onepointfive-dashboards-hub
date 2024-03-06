import anvil.files
from anvil.files import data_files
import anvil.secrets
import anvil.email
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server

ptlist = [] # This will be populated with a list of dictionaries defining all project type names and project type ids e.g. {'name': 'Heating Controls', 'project_type_id': 15} etc.

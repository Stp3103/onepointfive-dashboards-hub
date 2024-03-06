import anvil.server
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
# This is a module.
# You can define variables and functions here, and use them from any form. For example, in a top-level form:
#
#    from . import Utils

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
  

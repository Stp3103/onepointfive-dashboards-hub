import anvil.server
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
# This is a module.
# You can define variables and functions here, and use them from any form. 

from anvil import open_form
import custom_signup.login_flow
print('In module1 login')

custom_signup.login_flow.do_email_confirm_or_reset()
# Open Login_form
open_form('Login_form')


import anvil.files
from anvil.files import data_files
import anvil.secrets
import anvil.email
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server

# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.
#
# To allow anvil.server.call() to call functions here, we mark
# them with @anvil.server.callable.

#This is the name of the database table to be used for cibse benchmarks (including building_type)
benchmark_table_name = 'cibse_benchmarks_2021'
import anvil.files
from anvil.files import data_files
import anvil.secrets
import anvil.email
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server

import anvil.http
from anvil import app
import pandas as pd
import decimal
import time
import numpy as np
import io 
from rich import box
from rich.console import Console
from rich.table import Table
import Project_types as pt

import anvil.media
import Connections
import pyodbc
import sqlalchemy as salch
import urllib.parse
import urllib3
import json
import kv_calcs as kc
from datetime import datetime as dt

import sys, traceback


# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.
#
# To allow anvil.server.call() to call functions here, we mark
# them with @anvil.server.callable.
# Here is an example - you can replace it with your own:
#
# @anvil.server.callable
# def say_hello(name):
#   print("Hello, " + name + "!")
#   return 42
#
key = 'rfYyiAq706m_3lDlFiw_9xK1WUh8Zgc_h-y42_FHg5w='
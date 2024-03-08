# Copyright(C) Val-Cloud Ltd 2023. All rights reserved
from ._anvil_designer import MACCTemplate
from anvil import *
import plotly.graph_objects as go

import anvil.server
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
from datetime import datetime
from .. import Globals
#import numpy as np
#from sklearn.metrics import roc_curve
#import matplotlib.pyplot as plt

class MACC(MACCTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)
    # Check an entity has been selected otherwise disable file download button
    #alert("IN MACC init")

  pass

  def create_example_MACC(self, **properties):
#    ret = anvil.server.call('call_get_MACC_data',1005)
#    ef  = ret['ef']
#    em  = ret['em']
#    x   = ret['x']
#    y   = ret['y']
#    pt  = ret['pt']

#    if ef > 0:
#      alert(f"**Error getting MACC data - {em}")
#      return
 #   else:
      alert("Printing x,y to app log")
     # print(x)
     # print(y)
      self.image_1.source = anvil.server.call('make_MACC_plot')
      pass
      
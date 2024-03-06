# Copyright(C) Val-Cloud Ltd 2023. All rights reserved
from ._anvil_designer import MACCTemplate
from anvil import *
import anvil.server
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
from datetime import datetime
from .. import Globals
import numpy as np
from sklearn.metrics import roc_curve
import matplotlib.pyplot as plt

class MACC(MACCTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)
    # Check an entity has been selected otherwise disable file download button
    #alert("IN MACC init")

  pass

  def create_example_MACC(self, **properties):
  
    # Define cost parameters
    cost_fn = 1.0  # C(-|+), cost of misclassifying negative samples as positive
    cost_fp = 1.0  # C(+|-), cost of misclassifying positive samples as negative
    
    # Ground truth labels (0 for negative class, 1 for positive class)
    truth = [0, 1, 0, 1, 0, 1]
    
    # Predictions from a classifier (class probabilities)
    score = [0.2, 0.8, 0.4, 0.6, 0.3, 0.7]
    
    # Compute ROC curve
    roc_fpr, roc_tpr, _ = roc_curve(truth, score)
    
    # Compute normalized p(+)*C(-|+) thresholds
    thresholds = np.arange(0, 1.01, 0.01)
    pc = (thresholds * cost_fn) / (thresholds * cost_fn + (1 - thresholds) * cost_fp)
    
    # Compute lines in the cost space for each point in the ROC space
    lines = []
    for fpr, tpr in zip(roc_fpr, roc_tpr):
        slope = (1 - tpr - fpr)
        intercept = fpr * cost_fn
        lines.append((slope, intercept))
    
    # Compute the lower envelope of the lines
    lower_envelope = np.minimum.reduce([slope * pc + intercept for slope, intercept in lines])
    
    # Calculate the area under the lower envelope (the smaller, the better)
    area = np.trapz(lower_envelope, pc)
    
    # Plot the MACC
    plt.figure(figsize=(8, 6))
    plt.plot(pc, lower_envelope, label="MACC")
    plt.xlabel("Normalized Expected Cost (PC)")
    plt.ylabel("Normalized Expected Cost (NEC)")
    plt.title("Marginal Abatement Cost Curve")
    plt.legend()
    plt.grid(True)
    plt.show()
    return

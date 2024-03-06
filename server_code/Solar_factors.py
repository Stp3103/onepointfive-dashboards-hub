import anvil.files
from anvil.files import data_files
# Copyright(C) Val-Cloud Ltd 2023. All rights reserved
import anvil.secrets
import anvil.email
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
import decimal

# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.
# This module holds solar factors used in calculations for Solar PV and Solar Thermmal projects.
PitchCF            = decimal.Decimal(6.5)
FlatCF             = decimal.Decimal(12)
RoofCF             = decimal.Decimal(0.8)
Generation_Factor  = decimal.Decimal(800)
Solar_thermal_kwh_per_panel = decimal.Decimal(450)
Roof_GIA_factor    = decimal.Decimal(1/3)
Kwpeak_factor      = decimal.Decimal(8.5)

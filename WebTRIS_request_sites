"""
Created on Mon Apr 11 18:08:53 2022

@author: UKECF002

This file connects to the WebTRIS API and extracts the metadata for all the sites in their database
"""

import pandas as pd

from pytris.models import Site
from pytris import API
    
api = API()
sites = api.sites()
site_list = pd.DataFrame(columns=["id","name","description","longitude","latitude","status"])

for site in sites.all():
    # Do something with each site
    site_list = site_list.append({"id":site.id,"name":site.name,"description":site.description,"longitude":site.longitude,"latitude":site.latitude,"status":site.status},ignore_index=True)
export_csv=site_list.to_csv("site_list.csv")

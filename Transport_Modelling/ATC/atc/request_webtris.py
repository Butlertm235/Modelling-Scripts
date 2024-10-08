"""
Created September 2024, compiling previous scripts

@author: UKSAC775
"""

import pandas as pd
from pytris.models import Site
from pytris import API

def request_sites(export_dir=""):
    """
    Connects to the WebTRIS API and extracts the metadata for all the sites in their database

    Returns
    -------
    site_list: dataframe       
           Dataframe with all Webtris sites
    """

    api = API()
    sites = api.sites()
    site_list = pd.DataFrame(columns=["id","name","description","longitude","latitude","status"])

    for site in sites.all():
        # Do something with each site
        site_list = site_list._append({"id":site.id,"name":site.name,"description":site.description,"longitude":site.longitude,"latitude":site.latitude,"status":site.status},ignore_index=True)
    export_csv=site_list.to_csv(export_dir+"site_list.csv")

    return site_list

def request_daily_report(WebTRIS_sites,date_start,date_end,export_dir=""):
    """
    Downloads the daily reports (in 15-min intervals) for the selected WebTRIS sites and dates
    This request can take 7+ hours for a whole county

    Parameters
    ----------
    WebTRIS_sites : Array
        List of WebTRIS sites to download
    date_start : string
        Start date in "DDMMYYYY" format
    end_start : string
        End date in "DDMMYYYY" format
    export_dir : string
    
    Returns
    -------
    Indiviudal csv files per site of the request daily data

    """

    api = API()
    daily = api.daily_reports()

    appended = pd.DataFrame()

    for i in WebTRIS_sites:
        try:
            result = daily.get(sites=str(i), start_date=date_start, end_date=date_end)
            df = result.to_frame()
            df["site"] = i
            df["Report Date"] = pd.to_datetime(df["Report Date"])
            export_csv = df.to_csv(export_dir+"WebTRIS_site_"+str(i)+".csv")
        except:
            pass
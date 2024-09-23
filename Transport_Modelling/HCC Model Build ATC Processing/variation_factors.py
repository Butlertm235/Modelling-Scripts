import pandas as pd
import File_directories
import os
import util


def average_flows_by_site_month_and_year(data: pd.DataFrame):

    """
    Takes a dataframe and returns average flows by site, month and year, also by direction
    
    Check column names match up with those used in groupby before running
    """

    data["month"] = pd.to_datetime(data['date'], dayfirst = True).dt.month
    data["site - Source - direction"] = data["site - Source"] + " - " + data["direction"]
    
    average_flows_by_month = data.groupby(["site", "Source", "site - Source","site - Source - direction", "direction", "Road type","year", "month", "time"]).mean()
    average_flows_by_month = average_flows_by_month.reset_index()
    return average_flows_by_month

def average_two_way_flows_by_site_month_and_year(data: pd.DataFrame):
    
    """
    Takes dataframe and returns average flows by site, month and year
    Calculates two way 
    """

    average_flows_by_month = average_flows_by_site_month_and_year(data)
    average_two_way_flows_by_month = average_flows_by_month.groupby(["site", "Source", "site - Source", "year", "month", "time"]).sum()
    average_two_way_flows_by_month = average_two_way_flows_by_month.reset_index()
    return average_two_way_flows_by_month

def filter_pd_df_by_sample_size_for_one_month(data: pd.DataFrame, sample_size: float, month: int):
    
    """
    Filters a dataframe for sample size in number of days for one specific month, to be used for month variation factor calculations
    """

    data["date"] = pd.to_datetime(data["date"], dayfirst = True)

    data["month"] = data["date"].dt.month

    data_count = data.groupby(["site", "direction", "Source", "month"]).count()
    
    data_count = data_count.reset_index()

    site_filter = data_count.loc[(data_count["month"] == month) & (data_count["flow_total"] >= sample_size*24)]
    
    filtered_data = pd.merge(data,site_filter[["site", "Source", "direction"]], on = ["site", "Source", "direction"], how = "inner")

    return filtered_data

def filter_pd_df_for_insufficient_data_for_one_month(data: pd.DataFrame, month: int, sample_size: float):

    """
    Filter a dataframe for insifficient amount of data for a specific month

    To be used when identifying the sites for which there isn't a sufficient amount of March 2023 data without conversion
    """

    data["date"] = pd.to_datetime(data["date"], dayfirst = True)

    data["month"] = data["date"].dt.month

    data_count = data.groupby(["site", "direction", "Source", "month"]).count().unstack(fill_value=0).stack()
    
    data_count = data_count.reset_index()
    
    site_filter = data_count.loc[(data_count["month"] == month) & (data_count["flow_total"] < sample_size*24)]
    
    filtered_data = pd.merge(data,site_filter[["site", "Source", "direction"]], on = ["site", "Source", "direction"], how = "inner")

    return filtered_data


def calculate_monthly_variation_factor(input_db: pd.DataFrame, origin_month: int, target_month: int, time_intervals: dict, sample_size: float, year: str, flow_profile: bool):
    """
    Calculates monthly variation for one month to target month for a particular year. 
    This is done by first filtering data to only include sites with the specified sample size in both the origin and target months.
    
    Note that only continuous sites should be used. Test tomorrow

    Flow profile defines whether or not the function will convert for a 24h period or just AM IP and PM flows
    """

    input_db = util.attach_roadtypes(input_db)
    input_db_filtered_by_origin_month_sample = filter_pd_df_by_sample_size_for_one_month(input_db, sample_size, origin_month)

    input_db_filtered_by_sample_size = filter_pd_df_by_sample_size_for_one_month(input_db_filtered_by_origin_month_sample, sample_size, target_month)
    
    average_two_way_flows_by_site_month = average_two_way_flows_by_site_month_and_year(input_db_filtered_by_sample_size)

    
    average_flows_by_month = average_two_way_flows_by_site_month.groupby(["year","month", "time"]).mean()
    average_flows_by_month = average_flows_by_month.reset_index()

    average_flows_by_month.to_csv("avg_flow_by_month.csv", index = False)
    if flow_profile == True:
        target_month_avg_flows = average_flows_by_month.loc[average_flows_by_month["month"] == target_month]
        target_month_avg_flows = target_month_avg_flows.rename(columns = {"month" : "target_month"})
        target_month_avg_flows = target_month_avg_flows.reset_index()
        target_month_avg_flows = target_month_avg_flows.rename(columns = {"flow_total": "target_month_flow_total"})

        origin_month_avg_flows = average_flows_by_month.loc[average_flows_by_month["month"] == origin_month]
        origin_month_avg_flows = origin_month_avg_flows.rename(columns = {"month" : "origin_month"})
        origin_month_avg_flows = origin_month_avg_flows.reset_index()
        
        average_flow_monthly_comparison_db = pd.merge(origin_month_avg_flows,target_month_avg_flows[["target_month", "time", "target_month_flow_total"]], how = "left", on = [ "time"])
        average_flow_monthly_comparison_db["target_month_uplift_factor"] = average_flow_monthly_comparison_db["target_month_flow_total"] / average_flow_monthly_comparison_db["flow_total"]
        average_flow_monthly_comparison_db["year"] = year

        return average_flow_monthly_comparison_db[["year","origin_month", "target_month", "time", "target_month_uplift_factor"]]

    else:
        average_flows_by_month_AM_IP_PM = util.calc_AM_IP_PM_Flows_by_month(average_flows_by_month, time_intervals)

        average_flows_by_month_AM_IP_PM.to_csv("TP_flows_test.csv", index = False)


        target_month_avg_flows = average_flows_by_month_AM_IP_PM.loc[average_flows_by_month_AM_IP_PM["month"] == target_month]
        target_month_avg_flows = target_month_avg_flows.rename(columns = {"month" : "target_month"})
        target_month_avg_flows = target_month_avg_flows.reset_index()
        target_month_avg_flows = target_month_avg_flows.rename(columns = {"flow_total": "target_month_flow_total"})

        origin_month_avg_flows = average_flows_by_month_AM_IP_PM.loc[average_flows_by_month_AM_IP_PM["month"] == origin_month]
        origin_month_avg_flows = origin_month_avg_flows.rename(columns = {"month" : "origin_month"})
        origin_month_avg_flows = origin_month_avg_flows.reset_index()
        
        average_flow_monthly_comparison_db = pd.merge(origin_month_avg_flows,target_month_avg_flows[["target_month", "time_period", "target_month_flow_total"]], how = "left", on = [ "time_period"])
        average_flow_monthly_comparison_db["target_month_uplift_factor"] = average_flow_monthly_comparison_db["target_month_flow_total"] / average_flow_monthly_comparison_db["flow_total"]
        average_flow_monthly_comparison_db["year"] = year

        return average_flow_monthly_comparison_db[["year","origin_month", "target_month", "time_period", "target_month_uplift_factor"]]

def calc_all_monthly_variation_factors(input_dir: str, target_month: int, time_intervals: dict, sample_size: float, flow_profile: bool):
    
    """
    Calculates all monthly variation factors for a particular target month (March for the COMET model build) based on sites which have sufficient data for both the origin month and the target
    """
    
    os.chdir(input_dir)
    monthly_variation_factors = pd.DataFrame(columns = ["year","origin_month", "target_month" "time_period", "target_month_uplift_factor"])
    for year in ["2019", "2022", "2023"]: #
        
        #Read in dataframe for continuous sites with an hourly time interval
        data = pd.read_csv(f"ATC_{year}_continuous_combined_filtered_outliers_database.csv")
        data["month"] = pd.to_datetime(data["date"], dayfirst = True).dt.month
        data["site - Source"] = data["site"].astype(str) + " - " + data["Source"].astype(str)
        
        #Calculate variation factors for each month in turn
        for month in data["month"].unique():

            monthly_variation_db = calculate_monthly_variation_factor(data, month, target_month, time_intervals, 5.5, year, flow_profile)

            monthly_variation_factors = pd.concat([monthly_variation_factors, monthly_variation_db])
    
    #Output variation factors with correct file names
    if flow_profile == True:
        monthly_variation_factors.to_csv(f"{File_directories.MONTHLY_VARIATION_FACTORS_24H}", index = False)
    else:
        monthly_variation_factors.to_csv(f"{File_directories.MONTHLY_VARIATION_FACTORS}", index = False)

    return monthly_variation_factors




def apply_monthly_variation_factors(input_dir: str, output_dir: str, monthly_variation_lookup: str, target_month: int, sample_size: float, time_periods: dict, flow_profile: bool):
    """
    Applies monthly variation factors to an input directory, without averaging the results by site.
    """
    os.chdir(input_dir)

    monthly_variation = pd.read_csv(monthly_variation_lookup)
    monthly_variation["year"] = monthly_variation["year"].astype(str)
    
    #Create datetime time column for 24h dataframes
    if flow_profile == True:

        monthly_variation["time"] = pd.to_datetime(monthly_variation["time"], dayfirst= True).dt.time


    #Read in the data for each year, before filtering for sites requiring conversion
    for year in ["2019", "2022", "2023"]: #, 

        data = pd.read_csv(f"{input_dir}\\{year}\\ATC_{year}_Combined_filtered_outliers_database.csv")

        data["site - Source"] = data["site"].astype(str) + " - " + data["Source"].astype(str)
        data["site - Source - direction"] = data["site"].astype(str) + " - " + data["Source"].astype(str) + " - " + data["direction"]
        data["date"] = pd.to_datetime(data["date"], dayfirst = True)
        data["time"] = data["date"].dt.time


        ##### Includes sites with target month data below required sample size
        data_requiring_conversion = filter_pd_df_for_insufficient_data_for_one_month(data, target_month, sample_size) ###CHECK FUNCTION WORKS WITH SITE _ SOURCE _ DIRECTION
        
        data_requiring_conversion = data_requiring_conversion.loc[data_requiring_conversion["Source"] != "Severnside ATC"]

        sites_requiring_conversion = data_requiring_conversion["site - Source - direction"].unique()
        
        #Save sites for sufficient target month data for appending later
        good_target_month_data = data.loc[((data["month"] == target_month) & (~data["site - Source - direction"].isin(sites_requiring_conversion))| (data["Source"] == "Severnside ATC"))]


        months_to_convert = data_requiring_conversion["month"].unique()
        months_to_convert = months_to_convert.astype(int)

        #Loop through months required to be converted
        converted_data = pd.DataFrame()
        for month in months_to_convert:
            
            #Filter data for specific month
            data_to_convert = data_requiring_conversion.loc[data_requiring_conversion["month"] == month]
            
            #Skip year-month pairs with no conversion factor
            if len(monthly_variation.loc[(monthly_variation["year"] == year) & (monthly_variation["origin_month"] == month)]) == 0 :
                continue
            

            if flow_profile == True:
                #Final conversion factor
                conversion_factors = monthly_variation.loc[(monthly_variation["year"] == year) & (monthly_variation["origin_month"] == month)]
                conversion_factors = conversion_factors[["year", "origin_month", "target_month_uplift_factor", "time"]]

                #Add factor to data to convert, and multiply flow by the factor
                data_to_convert["year"] = data_to_convert["year"].astype(str)
                data_to_convert = pd.merge(data_to_convert, conversion_factors, left_on = ["year", "month", "time"], right_on = ["year", "origin_month", "time"], how = "inner")
                data_to_convert["flow_total"] = data_to_convert["flow_total"] * data_to_convert["target_month_uplift_factor"]
                

                converted_data = pd.concat([converted_data, data_to_convert])

            else:
                #Apply factors by time period if flow_profile is set to false
                AM_conversion_factor = monthly_variation.loc[(monthly_variation["year"] == year) & (monthly_variation["origin_month"] == month) & (monthly_variation["time_period"] == "AM")].iloc[0,3]
                
                IP_conversion_factor = monthly_variation.loc[(monthly_variation["year"] == year) & (monthly_variation["origin_month"] == month) & (monthly_variation["time_period"] == "IP")].iloc[0,3]
                
                PM_conversion_factor = monthly_variation.loc[(monthly_variation["year"] == year) & (monthly_variation["origin_month"] == month) & (monthly_variation["time_period"] == "PM")].iloc[0,3]
                
                AM_factored = data_to_convert.loc[data_to_convert["time"].astype(str) == time_periods["AM"]]
                AM_factored[["flow_car", "flow_LGV", "flow_OGV_1", "flow_OGV_2", "flow_HGV", "flow_BUS", "flow_total"]] = AM_factored[["flow_car", "flow_LGV", "flow_OGV_1", "flow_OGV_2", "flow_HGV", "flow_BUS", "flow_total"]].apply(lambda x: x * AM_conversion_factor)
                
                IP_factored = data_to_convert.loc[data_to_convert["time"].astype(str).isin(time_periods["IP"])] # .apply(lambda x: x * IP_conversion_factor)
                IP_factored[["flow_car", "flow_LGV", "flow_OGV_1", "flow_OGV_2", "flow_HGV", "flow_BUS", "flow_total"]] = IP_factored[["flow_car", "flow_LGV", "flow_OGV_1", "flow_OGV_2", "flow_HGV", "flow_BUS", "flow_total"]].apply(lambda x: x * IP_conversion_factor)
                
                PM_factored = data_to_convert.loc[data_to_convert["time"].astype(str) == time_periods["PM"]] #.apply(lambda x: x * PM_conversion_factor) #
                PM_factored[["flow_car", "flow_LGV", "flow_OGV_1", "flow_OGV_2", "flow_HGV", "flow_BUS", "flow_total"]] = PM_factored[["flow_car", "flow_LGV", "flow_OGV_1", "flow_OGV_2", "flow_HGV", "flow_BUS", "flow_total"]].apply(lambda x: x * PM_conversion_factor)


                converted_data = pd.concat([converted_data, AM_factored, IP_factored, PM_factored])
        
        
        full_converted_db = pd.concat([good_target_month_data, converted_data])
        
        if flow_profile == True:
            full_converted_db = full_converted_db[["site", "Source", "site - Source", "site - Source - direction", "direction", "year","date", "time", "flow_total"]]
            full_converted_db.to_csv(f"{output_dir}\\Full_Flow_Profiles_March_Factored_{year}_no_avg.csv", index = False)
        
        else:

            AM_IP_PM_converted_flows = util.calc_AM_IP_PM_Flows_Hourly_by_site(full_converted_db, time_periods)
            AM_IP_PM_converted_flows.to_csv(f"{output_dir}\\AM_IP_PM_Flows_March_Factored_{year}_no_avg.csv", index = False)


def calculate_yearly_variation_factors(origin_db: pd.DataFrame, target_db: pd.DataFrame, output_file: str, continuous_site_filter: str, time_intervals: dict, flow_profile: bool):
    """
    Calculates yearly variation factors for AM IP and PM flows using a base year(s) and target year database.

    Origin and target databases should be averaged by site and converted to march data using monthly variation factors
    """
    continuous_sites = pd.read_csv(continuous_site_filter)
    continuous_sites["site - Source"] = continuous_sites["site"].astype(str) + " - " + continuous_sites["Source"]
    
    #Attach road types to the list of continuous sites and filter and filter for A roads included in HCC and Severnside data
    continuous_sites_RT = util.attach_roadtypes(continuous_sites)
    continuous_sites_RT = continuous_sites_RT.loc[(continuous_sites_RT["Source"] == "HCC ATC") | ((continuous_sites_RT["Source"] == "Severnside ATC") & (continuous_sites_RT["Road type"] == "A"))]
    continuous_sites_RT = continuous_sites_RT.drop(columns = ["site - Source"])
    
    #Filter target db for those used for conversion factoes (continuous A roads)
    target_db = pd.merge(target_db, continuous_sites_RT, on = ["site", "Source"], how = "inner")

    ######CHANGED 231222 - average by site, source direction time before calculating factors
    target_db = target_db.groupby(["site","Source", "site - Source", "direction", "year", "time"]).mean()
    target_db = target_db.reset_index()
    #Calculate average flows by time period for target year
    if flow_profile == False:
        target_db = util.calc_AM_IP_PM_Flows_Hourly_by_site(target_db, time_intervals)
        average_flows_target_year = target_db.groupby(["year", "time_period"]).mean()
    else:
        
        average_flows_target_year = target_db.groupby(["year", "time"]).mean()
    
    average_flows_target_year = average_flows_target_year.reset_index()
    
    #Attach filter for continuous A roads on dataframe to be converted
    origin_db = pd.merge(origin_db, continuous_sites_RT, on = ["site", "Source"], how = "inner")

    ###CHANGED 231222
    origin_db = origin_db.groupby(["site","Source", "site - Source", "direction", "year", "time"]).mean()
    origin_db = origin_db.reset_index()
    #Calculate average flows by time period for origin year
    if flow_profile == False:
        origin_db = util.calc_AM_IP_PM_Flows_Hourly_by_site(origin_db, time_intervals)
        average_flows_origin_year = origin_db.groupby(["year", "time_period"]).mean()
    else:
        average_flows_origin_year = origin_db.groupby(["year", "time"]).mean()
    
    average_flows_origin_year = average_flows_origin_year.reset_index()

    #Combine origin and target year dbs using merge, specifying flows in the columns
    average_flows_target_year = average_flows_target_year.rename(columns = {"flow_total": "target_flow_total", "year": "target_year"})

    if flow_profile == False:
        flow_comparison_db = pd.merge(average_flows_origin_year, average_flows_target_year, on = ["time_period"], how = "inner")
        flow_comparison_db["yearly_variation_factor"] = flow_comparison_db["target_flow_total"] / flow_comparison_db["flow_total"]
        flow_comparison_db = flow_comparison_db[["year","time_period", "target_year", "flow_total", "target_flow_total", "yearly_variation_factor"]]

    else:
        flow_comparison_db = pd.merge(average_flows_origin_year, average_flows_target_year, on = ["time"], how = "inner")
        flow_comparison_db["yearly_variation_factor"] = flow_comparison_db["target_flow_total"] / flow_comparison_db["flow_total"]
        flow_comparison_db = flow_comparison_db[["year","time", "target_year", "flow_total", "target_flow_total", "yearly_variation_factor"]]
    
    
    
    flow_comparison_db.to_csv(output_file, index = False)

    return flow_comparison_db
    
def apply_yearly_variation_factors(origin_db: pd.DataFrame, target_db: pd.DataFrame, factors_file: str, output_dir: str, continuous_site_filter: str, time_intervals: dict, flow_profile: bool, sample_size: float):
    """
    Converts data from origin year to target year when there is insufficient tata in the target year
    """

    #Calculate factors
    comparison_db = calculate_yearly_variation_factors(origin_db, target_db, factors_file, continuous_site_filter, time_intervals, flow_profile)
    
    
    
    if flow_profile == False:
        origin_db = util.calc_AM_IP_PM_Flows_Hourly_by_site_and_date(origin_db, time_intervals)
        target_db = util.calc_AM_IP_PM_Flows_Hourly_by_site_and_date(target_db, time_intervals)


    origin_db["site - Source - direction"] = origin_db["site"].astype(str) + " - " + origin_db["Source"] + " - " + origin_db["direction"]
    target_db["site - Source - direction"] = target_db["site"].astype(str) + " - " + target_db["Source"] + " - " + target_db["direction"]
    
    
    
    #Loop through origin years
    for year in ["2022", "2019"]:
        if flow_profile == True:

            target_db_data_count = target_db.groupby(["site", "direction", "Source"]).count()
            target_db_data_count.reset_index(inplace = True)
            
            site_filter = target_db_data_count.loc[target_db_data_count["flow_total"] >= sample_size*24]
            
            target_db_with_enough_data = pd.merge(target_db, site_filter[["site", "Source", "direction"]], on = ["site", "Source", "direction"], how = "inner")

        #Filter for data not included in target dataframe
        year_to_convert_db = origin_db.loc[(origin_db["year"].astype(str) == year) & (~origin_db["site - Source - direction"].isin(target_db_with_enough_data["site - Source - direction"]))]

        if flow_profile == False:
            #Get AM IP and PM conversion factors
            AM_conversion_factor = comparison_db.loc[(comparison_db["year"].astype(str) == year) & (comparison_db["time_period"] == "AM")].iloc[0,5]
            IP_conversion_factor = comparison_db.loc[(comparison_db["year"].astype(str) == year) & (comparison_db["time_period"] == "IP")].iloc[0,5]
            PM_conversion_factor = comparison_db.loc[(comparison_db["year"].astype(str) == year) & (comparison_db["time_period"] == "PM")].iloc[0,5]

            #Factor origin dataframes and append to target datafame
            AM_factored = year_to_convert_db.loc[year_to_convert_db["time_period"] == "AM"]
            AM_factored["flow_total"] = AM_factored["flow_total"].apply(lambda x: x * AM_conversion_factor)
            
            IP_factored = year_to_convert_db.loc[year_to_convert_db["time_period"] == "IP"]
            IP_factored["flow_total"] = IP_factored["flow_total"].apply(lambda x: x * IP_conversion_factor)
            
            PM_factored = year_to_convert_db.loc[year_to_convert_db["time_period"] == "PM"]
            PM_factored["flow_total"] = PM_factored["flow_total"].apply(lambda x: x * PM_conversion_factor)

            target_db = pd.concat([target_db, AM_factored, IP_factored, PM_factored])

        else:
            def apply_24h_factors(row, factors):
                
                row["flow_total"] = row["flow_total"] * factors.loc[(factors["year"].astype(str) == year) & (factors["time"] == row["time"])].iloc[0,5]
                return row
            
            converted_year_data = year_to_convert_db.apply(lambda x: apply_24h_factors(x, comparison_db), axis = 1)

            target_db = pd.concat([target_db, converted_year_data])
    
    target_db_data_count = target_db.groupby(["site", "direction", "Source"]).count()
    target_db_data_count.reset_index(inplace = True)
    
    site_filter = target_db_data_count.loc[target_db_data_count["flow_total"] >= sample_size*24]
    
    target_db = pd.merge(target_db, site_filter[["site", "Source", "direction"]], on = ["site", "Source", "direction"], how = "inner")

    if flow_profile == False:
        #reformat and output
        target_db_AM_IP_PM = target_db_AM_IP_PM[["site", "Source", "site - Source", "direction", "year", "time_period", "flow_total", "Date"]]
        
        target_db_AM_IP_PM["No_of_Days"] = target_db_AM_IP_PM.groupby(["site", "Source", "site - Source", "direction"])["flow_total"].count()
        
        rsd = lambda x: x.std() / x.mean()
        target_db_AM_IP_PM["RSD"] = target_db_AM_IP_PM.groupby(["site", "Source", "site - Source", "direction", "time"])["flow_total"].transform(rsd)

        target_db_AM_IP_PM = target_db_AM_IP_PM.groupby(["site", "Source", "site - Source", "direction", "time"]).mean()
        target_db_AM_IP_PM = target_db_AM_IP_PM.reset_index()

        target_db_AM_IP_PM.to_csv(f"{output_dir}\\Fully_factored_march_2023_AM_IP_PM_flows.csv", index = False)

    else:
        target_db = target_db[["site", "Source", "site - Source", "direction", "time", "flow_total"]]

        no_days = lambda x: x.count()
        target_db["No_of_Days"] = target_db.groupby(["site", "Source", "site - Source", "direction", "time"])["flow_total"].transform(no_days)
        
        rsd = lambda x: x.std() / x.mean()
        target_db["RSD"] = target_db.groupby(["site", "Source", "site - Source", "direction", "time"])["flow_total"].transform(rsd)

        target_db = target_db.groupby(["site", "Source", "site - Source", "direction", "time"]).mean()
        target_db = target_db.reset_index()

        target_db = util.join_final_site_id(target_db, File_directories.FINAL_SITE_ID_LOOKUP)

        target_db.to_csv(f"{output_dir}\\Fully_factored_march_2023_flows.csv", index = False)
        return target_db
    


if __name__ == "__main__":
    calc_all_monthly_variation_factors(File_directories.CONTINUOUS_COMBINED_FILTERED, 3, File_directories.TIME_PERIODS_HOURLY, 5.5, True)

    apply_monthly_variation_factors(File_directories.ATC_COMBINED_OUTLIER_FILTERED,File_directories.DATA_FACTORED_TO_MARCH, File_directories.MONTHLY_VARIATION_FACTORS_24H, 3, 5.5, File_directories.TIME_PERIODS_HOURLY, True)

    march_factored_2019 = pd.read_csv(f"{File_directories.DATA_FACTORED_TO_MARCH}\\Full_Flow_Profiles_March_Factored_2019_no_avg.csv")

    march_factored_2022 = pd.read_csv(f"{File_directories.DATA_FACTORED_TO_MARCH}\\Full_Flow_Profiles_March_Factored_2022_no_avg.csv")

    march_factored_2023 = pd.read_csv(f"{File_directories.DATA_FACTORED_TO_MARCH}\\Full_Flow_Profiles_March_Factored_2023_no_avg.csv")

    past_years_data = pd.concat([march_factored_2019, march_factored_2022])

    calculate_yearly_variation_factors(past_years_data, march_factored_2023, File_directories.YEARLY_VARIATION_FACTORS, File_directories.CONTINUOUS_SITE_FILTER, File_directories.TIME_PERIODS_HOURLY, True)
    final_data = apply_yearly_variation_factors(past_years_data,march_factored_2023,File_directories.YEARLY_VARIATION_FACTORS, File_directories.FINAL_FACTORED_DATA, File_directories.CONTINUOUS_SITE_FILTER, File_directories.TIME_PERIODS_HOURLY, True, 5.5)

    
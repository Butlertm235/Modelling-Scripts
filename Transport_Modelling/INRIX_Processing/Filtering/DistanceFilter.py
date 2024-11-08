import numpy as np
import pandas as pd
import os
import time
from io import StringIO
import sys
import File_scripts.directories as DR

#combines csvs in a directory, outputs to csv if output==True
def combineCsv(Directory):
    i=0
    for filename in os.listdir(Directory):
        if filename.endswith('.csv'):
            with open(os.path.join(Directory, filename)) as f:
                df=pd.read_csv(os.path.join(Directory, filename))
                        
                if i==0:
                    dataCombined = df
                else:
                    dataCombined=dataCombined._append(df)       
        i+=1
    return dataCombined
    
#extracts RouteLinks from a single dataframe
def extractRouteLinks(df,linkCol,outputDirectory,outputName):
    links = df.iloc[:,df.columns.get_loc(linkCol)]
    routeLinks = links.drop_duplicates()
    routeLinks.to_csv(outputDirectory+'/'+outputName+'.csv',index=False)
    return routeLinks  

#extracts links in df1 but not in df2
def dataframeDifference(df1,df2,outputDirectory,outputName):
    """
    extracts links in df1 but not in df2
    """
    data_merge = df1.to_frame().merge(df2,indicator=True,how='outer')
    data_merge = data_merge.loc[lambda x : x['_merge'] == 'left_only']
    data_merge.drop(columns='_merge',inplace=True)
    data_merge.to_csv(outputDirectory+'/'+outputName+'.csv',index=False)
    return data_merge

def distanceFilter(parameters_path):

    """
    This function filters out records from the INRIX journey time data CSVs where journeys are incomplete.
    An incomplete journey can be defined by any combination of:
        1. An incomplete flag marker
        2. A specified absolute distance between the length of the reported journey and the length of the link
        3. A specified relative (%) distance between the length of the reported journey and the length of the link
    
    These filtering options plus others must be specified in a paramters file called 'parameters.py'
    This should be saved in the path of the function's argument when called.

    The final distance filtered data will be saved in its own 'distanceFilteredData' folder
    The other outputs in the main output directory are useful for analysis

    Parameters
        ----------       
        parameters_path : string
           Filepath to the location of the parameters.py parameters file
    Returns
       -------
       distanceFilteredData: dataframe       
           Dataframe of the distance filtered data
    """

    st=time.time()

    #add directory of parameters file to system path
    sys.path.append(parameters_path)
    import parameters as pm

    #read in parameters from paramters file
    directory = pm.directory
    outputDirectory = pm.outputDirectory
    gisAttributesFile = pm.gisAttributesFile
    
    gpsLinkId = pm.gpsLinkId
    gpsDist = pm.gpsDist
    gisLinkId = pm.gisLinkId
    gisDist = pm.gisDist
    incompleteFlag = pm.incompleteFlag
    selectedColumnsUnfiltered = pm.selectedColumnsUnfiltered
    selectedColumnsFiltered = pm.selectedColumnsFiltered
    TMJ_columns = pm.TMJ_columns
    incompleteJourneyFilter = pm.incompleteJourneyFilter
    distFilterAbs = pm.distFilterAbs
    distFilterRel = pm.distFilterRel
    distFilterAbsValue = pm.distFilterAbsValue
    distFilterRelValue = pm.distFilterRelValue
    trimToSelectedLinks = pm.trimToSelectedLinks
    if trimToSelectedLinks:
        selectedLinkIDs = pm.selectedLinkIDs
    outputGPSCombined = pm.outputGPSCombined
    outputGPSCombinedConcatCol = pm.outputGPSCombinedConcatCol
    outputAllCombined = pm.outputAllCombined
    outputAllRouteLinks = pm.outputAllRouteLinks
    outputGISRouteLinks = pm.outputGISRouteLinks
    outputCombinedRouteLinks = pm.outputCombinedRouteLinks
    outputdistanceFilteredData = pm.outputdistanceFilteredData
    outputDistanceFilteredRouteLinks = pm.outputDistanceFilteredRouteLinks
    outputFlagFilteredRouteLinks = pm.outputFlagFilteredRouteLinks
    outputRouteLinksToGIS = pm.outputRouteLinksToGIS

    #create a directory for all output file (if not already existing)
    if distFilterAbs & distFilterRel:
        dir_ext = str(distFilterAbsValue) + 'metres '+ str(distFilterRelValue) + 'perc'
    elif distFilterAbs:
        dir_ext = str(distFilterAbsValue) + 'metres'
    elif distFilterRel:
        dir_ext = str(distFilterRelValue) + 'perc'
    else:
        dir_ext = 'NoDistanceFilter'

    outputDirectory=outputDirectory+'_'+dir_ext
    DR.makeNewDir(outputDirectory)

    if outputRouteLinksToGIS:
        DR.makeNewDir(outputDirectory+'/GIS')

    #combine all raw data GPS CSVs into one large csv
    allGpsData = combineCsv(directory)
    if trimToSelectedLinks:
        routeLinks = pd.read_csv(selectedLinkIDs)
        allGpsData = allGpsData.merge(routeLinks,on=gpsLinkId)
    if outputGPSCombined:
        allGpsData.to_csv(outputDirectory+'/allGPSdataCombined.csv',index=False)

    print('GPS data combined')

    #read in GIS data
    gisData = pd.read_csv(gisAttributesFile)
    print('GIS data read in at: ',np.round(time.time()-st,2), ' seconds')

    #output route links file for every link of the raw GPS data
    if outputAllRouteLinks:
        allGPSRouteLinks = extractRouteLinks(allGpsData,gpsLinkId,outputDirectory,'allGPSRouteLinks')
        print('allGPSRouteLinks extracted')
        print('Time elapsed so far: ',np.round(time.time()-st,2), ' seconds')

        #optional additional merge with a GIS dataset
        if outputRouteLinksToGIS:
            allGPSRouteLinks_GIS = allGPSRouteLinks.to_frame().merge(gisData,left_on=gpsLinkId,right_on=gisLinkId)
            allGPSRouteLinks_GIS.to_csv(outputDirectory+'/GIS/allGPSRouteLinks_GIS.csv') 

    #adds a concatenated link_id,distance column and outputs to csv
    if outputGPSCombinedConcatCol:
        gpsLinkIds = allGpsData[gpsLinkId].astype(str)
        gpsDists = allGpsData[gpsDist].astype(str)
        allGpsData.insert(allGpsData.columns.get_loc(gpsLinkId)+1,gpsLinkId+"_"+gpsDist,gpsLinkIds.str.cat(gpsDists,sep="_"))
        allGpsData.to_csv(outputDirectory+'/allGPSdataCombined.csv',index=False)

    
    #output route links file of GIS data in isolation
    if outputGISRouteLinks:
        GISRouteLinks = extractRouteLinks(gisData,gisLinkId,outputDirectory,'GISRouteLinks')
        print('GISRouteLinks extracted')
        print('Time elapsed so far: ',np.round(time.time()-st,2), ' seconds')

        #optional additional merge with a GIS dataset
        if outputRouteLinksToGIS:
            GISRouteLinks_GIS = GISRouteLinks.to_frame().merge(gisData,left_on=gpsLinkId,right_on=gisLinkId)
            GISRouteLinks_GIS.to_csv(outputDirectory+'/GIS/GISRouteLinks_GIS.csv') 

        

    #merges GPS and GIS data based on link_ids (and optionally outputs to csv)

    allCombinedData = allGpsData.merge(gisData,left_on=gpsLinkId,right_on=gisLinkId)

    if outputCombinedRouteLinks:
        combinedRouteLinks = extractRouteLinks(allCombinedData,gpsLinkId,outputDirectory,'combinedRouteLinks')

        #optional additional merge with a GIS dataset
        if outputRouteLinksToGIS:
            combinedRouteLinks_GIS = combinedRouteLinks.to_frame().merge(gisData,left_on=gpsLinkId,right_on=gisLinkId)
            combinedRouteLinks_GIS.to_csv(outputDirectory+'/GIS/combinedRouteLinks_GIS.csv') 

        print('combinedRouteLinks extracted')
        print('Time elapsed so far: ',np.round(time.time()-st,2), ' seconds')
        if outputAllRouteLinks:
            notInGPSRouteLinks = dataframeDifference(GISRouteLinks,combinedRouteLinks,outputDirectory,'notInGPSRouteLinks')
            
            #optional additional merge with a GIS dataset
            if outputRouteLinksToGIS:
                notInGPSRouteLinks_GIS = notInGPSRouteLinks.merge(gisData,left_on=gpsLinkId,right_on=gisLinkId)
                notInGPSRouteLinks_GIS.to_csv(outputDirectory+'/GIS/notInGPSRouteLinks_GIS.csv')                

            print('notInGISRouteLinks extracted')
            print('Time elapsed so far: ',np.round(time.time()-st,2), ' seconds')
            notInGISRouteLinks = dataframeDifference(allGPSRouteLinks,combinedRouteLinks,outputDirectory,'notInGISRouteLinks')

            #optional additional merge with a GIS dataset
            if outputRouteLinksToGIS:
                notInGISRouteLinks_GIS = notInGISRouteLinks.merge(gisData,left_on=gpsLinkId,right_on=gisLinkId)
                notInGISRouteLinks_GIS.to_csv(outputDirectory+'/GIS/notInGISRouteLinks_GIS.csv')    

    allCombinedData["Dist_diff"] = allCombinedData[gpsDist] - allCombinedData[gisDist]
    #allCombinedData["Perc_Dist_diff"] = 2*allCombinedData["Dist_diff"]/(allCombinedData[gisDist] + allCombinedData[gpsDist])
    allCombinedData["Perc_Dist_diff"] = allCombinedData["Dist_diff"]/allCombinedData[gisDist]

    allCombinedDataSelected = allCombinedData[selectedColumnsFiltered] #choose output columns here
    if outputAllCombined:        
        allCombinedDataSelected.to_csv(outputDirectory+'/allDataCombined.csv',index=False)
        print('allDataCombined saved to file')
        print('Time elapsed so far: ',np.round(time.time()-st,2), ' seconds')
        

    #distance filtering based on user input (and outputting to csv)
    if incompleteJourneyFilter:
        allCombinedData = allCombinedData[allCombinedData[incompleteFlag]==0]        
        flagFilteredData = allCombinedData.copy()
        
    if distFilterAbs:
        allCombinedData = allCombinedData[allCombinedData.Dist_diff < distFilterAbsValue]
        allCombinedData = allCombinedData[allCombinedData.Dist_diff > -distFilterAbsValue]
    if distFilterRel:
        allCombinedData = allCombinedData[allCombinedData.Perc_Dist_diff < distFilterRelValue/100]
        allCombinedData = allCombinedData[allCombinedData.Perc_Dist_diff > -distFilterRelValue/100]

    print('distance filtering complete')
    print('Time elapsed so far: ',np.round(time.time()-st,2), ' seconds')

    #output distance filtered data to csv
    distanceFilteredData = allCombinedData
    distanceFilteredDataSelected = distanceFilteredData[TMJ_columns]
    if outputdistanceFilteredData:        
        DR.makeNewDir(outputDirectory+'/distanceFilteredData')
        distanceFilteredDataSelected.to_csv(outputDirectory+'/distanceFilteredData/distanceFilteredData.csv',index=False)
        print('distanceFilteredData saved to file')
        print('Time elapsed so far: ',np.round(time.time()-st,2), ' seconds')

    #output route links file of links satisfying distance difference criteria
    if outputDistanceFilteredRouteLinks:
        distanceFilteredRouteLinks = extractRouteLinks(distanceFilteredData,gpsLinkId,outputDirectory,'distanceFilteredRouteLinks')
        print('distanceFilteredRouteLinks extracted')
        print('Time elapsed so far: ',np.round(time.time()-st,2), ' seconds')

        #optional additional merge with a GIS dataset
        if outputRouteLinksToGIS:
            distanceFilteredRouteLinks_GIS = distanceFilteredRouteLinks.to_frame().merge(gisData,left_on=gpsLinkId,right_on=gisLinkId)
            distanceFilteredRouteLinks_GIS.to_csv(outputDirectory+'/GIS/distanceFilteredRouteLinks_GIS.csv')    

        if outputCombinedRouteLinks:
            filteredOutRouteLinks = dataframeDifference(combinedRouteLinks,distanceFilteredRouteLinks,outputDirectory,'filteredOutRouteLinks')
            print('filteredOutRouteLinks extracted')
            print('Time elapsed so far: ',np.round(time.time()-st,2), ' seconds')

        #optional additional merge with a GIS dataset
        if outputRouteLinksToGIS:
            filteredOutRouteLinks_GIS = filteredOutRouteLinks.merge(gisData,left_on=gpsLinkId,right_on=gisLinkId)
            filteredOutRouteLinks_GIS.to_csv(outputDirectory+'/GIS/filteredOutRouteLinks_GIS.csv')

        if outputAllRouteLinks:
            notInGIS_AND_filteredOutByDist_RouteLinks = dataframeDifference(allGPSRouteLinks,distanceFilteredRouteLinks,outputDirectory,'notInGIS_AND_filteredOutByDist_RouteLinks')
            print('notInGIS_AND_filteredOutByDist_RouteLinks extracted')
            print('Time elapsed so far: ',np.round(time.time()-st,2), ' seconds')

        #optional additional merge with a GIS dataset
        if outputRouteLinksToGIS:
            notInGIS_AND_filteredOutByDist_RouteLinks_GIS = notInGIS_AND_filteredOutByDist_RouteLinks.merge(gisData,left_on=gpsLinkId,right_on=gisLinkId)
            notInGIS_AND_filteredOutByDist_RouteLinks_GIS.to_csv(outputDirectory+'/GIS/notInGIS_AND_filteredOutByDist_RouteLinks_GIS.csv')

    #FLAG FILTERED
    if outputFlagFilteredRouteLinks:
        flagFilteredDataRouteLinks = extractRouteLinks(flagFilteredData,gpsLinkId,outputDirectory,'flagFilteredDataRouteLinks')
        filteredOutByFlagRouteLinks = dataframeDifference(combinedRouteLinks,flagFilteredDataRouteLinks,outputDirectory,'filteredOutByFlagRouteLinks')

        #optional additional merge with a GIS dataset
        if outputRouteLinksToGIS:
            filteredOutByFlagRouteLinks_GIS = filteredOutByFlagRouteLinks.merge(gisData,left_on=gpsLinkId,right_on=gisLinkId)
            filteredOutByFlagRouteLinks_GIS.to_csv(outputDirectory+'/GIS/filteredOutByFlagRouteLinks_GIS.csv')

        
        notFlaggedButFilteredOutByDistance = dataframeDifference(flagFilteredDataRouteLinks,distanceFilteredRouteLinks,outputDirectory,'notFlaggedButFilteredOutByDistance')

        #optional additional merge with a GIS dataset
        if outputRouteLinksToGIS:
            notFlaggedButFilteredOutByDistance_GIS = notFlaggedButFilteredOutByDistance.merge(gisData,left_on=gpsLinkId,right_on=gisLinkId)
            notFlaggedButFilteredOutByDistance_GIS.to_csv(outputDirectory+'/GIS/notFlaggedButFilteredOutByDistance_GIS.csv')

    #prints out data filtering stats
    df_NF = allCombinedDataSelected #NF for not filtered
    if incompleteJourneyFilter:
        df_FF = flagFilteredData #FF for flagFiltered
    else:
        df_FF = allCombinedDataSelected.copy() #flag filtered summary stats will be nonsensical with this condition
    print('data read')
    df_NF['Perc_Dist_diff']=df_NF['Perc_Dist_diff']*100
    df_FF['Perc_Dist_diff']=df_FF['Perc_Dist_diff']*100
    mindf=df_NF['Perc_Dist_diff'].min()
    maxdf=df_NF['Perc_Dist_diff'].max()
    print('total number of records: ',sum(df_NF['n']))
    df_flagged=df_NF[df_NF.incomplete_flag==1]
    df_above=df_NF[df_NF.Perc_Dist_diff>distFilterRelValue]
    df_above_FF=df_FF[df_FF.Perc_Dist_diff>distFilterRelValue]
    print('number of records above filter value: ',sum(df_above["n"]))
    #print('perc of records above 1%: ',sum(df_above1["n"])*100/len(df))
    df_below=df_NF[df_NF.Perc_Dist_diff<-distFilterRelValue]
    df_below_FF=df_FF[df_FF.Perc_Dist_diff<-distFilterRelValue]
    print('number of records below filter value: ',sum(df_below["n"]))
    #print('perc of records below -1%: ',sum(df_belowminus1["n"])*100/len(df))
    StringData = StringIO("""Measure;Value
                Total number of obsv before filtering;"""+str(sum(df_NF["n"]))+"""
                Number of obsv with incomplete flag """+";"+str(sum(df_flagged["n"]))+"""
                Number of obsv above """+str(distFilterRelValue)+"%;"+str(sum(df_above["n"]))+"""
                Number of obsv below -"""+str(distFilterRelValue)+"%;"+str(sum(df_below["n"]))+"""
                Number of obsv above """+str(distFilterRelValue)+"% without incomplete flag;"+str(sum(df_above_FF["n"]))+"""
                Number of obsv below -"""+str(distFilterRelValue)+"% without incomplete flag;"+str(sum(df_below_FF["n"]))+"""
                Number of obsv removed from distance filters;"""+str(sum(df_above_FF["n"])+sum(df_below_FF["n"]))+"""
                Number of obsv remaining;"""+str(sum(distanceFilteredData["n"])))
                #Number of obsv remaining;"""+str(sum(df_NF["n"])-sum(df_above["n"])-sum(df_below["n"])))

    df_out=pd.read_csv(StringData, sep =";")
    df_out.to_csv(outputDirectory+'/DataFilteringSummaryStats'+str(distFilterRelValue)+'.csv',index=False)                 
               
    print('Script complete in ',np.round(time.time()-st,2), ' seconds')

    return distanceFilteredData

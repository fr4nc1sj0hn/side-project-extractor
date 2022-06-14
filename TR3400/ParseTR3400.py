import pandas as pd
from os import listdir
from os.path import isfile, join
from datetime import datetime
import numpy as np

import config as cfg

def ParseTR3400FinalLog():
	path = cfg.logfolderlocation
	fullPath = path + cfg.logfile
	outputfolder = cfg.LocalCSVLocation

	final = pd.read_csv(fullPath)

	#tool
	tool = {
	    "ToolID":[1],
	    "ToolName":["TR3400"]
	}
	tools = pd.DataFrame.from_dict(tool)

	toolCSV = outputfolder + "tools.csv"
	tools.to_csv(toolCSV)

	#fact
	fact = final[
	    [
	        "Job Name",
	        "Lot",
	        "Lot_Final",
	        "FOUP",
	        "Wafer",
	        "Slot",
	        "Lot Start Time",
	        "Lot End Time",
	        "Wafer Start Time",
	        "Wafer End Time"
	    ]
	]
	fact["ToolID"] = 1
	fact['DateTime'] = fact['Lot Start Time'].apply(lambda x: pd.to_datetime(str(x), format='%Y-%m-%d %H:%M:%S'))
	fact['Wafer Start Time'] = fact['Wafer Start Time'].apply(lambda x: pd.to_datetime(str(x), format='%Y-%m-%d %H:%M:%S'))
	fact['Wafer End Time'] = fact['Wafer End Time'].apply(lambda x: pd.to_datetime(str(x), format='%Y-%m-%d %H:%M:%S'))
	fact["Date"] = fact['DateTime'].dt.date
	fact["Time"] = fact['DateTime'].dt.time
	fact["Duration (minutes)"] = (fact['Wafer End Time'] - fact['Wafer Start Time']).astype("timedelta64[s]")/60.0
	fact[['Lot']] = fact[['Lot']].fillna(value="<blank>")

	fact = fact.sort_values(['Job Name', 'Lot','DateTime'],
                  ascending=[True, True,True])
	fact['rownumber'] = fact.groupby(['Job Name','Lot']).cumcount()
	fact['countnumber'] = fact.groupby(['Job Name','Lot'])['DateTime'].transform('count')


	jobs = fact["Job Name"].unique()

	columns = [
	    "Job Name",
	    "Lot",
	    "Lot_Final",
	    "FOUP",
	    "Wafer",
	    "Slot",
	    "Lot Start Time",
	    "Lot End Time",
	    "Wafer Start Time",
	    "Wafer End Time",
	    "Message",
	    "ToolID",
	    "DateTime",
	    "Date",
	    "Time",
	    "Duration (minutes)"
	]
	FinalOutput = pd.DataFrame(columns = columns)


	for job in jobs:
	    jobdata = fact[fact["Job Name"] == job]
	    lots = jobdata["Lot"].unique()
	    
	    for lot in lots:
	        lotdata = jobdata[jobdata["Lot"] == lot]
	        lotStart = lotdata[lotdata["rownumber"] == 0]["Lot Start Time"].values[0]
	        lotEnd = lotdata.iloc[-1:]["Lot End Time"].values[0]
	        
	        lotStart =  datetime.strptime(lotStart, '%Y-%m-%d %H:%M:%S')
	        lotEnd = datetime.strptime(lotEnd, '%Y-%m-%d %H:%M:%S')

	        LotDuration = (lotEnd - lotStart).seconds/60

	        lotStart_df = pd.DataFrame()
	        lotStart_df["Job Name"] =  lotdata["Job Name"]
	        lotStart_df["Lot"] =  lotdata["Lot"]
	        lotStart_df["Lot_Final"] =  lotdata["Lot_Final"]
	        lotStart_df["FOUP"] =  lotdata["FOUP"]
	        lotStart_df["Wafer"] =  lotdata["Wafer"]
	        lotStart_df["Slot"] =  -1
	        lotStart_df["Lot Start Time"] =  lotStart
	        lotStart_df["Lot End Time"] =  lotEnd
	        lotStart_df["Wafer Start Time"] =  ""
	        lotStart_df["Wafer End Time"] =  ""
	        lotStart_df["Message"] =  "Lot Start"
	        lotStart_df["ToolID"] =  lotdata["ToolID"]
	        lotStart_df["DateTime"] =  lotdata["DateTime"]
	        lotStart_df["Date"] =  lotdata["Date"]
	        lotStart_df["Time"] =  lotdata["Time"]
	        lotStart_df["Duration (minutes)"] =  np.nan

	        FinalOutput = pd.concat([FinalOutput,lotStart_df])
	        
	        lotEnd_df = pd.DataFrame()
	        lotEnd_df["Job Name"] =  lotdata["Job Name"]
	        lotEnd_df["Lot"] =  lotdata["Lot"]
	        lotEnd_df["Lot_Final"] =  lotdata["Lot_Final"]
	        lotEnd_df["FOUP"] =  lotdata["FOUP"]
	        lotEnd_df["Wafer"] =  lotdata["Wafer"]
	        lotEnd_df["Slot"] =  9999
	        lotEnd_df["Lot Start Time"] =  lotStart
	        lotEnd_df["Lot End Time"] =  lotEnd
	        lotEnd_df["Wafer Start Time"] =  ""
	        lotEnd_df["Wafer End Time"] =  ""
	        lotEnd_df["Message"] =  "Lot End"
	        lotEnd_df["ToolID"] =  lotdata["ToolID"]
	        lotEnd_df["DateTime"] =  lotdata["DateTime"]
	        lotEnd_df["Date"] =  lotdata["Date"]
	        lotEnd_df["Time"] =  lotdata["Time"]
	        lotEnd_df["Duration (minutes)"] =  LotDuration
	        
	        FinalOutput = pd.concat([FinalOutput,lotEnd_df])
	        
	        df = pd.DataFrame()
	        df["Job Name"] =  lotdata["Job Name"]
	        df["Lot"] =  lotdata["Lot"]
	        df["Lot_Final"] =  lotdata["Lot_Final"]
	        df["FOUP"] =  lotdata["FOUP"]
	        df["Wafer"] =  lotdata["Wafer"]
	        df["Slot"] =  lotdata["Slot"]
	        df["Lot Start Time"] =  lotStart
	        df["Lot End Time"] =  lotEnd
	        df["Wafer Start Time"] =  lotdata["Wafer Start Time"]
	        df["Wafer End Time"] =  lotdata["Wafer End Time"]
	        df["Message"] =  lotdata["Duration (minutes)"]
	        df["ToolID"] =  lotdata["ToolID"]
	        df["DateTime"] =  lotdata["DateTime"]
	        df["Date"] =  lotdata["Date"]
	        df["Time"] =  lotdata["Time"]
	        df["Duration (minutes)"] =  np.nan

	        FinalOutput = pd.concat([FinalOutput,df])

	FinalOutput = FinalOutput.sort_values(['Lot','Job Name','Wafer','DateTime','Slot'],
                  ascending=[True, True,True,True,True])

	finalCSV = outputfolder + 'TR3400Final.csv'
	FinalOutput.to_csv(finalCSV)
	        
if __name__ == "__main__":
	print("reading files from: ", cfg.logfolderlocation)

	ParseTR3400FinalLog()
	
	print("written to:",cfg.LocalCSVLocation)
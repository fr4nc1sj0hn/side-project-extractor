import pandas as pd
from os import listdir
from os.path import isfile, join
from datetime import datetime
import config as cfg

def ParseAFMLogs():
	path = cfg.logfolderlocation

	fullPaths = [folder for folder in listdir(path)]
	

	#Parse the folders and sort by Lot and Datetime
	logInfo = pd.DataFrame()
	logInfo["SlotFolder"] = [path + "/" + f for f in fullPaths]
	lots = [f.split("_")[2] for f in fullPaths]
	logInfo["Lot"] = lots
	logInfo["Date"] = ["20" + f.split("_")[0] for f in fullPaths]
	logInfo["Time"] = [f.split("_")[1] for f in fullPaths]
	logInfo["DateTime"] = logInfo["Date"] + " " + logInfo["Time"]
	logInfo['Date'] = logInfo['Date'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d'))
	logInfo['DateTime'] = logInfo['DateTime'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d %H%M%S'))
	logInfo['DateTimeStr'] = logInfo['DateTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
	logInfo["Date"] = logInfo['DateTime'].dt.date
	logInfo["Time"] = logInfo['DateTime'].dt.time


	logInfoSorted = logInfo.sort_values(['Lot', 'DateTime'],
	                  ascending=[True, True])
	logInfoSorted['rownumber'] = logInfo.groupby(['Lot']).cumcount()
	logInfoSorted['countnumber'] = logInfo.groupby(['Lot'])['DateTime'].transform('count')

	#these are the lots
	lots = logInfoSorted["Lot"].unique()


	#parsing of the logs
	columns = ["LotID","CarrierID","TimeStamp","Message","WaferID","SlotNumber","Date","Time"]

	lotdf = pd.DataFrame(columns = columns)

	for lot in lots:
	    slotdata = logInfoSorted[logInfoSorted["Lot"] == lot]
	    timestamp = slotdata[slotdata["rownumber"] == 0].DateTime.values[0]
	    date = slotdata[slotdata["rownumber"] == 0].Date.values[0]
	    time = slotdata[slotdata["rownumber"] == 0].Time.values[0]

	    
	    lotStart = {
	        "LotID": [lot],
	        "CarrierID": [""],
	        "TimeStamp": [timestamp],
	        "Message": ["Lot Start"],
	        "WaferID": [""],
	        "SlotNumber": [""],
	        "Date": [date],
	        "Time": [time]
	    }
	    df_lotstart = pd.DataFrame.from_dict(lotStart)
	    lotdf = pd.concat([lotdf,df_lotstart])

	    for index, row in slotdata.iterrows():
	        csvPath = row["SlotFolder"] + "/Info/"
	        files = [f for f in listdir(csvPath) if isfile(join(csvPath, f))]
	        
	        for file in files:
	            slotNum = int(file.split("_")[0].replace("Slot",""))
	            WaferID = "@" + str(slotNum)
	           
	            file = csvPath + file
	            columnPlaceholder = ['col' + str(i) for i in range(15)]
	            df = pd.read_csv(file,names=columnPlaceholder)
	            carrier_id = df[df["col0"] == "Carrier ID"]["col1"].values[0]
	  
	            #now get the date
	            #look for Tip Slot No
	            index = df[df["col0"] == "Tip Slot No"].index[0]
	            steps_header = index + 1
	            new_header = df.iloc[steps_header] #grab the first row for the header
	            steps = df[steps_header + 1:] #take the data less the header row
	            steps.columns = new_header #set the header row as the df header
	            end_date_time = steps.iloc[-1:]["Date"].values[0]
	            end_date_time =  datetime.strptime(end_date_time, '%Y/%m/%d %H:%M:%S')
	            #print("Lot ID: ", lot,"Carrier ID:", carrier_id,"End Date:" , end_date )
	            
	            lotFileInfo = {
	                "LotID": [lot],
	                "CarrierID": [carrier_id],
	                "TimeStamp": [end_date_time],
	                "Message": [""],
	                "WaferID": [WaferID],
	                "SlotNumber": [slotNum],
	                "Date": [end_date_time.date()],
	                "Time": [end_date_time.time()]
	            }
	            df_lotfileInfo = pd.DataFrame.from_dict(lotFileInfo)
	            lotdf = pd.concat([lotdf,df_lotfileInfo])


	    lotEnd = {
	        "LotID": [lot],
	        "CarrierID": [""],
	        "TimeStamp": [end_date_time],
	        "Message": ["Lot End"],
	        "WaferID": [""],
	        "SlotNumber": [""],
	        "Date": [end_date_time.date()],
	        "Time": [end_date_time.time()]
	    }
	    df_lotend = pd.DataFrame.from_dict(lotEnd)
	    lotdf = pd.concat([lotdf,df_lotend])

	return lotdf


if __name__ == "__main__":
	print("reading files from: ", cfg.logfolderlocation)

	df = ParseAFMLogs()
	#write to CSV
	LocalCSVLocation = cfg.LocalCSVLocation

	print("first 10 rows...")
	print("========================================================================")
	print(df.head(10))

	csvName = "AFMLogs.csv"

	fullPath = LocalCSVLocation + csvName

	if not isfile(fullPath):
		df.to_csv(fullPath,index=False)
	else: # else it exists so append without writing the header
		df.to_csv(fullPath, mode='a', header=False,index=False)
	
	print("written to:",fullPath)
import pandas as pd
from os import listdir
from os.path import isfile, join
import sys
import warnings

class LogParser():
    def __init__(self,path,isFolder):
        self.path = path
        self.isFolder = isFolder
        columns = ['Timestamp', 'Log Message', 'Message', 'User Path', 'Customer Path','Lot ID', 'Wafer ID', 'Port ID', 'Date', 'Time']
        df = pd.DataFrame(columns=columns)
        self.parsedLog = df
        
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            if isFolder == 'Y':
                files = [path + f for f in listdir(self.path) if isfile(join(self.path, f))]
                for file in files:
                    self.ParseLogs(file)
            else:
                self.ParseLogs(self.path)
        
    def ParseLogs(self,file):
        #placeholder columns to get all data

        columnPlaceholder = [
            'Event Type',
            'Timestamp',
            'Log Message',
            'Category',
            'User',
            'Thread ID',
            'Error Code',
            'Application',
            'Machine'
        ]
        columnPlaceholder = columnPlaceholder + ['col' + str(i) for i in range(100)]
        print("processing",file)
        df = pd.read_csv(file, sep='\t', lineterminator='\n',names=columnPlaceholder, on_bad_lines='skip')
        
        #step 1
        #Table.RemoveColumns({"Error Code", "Application", "Machine", "Category", "Event Type", "Thread ID", "User", "Source.Name"})
        step1 = df.drop(["Error Code", "Application", "Machine", "Category", "Event Type", "Thread ID", "User"],axis=1)
        
        #step2 Table.SelectRows(each Text.StartsWith([Log Message], "Lot Run - Dumping special environment variables") or 
        #Text.StartsWith([Log Message], "Lot Run - Lot Inspection Done") or 
        #Text.StartsWith([Log Message], "Wafer Run - Time per wafer inspection ") and Text.EndsWith([Log Message], ">"))
        step2 = step1[
            (step1["Log Message"].str.startswith('Lot Run - Dumping special environment variables',na=False))
            | (step1["Log Message"].str.startswith('Lot Run - Lot Inspection Done',na=False))
            | (step1["Log Message"].str.startswith('Wafer Run - Time per wafer inspection ',na=False))
            & (step1["Log Message"].str.endswith('>',na=False))
        ]
        
        #step3 Table.ReplaceValue("Lot Run - Dumping special environment variables:<Lot Run -Lot name : ",
        #"Lot Start <",
        #Replacer.ReplaceText,{"Log Message"})
        step2["Log Message"] = step2["Log Message"].str.replace(
            "Lot Run - Dumping special environment variables:<Lot Run -Lot name : ",
            "Lot Start <"
        )
        
        #step4 Table.ReplaceValue(
        #"Wafer Run - Time per wafer inspection = ",
        #"Duration = ",Replacer.ReplaceText,{"Log Message"})

        step2["Log Message"] = step2["Log Message"].str.replace(
            "Wafer Run - Time per wafer inspection = ",
            "Duration = "
        )
        
        #step5 Table.ReplaceValue(
        #"Lot Run - Lot Inspection Done<Lot Run -Lot name : ",
        #"Lot End <",Replacer.ReplaceText,{"Log Message"})
        step2["Log Message"] = step2["Log Message"].str.replace(
            "Lot Run - Lot Inspection Done<Lot Run -Lot name : ",
            "Lot End <"
        )
        
        #step6 Table.ReplaceValue(
        #"<Wafer Run - Lot name: ",
        #"<Wafer Run - Lot name: ",Replacer.ReplaceText,{"Log Message"})

        step2["Log Message"] = step2["Log Message"].str.replace(
            "<Wafer Run - Lot name: ",
            "<Wafer Run - Lot name: "
        )
        
        logs = step2[["Timestamp","Log Message"]]
        
        #step7 Table.SplitColumn("Log Message", Splitter.SplitTextByEachDelimiter({"<"}, QuoteStyle.Csv, false), {"Log Message.1", "Log Message.2"})
        logs[["Log Message.1", "Log Message.2"]] = logs['Log Message'].str.split('<', expand=True)
        
        #step8 Table.ReplaceValue("Wafer Run - Lot name: ","<",Replacer.ReplaceText,{"Log Message.2"})
        logs["Log Message.2"] = logs["Log Message.2"].str.replace(
            "Wafer Run - Lot name: ",
            "<"
        )
        
        #step9 Table.ReplaceValue("<","",Replacer.ReplaceText,{"Log Message.2"})
        logs["Log Message.2"] = logs["Log Message.2"].str.replace(
            "<",
            ""
        )
        
        #step10 Table.ReplaceValue("Duration = ","",Replacer.ReplaceText,{"Log Message.1"})
        logs["Log Message.1"] = logs["Log Message.1"].str.replace(
            "Duration = ",
            ""
        )
        
        #step11 Table.ReplaceValue(">","",Replacer.ReplaceText,{"Log Message.2"})
        logs["Log Message.2"] = logs["Log Message.2"].str.replace(
            ">",
            ""
        )
        
        #Step12 Table.SplitColumn("Log Message.2", Splitter.SplitTextByDelimiter("\", QuoteStyle.Csv), {"Log Message.2.2", "Log Message.2.3"})
        logs[["Log Message.2.1","Log Message.2.2", "Log Message.2.3"]] = logs['Log Message.2'].str.split("\\", expand=True)
        
        #step13 Table.SplitColumn("Log Message.2.3", Splitter.SplitTextByEachDelimiter({"Wafer ID: "}, QuoteStyle.Csv, false), {"Log Message.2.3.1", "Log Message.2.3.2"})
        logs[["Log Message.2.3.1","Log Message.2.3.2"]] = logs['Log Message.2.3'].str.split("Wafer ID: ", expand=True)
        
        #step14 Table.SplitColumn("Log Message.2.3.2", Splitter.SplitTextByEachDelimiter({"Port No: "}, QuoteStyle.Csv, false), {"Log Message.2.3.2.1", "Log Message.2.3.2.2"})
        logs[["Log Message.2.3.2.1","Log Message.2.3.2.2"]] = logs['Log Message.2.3.2'].str.split("Port No: ", expand=True)
        
        #step 15 
        """
        Table.RenameColumns({
        #{"Log Message.2.3.2.1", "Wafer ID"}, 
        #{"Log Message.2.3.2.2", "Port ID"}, 
        #{"Log Message.2.3.1", "Lot ID"}, 
        #{"Log Message.2.2", "Customer Path"}, 
        #{"Log Message.2.1", "User Path"}, 
        #{"Log Message.1", "Message"}})
        """
        logs.rename(columns = {
            "Log Message.2.3.2.1": "Wafer ID", 
            "Log Message.2.3.2.2": "Port ID", 
            "Log Message.2.3.1": "Lot ID", 
            "Log Message.2.2": "Customer Path", 
            "Log Message.2.1": "User Path", 
            "Log Message.1": "Message"
        }, inplace = True)
        
        columns = [col for col in logs.columns if col not in ["Log Message.2","Log Message.2.3","Log Message.2.3.2"]]
        step16 = logs[columns]
        
        step16["Timestamp"] = pd.to_datetime(step16.Timestamp)
        step16["Date"] = step16['Timestamp'].dt.date
        step16["Date"] = pd.to_datetime(step16.Date)
        step16["Time"] = step16['Timestamp'].dt.time
        
        self.parsedLog = pd.concat([self.parsedLog,step16],ignore_index=True)
        self.parsedLog.to_csv("logcompiled.csv")


if __name__ == "__main__":
    path = sys.argv[1]
    isFolder = sys.argv[2]
    print(path)
    parser = LogParser(path=path,isFolder = isFolder)
    print("first 10 rows...")
    print("========================================================================")
    print(parser.parsedLog.head(10))
    print("rows and columns",parser.parsedLog.shape)
import pandas as pd
import json
import os
from glob import glob
import argparse
import xlsxwriter
from datetime import datetime

version=0.1

# Argumment Setup. User to insert data file location with --location flag

print ("Merge Process Innitiated...")
#parser = argparse.ArgumentParser(description = 'Merges csv and json files into one file')
#parser.add_argument('-p', '--path', required =True, help = 'File Path of Data')
#parser.add_argument('-p', '--path', required =False, help = 'File Path of Data')
#args = parser.parse_args()
#path = args.path
#path="C:\Users\Desktop\Scripts\zt_trial_run_data"
#path="C:/Users/minasamy/Desktop/Scripts/zt_trial_run_data"
#path="C:\\Users\\minasamy\\Desktop\\Scripts\\MP2\\ROCm_FTB"
path="C:\\Data"

def write_xl_tables(df, df2, input_file_name, input_file_name2, output_file):
    writer = pd.ExcelWriter(output_file.replace("csv", "xlsx"), engine='xlsxwriter')
    # Convert the dataframe to an XlsxWriter Excel object. Turn off the default
    # header and index and skip one row to allow us to insert a user defined
    # header.
    sheet_name = input_file_name
    sheet_name =  sheet_name[:31]
    df["input_file"] = input_file_name
    df.to_excel(writer, sheet_name= sheet_name, startrow=1, header=False, index=False)
    sheet_name2 = input_file_name2
    sheet_name2 =  sheet_name2[:31]
    df2["input_file"] = input_file_name2
    df2.to_excel(writer, sheet_name= sheet_name2, startrow=1, header=False, index=False)
    # Get the xlsxwriter workbook and worksheet objects.
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]
    worksheet2 = writer.sheets[sheet_name2]
    # Get the dimensions of the dataframe.
    (max_row, max_col) = df.shape
    (max_row2, max_col2) = df2.shape
    # Create a list of column headers, to use in add_table().
    column_settings = []
    for header in df.columns:
        column_settings.append({'header': header})
    column_settings2 = []
    for header in df2.columns:
        column_settings2.append({'header': header})
    # Add the table.
    worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})
    worksheet2.add_table(0, 0, max_row2, max_col2 - 1, {'columns': column_settings2})
    # Make the columns wider for clarity.
    worksheet.set_column(0, max_col - 1, 12)
    worksheet2.set_column(0, max_col2 - 1, 12)
    # Close the Pandas Excel writer and output the Excel file.
    writer.save()

def merge(path):
    #Aggregated files
    #Finds all the .csv files in all the aggregated folders. Avoids the aggregated file in the unilog folder as the data there is already merged
    
    '''
    #setting 1: merge all aggregated unilogs of each app
    EXT = "*.csv"
    all_agg_files = []
    for path, subdir, files in os.walk(path):
        for file in glob(os.path.join(path, EXT)):
            if 'aggrrregated' in file:
                if not "unilog_" in file:
                    all_agg_files.append(file)
    '''
    '''
    #setting 2: merge "suite_summary" by bass
    EXT = "*.csv"
    all_agg_files = []
    for path, subdir, files in os.walk(path):
        for file in glob(os.path.join(path, EXT)):
            if 'suite_summary' in file:
                #if "unilog_" in file:
                    all_agg_files.append(file)
    
    '''
    #setting 3: merge "threshold_analysis" by bass
    EXT = "*.csv"
    all_agg_files = []
    for path, subdir, files in os.walk(path):
        for file in glob(os.path.join(path, EXT)):
            if 'threshold_analysis' in file:
               # if "unilog_" in file:
                    all_agg_files.append(file)
    
    #Suite Summary json files
    #Finds all the .json files with suite_summary in the title
    EXT = "*.jsson"
    #path=args.path
    all_suite_summary_files = []
    for path, subdir, files in os.walk(path):
        for file in glob(os.path.join(path, EXT)):
            if 'suite_summary' in file:
                all_suite_summary_files.append(file)


    li =[]

    # loop over the list of csv files
    for f in all_agg_files:
        
        # read the csv file, remove redundant columns, update column names and add new columns with relevant file information
        try:
            df = pd.read_csv(f)
            #df.rename(columns={'Unnamed: 0': 'Operation'}, inplace=True)
            #df.drop('Device Number',axis=1, inplace=True)
            #df.drop('File Name', axis=1, inplace=True)
            #df.drop('File Path', axis=1, inplace=True)
            metadata = f.split('\\')
            run_info= metadata[-4].split('_')[1].split('-') #setting 1,3
            #run_info= metadata[-2].split('_')[1].split('-') #setting 2
            time= run_info
            #metadata: ['C:', 'Users', 'minasamy', 'Desktop', 'Scripts', 'zt_trial_run_dat', '1108', 'PCB049525-0016_Nov_04_2021_12-48-44', 'unilogs', 'analysis', 'GPU0_aggregated_rochpl_uni.csv']
            #metadataC: C:\Data\MP2\ROCm_FTB\20220321\692211000089_102D652080001_032202248041_T20071_00001A1E4F59_102D554010C01692135002636_TE_109A203P_20220321214435_HANG\Results_2022-03-21-21-48-02-137801\unilogs\analysis\GPU1_aggregated_gemm_uni.csv
            df.insert(0, 'File Path', f)
            df.insert(1, 'File Name', metadata[-1])
            #df.insert(2, 'PCB Serial No.', run_info[0]) #setting 0
            df.insert(2, 'MP Run', metadata[2])
            df.insert(3, 'FTA/FTB', metadata[3])
            df.insert(2, 'PCB Serial No.', metadata[-5].split('_')[0]) #setting 1,3
            #df.insert(4, 'PCB Serial No.', metadata[-3].split('_')[0]) #setting 2
            df.insert(5, 'Run Date', run_info[0] + '/' + run_info[1] + '/' + run_info[2])
            df.insert(6, 'Run Time', time[3] + ':' + time[4] + ':' + time[5])
            li.append(df)

        except pd.errors.EmptyDataError:
            print("could not read this file: ", f)

    #Checks if filepath provided contains .csv files that match the criteria
    if len(li)==0:
        print("The directory agg files is empty")
    else:
        print("Merging Aggregated Data...found: ", len(li), " files")
        df=pd.concat(li, axis=0)
        # Outputs dataframe to .csv file
        #df.to_csv('C:\\Users\\minasamy\\Desktop\\Scripts\\merged_aggregated_data.csv', mode = 'w' , index = False,)
        #df.to_csv('C:\\Users\\minasamy\\Desktop\\Scripts\\merged_GPU_aggregated_[app]_uni.csv', mode = 'w' , index = False,) #setting 1
        #df.to_csv('C:\\Users\\minasamy\\Desktop\\Scripts\\merged_suite_summary_t.csv', mode = 'w' , index = False,) #setting 2
        df.to_csv('C:\\Users\\minasamy\\Desktop\\Scripts\\merged_threshold_analysis_[app].csv', mode = 'w' , index = False,) #setting 3
        print("Aggregated Data merged successfuly")

    

    li2 =[]

    # loop over the list of csv files
    for f in all_suite_summary_files:
        
        # read the json file
        df2 = pd.read_json(f, lines=True)
        metadata = f.split('\\')
        #metadata= ['C:', 'Users', 'minasamy', 'Desktop', 'Scripts', 'zt_trial_run_dat', '1108', 'PCB049525-0011_Nov_03_2021_18-19-56', '20211103_18h05_suite_summary.json']
        run_info= metadata[-2].split('_')[1].split('-')
        time= run_info
        df2.insert(0, 'File Path', f)
        df2.insert(1, 'File Name', metadata[-1])
        df2.insert(2, 'PCB Serial No.', run_info[0])
        df2.insert(3, 'Run Date', run_info[0] + '/' + run_info[1] + '/' + run_info[2])
        df2.insert(4, 'Run Time', time[3] + ':' + time[4] + ':' + time[5])
        li2.append(df2)
    
    #Checks if filepath provided contains .csv files that match the criteria
    if len(li2)==0:
        print("The directory for json files is empty")
    else:
        print("Merging Suite Summary Data...")
        df2=pd.concat(li2, axis=0)
        # Outputs dataframe to .csv file
        df2.to_csv('merged_suite_summary_data.csv', mode = 'w' , index = False)
        #Formats and combines csv files into one excel file with desired table format
        xlsx_filename = 'Merged_Data_' + str(datetime.now().strftime('%Y_%m_%d_%H_%M_%S')) + '.xlsx'
        write_xl_tables(df2, df, 'merged_suite_summary_data.csv','merged_aggregated_data.csv' , str(xlsx_filename))
        os.remove('merged_suite_summary_data.csv')
        os.remove('merged_aggregated_data.csv')
        print("Suite Summary Data merged successfuly")
        

merge(path)
print("Merge Process Complete")

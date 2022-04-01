from ast import Param
from numpy import number
import pandas as pd
import json
import os
from glob import glob
import argparse
import xlsxwriter
from datetime import datetime

version='0.2' #can merge three different csv files.

print("Merge Script Version:", version)
# Argumment Setup. User to insert data file location with --location flag

#print ("Merge Process Innitiated...")
#parser = argparse.ArgumentParser(description = 'Merges csv and json files into one file')
#parser.add_argument('-p', '--path', required =True, help = 'File Path of Data')
#parser.add_argument('-p', '--path', required =False, help = 'File Path of Data')
#args = parser.parse_args()
#path = args.path
#path="C:\\Users\\minasamy\\Desktop\\Scripts\\zt_trial_run_data"
#path="C:/Users/minasamy/Desktop/Scripts/zt_trial_run_data"
#path="C:\\Users\\minasamy\\Desktop\\Scripts\\MP2\\ROCm_FTB"
#path="C:\\Data"

#print ("hiiiiiii", os.getcwd())

input_path= input("Please enter path of the Data Directory:")
#print("You have entered: ", input_path)
path=input_path
path2=path
merge_flag=0

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
    num_of_files_scanned=0
    EXT = "*.csv"
    all_agg_files0 = []
    all_agg_files1 = []
    all_agg_files2 = []
    print("Scanning Directory has started...")
    for path, subdir, files in os.walk(path):
        for file in glob(os.path.join(path, EXT)):
            num_of_files_scanned+=1
            if num_of_files_scanned%5000==0:
                #print("Scanned ", num_of_files_scanned, " files. Found GPU aggregated", len(all_agg_files), " files total. Still Scanning...")
                print("Scanned ", num_of_files_scanned, " files. Still Scanning...")
            if 'aggregated' in file and 'unilog_' not in file:
                all_agg_files0.append(file)
            if 'suite_summary' in file:
                all_agg_files1.append(file)
            if 'threshold_analysis' in file:
                all_agg_files2.append(file)
        #if num_of_files_scanned>200:
        #    break
    print("Scan complete. Scanned a total of ", num_of_files_scanned, " files.\n")
    print("Located a total of ", len(all_agg_files0), " GPU_aggregated[app].csv files.")
    print("Located a total of ", len(all_agg_files1), " suite_summary.csv files.")
    print("Located a total of ", len(all_agg_files2), " threshold_analysis[app].csv files. \n")
    
    #Suite Summary json files
    #Finds all the .json files with suite_summary in the title
    EXT = "*.jsson"
    #path=args.path
    all_suite_summary_files = []
    for path, subdir, files in os.walk(path):
        for file in glob(os.path.join(path, EXT)):
            if 'suite_summary' in file:
                all_suite_summary_files.append(file)


    
    #print("Scanning Complete. Scanned ", num_of_files_scanned, " files total. Found ", len(all_agg_files), " files total." )
    if num_of_files_scanned:
       # print("Adding marker columns to CSVs...")
       a=1
    files_not_found_counter=0
    # loop over the list of csv files

    for flag in range(3):
        li =[]
        if flag==0:
            all_agg_files=all_agg_files0
            file_name='merged_GPU_aggregated_[app]_uni.csv'
            
        elif flag==1:
            all_agg_files=all_agg_files1
            file_name='merged_suite_summary.csv'
        elif flag==2:
            all_agg_files=all_agg_files2
            file_name='merged_threshold_analysis_[app].csv'
        else:
            print("something is wrong")
        print("Processing", file_name, "CSVs...")
        for f in all_agg_files:            
            # read the csv file, remove redundant columns, update column names and add new columns with relevant file information
            try:
                df = pd.read_csv(f)
                try:
                    df.rename(columns={'Unnamed: 0': 'Operation'}, inplace=True)
                    df.drop('Device Number',axis=1, inplace=True)
                    df.drop('File Name', axis=1, inplace=True)
                    df.drop('File Path', axis=1, inplace=True)
                except:
                    pass
                try:
                    metadata = f.split('\\')
                    if flag==0 or flag==2:
                        run_info= metadata[-4].split('_')[1].split('-') #setting 1,3
                    else:
                        run_info= metadata[-2].split('_')[1].split('-') #setting 2
                    time= run_info
                    #metadataC: C:\Data\MP2\ROCm_FTB\20220321\692211000089_102D652080001_032202248041_T20071_00001A1E4F59_102D554010C01692135002636_TE_109A203P_20220321214435_HANG\Results_2022-03-21-21-48-02-137801\unilogs\analysis\GPU1_aggregated_gemm_uni.csv
                    df.insert(0, 'File Name', metadata[-1])
                    df.insert(1, 'File Path', f)
                    #df.insert(2, 'PCB Serial No.', run_info[0]) #setting 0
                    mp_run_info=f.find('MP')
                    if mp_run_info>=0:
                        df.insert(2, 'MP Run', f[mp_run_info:mp_run_info+3])
                    #df.insert(2, 'MP Run', metadata[2])
                    FT_no=f.find('FT')
                    if FT_no>=0:
                        df.insert(3, 'FTA/FTB', f[FT_no:FT_no+3])
                    #df.insert(3, 'FTA/FTB', metadata[3])
                    part_no=f.find('D65')
                    if part_no>=0:
                        df.insert(4,'Part No.',f[part_no:part_no+6] )
                    try:
                        if flag==0 or flag==2:
                            df.insert(5, 'PCB Serial No.', metadata[-5].split('_')[0]) 
                        else:
                            df.insert(5, 'PCB Serial No.', metadata[-3].split('_')[0]) 
                        df.insert(6, 'Run Date', run_info[0] + '/' + run_info[1] + '/' + run_info[2])
                        df.insert(7, 'Run Time', time[3] + ':' + time[4] + ':' + time[5])
                    except:
                        pass    
                    li.append(df)
                except:
                    print("the format of file:", f, "is not as excpected")

            except pd.errors.EmptyDataError:
                pass
                #print("Could not read this file as it was empty. HANG Test: \n", f)
            except FileNotFoundError:
                files_not_found_counter+=1
                long_path=f
            except:
                print("File Path not in expected format")


        if files_not_found_counter:
            print("Could not open ", files_not_found_counter, " out of ", num_of_files_scanned, " because file paths are more than 259 characters. ")
            print("Example of file path: ", long_path)

        #Checks if filepath provided contains .csv files that match the criteria
        if len(li)==0: 
            #if not files_not_found_counter:
                print("The directory is either empty or not found")
        else:
            #print("Merging Aggregated Data...found: ", len(li), " files")
            df=pd.concat(li, axis=0)
            print("Merging", file_name , "files...")
            # Outputs dataframe to .csv file
            #df.to_csv('C:\\Users\\minasamy\\Desktop\\Scripts\\merged_aggregated_data.csv', mode = 'w' , index = False,)
            #df.to_csv('C:\\Users\\minasamy\\Desktop\\Scripts\\merged_GPU_aggregated_[app]_uni.csv', mode = 'w' , index = False,) #setting 1
            #df.to_csv('C:\\Users\\minasamy\\Desktop\\Scripts\\merged_suite_summary_t.csv', mode = 'w' , index = False,) #setting 2
            #df.to_csv('C:\\Users\\minasamy\\Desktop\\Scripts\\merged_threshold_analysis_[app].csv', mode = 'w' , index = False,) #setting 3
            #df.to_csv('merged_threshold_analysis_[app].csv', mode = 'w' , index = False,)
            try:
                df.to_csv(file_name, mode = 'w' , index = False,)
                print("Successfully merged all", file_name, "files\n")
            except:
                print('------Could not save merged file. Please make sure any open CSVs are closed, and this script is run with administrator access.-------\n')
                merge_flag=1
                return merge_flag
                break

#add to github
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
            x=0
            #print("The directory for json files is empty")
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
        

merge_flag=merge(path)

if merge_flag:
    print("Merge Process Incomplete")
else:
    print("Merge Process Complete")
a=input('press any key to exit')

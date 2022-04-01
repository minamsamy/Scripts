import pandas as pd
import json
import os
from glob import glob
import argparse
import xlsxwriter
from datetime import datetime

version= '0.3' #same as v0.2, cleaned up redundant code
#version='0.2' #can merge three different csv files.

print("Merge Script Version:", version)
# Argumment Setup. User to insert data file location with --location flag



input_path= input("Please enter path of the Data Directory: ")
path=input_path
path2=path
merge_flag=0


def merge(path):
    #Finds all the .csv files in all the aggregated folders. Avoids the aggregated file in the unilog folder as the data there is already merged
    
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
    print("Scan complete. Scanned a total of ", num_of_files_scanned, " files.\n")
    print("Located a total of ", len(all_agg_files0), " GPU_aggregated[app].csv files.")
    print("Located a total of ", len(all_agg_files1), " suite_summary.csv files.")
    print("Located a total of ", len(all_agg_files2), " threshold_analysis[app].csv files. \n")   
    
    
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
            df=pd.concat(li, axis=0)
            print("Merging", file_name , "files...")
            # Outputs dataframe to .csv file
            try:
                df.to_csv(file_name, mode = 'w' , index = False,)
                print("Successfully merged all", file_name, "files\n")
            except:
                print('------Could not save merged file. Please make sure any open CSVs are closed, and this script is run with administrator access.-------\n')
                merge_flag=1
                return merge_flag
                break
        
merge_flag=merge(path)

if merge_flag:
    print("Merge Process Incomplete")
else:
    print("Merge Process Complete\n")
a=input('Press any key to exit')

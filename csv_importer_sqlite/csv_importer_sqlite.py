import argparse
from pathlib import Path
import sqlite3
import pandas as pd
import os
import sys


def create_database(dbFilePath, force):
    if (os.path.isfile(dbFilePath)):
        if (force):
           os.remove(dbFilePath)
    else:        
        Path(dbFilePath).touch()


def check_if_table_exists(cursor, tableName):
    cursor.execute(" SELECT count(1) FROM sqlite_master WHERE type='table' AND name=? ", (tableName,))
    if(cursor.fetchone()[0] == 1):
        return True

    return False

def drop_table(cursor, tableName):
    cursor.execute(''' DROP TABLE '''+tableName+''' ''')

def create_table(cursor, tableName, columns):
    cursor.execute(" CREATE TABLE "+tableName+" ("+columns+")")   
    print("Table "+tableName+" created.") 


def import_csv_to_db (conn, cursor, path, force, separator):     
    for filename in os.listdir(path):
        if (filename.lower().endswith('.csv')):
            f = os.path.join(path, filename)            
            if os.path.isfile(f):
                #dtype={"user_id": int, "username": "string"}
                csvHeader = pd.read_csv(f, sep=separator, encoding='utf8', nrows=0, header=0, skipinitialspace=True) # load to DataFrame
                csvHeader.columns = csvHeader.columns.str.replace(' ', '')                
                columnsCreateTable = ' text, '.join(csvHeader.columns.tolist()) + ' text'
                #columnsDType = ': "string", '.join(csvHeader.columns.tolist()) + ': "string'
                #print("columnsDType: "+columnsDType)
                tableName = os.path.splitext(os.path.basename(f))[0]                

                if(check_if_table_exists(cursor, tableName)):
                    if(force):
                        drop_table(cursor, tableName)                        
                        create_table(cursor, tableName, columnsCreateTable)
                    else:
                        print("The table already exists: " + tableName + ". Skipping...")
                        continue
                else:
                    create_table(cursor, tableName, columnsCreateTable)
                                
                print("Importing file "+os.path.basename(f)+" into database.")                
                csvPayload = pd.read_csv(f, sep=separator, encoding='utf8') # load to DataFrame
                csvPayload.columns = csvPayload.columns.str.replace(' ', '')
                csvPayload.to_sql(tableName, conn, if_exists='append', index = False) # write to sqlite table
                print("File imported.")

def main():
    # Construct the argument parser
    ap = argparse.ArgumentParser()

    # Add the arguments to the parser
    ap.add_argument("-f", "--force", required=False, action='store_true', help="Recreate database if it exists")
    ap.add_argument("-p", "--path", required=True, help="Path to CSV files")
    ap.add_argument("-n", "--dbfilename", required=True, help="Database file name")
    ap.add_argument("-s", "--separator", required=True, help="CSV separator")
    args = vars(ap.parse_args())

    dbFilePath = os.path.join(args['path'], args['dbfilename'])+'.db'  
    create_database(dbFilePath, args['force'])
    conn = sqlite3.connect(dbFilePath)
    cursor = conn.cursor()

    import_csv_to_db (conn, cursor, args['path'], args['force'], args['separator'])

    #commit the changes to db			
    conn.commit()
    #close the connection
    conn.close()


if __name__ == '__main__':
    main()

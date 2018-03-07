import sys
import os

import pandas as pd
import sqlalchemy 
import re


# Files path
chemin = sys.path[0]
chemin_data = chemin + "/data"


import pdb

def transit_files(chemin_data):
    """
    Returns a dictionnary containing the transit file names and index column.
    """

    # Get a list of name of all transit data files. Don't take into account
    # the files beginning with '.' (private files)
    # Default index column = column 0
    dict_files = dict.fromkeys([file.rstrip('.txt') for file in os.listdir(chemin_data) if file[0] != '.'],0)
    
    # Other index column for stop_times and trips files
    dict_files['trips'] = 2
    dict_files['stop_times'] = (0,4)

    return dict_files




def ask_input(engine):
    """
    Creates the parameters table in db containing the parameters of the travel choices
    Origin, destination, trip_duration, transfer_duration, day
    Retruns a dataframe containing the parameters
    """

    dict_para = {}

    # Building a dict of paramaters structured like dict[nae_of_para] = list() (to use pd from_dict method)
    
    dict_para['origin'] = [input("Travel origin: ")]
    dict_para['destination'] = [input("Travel destination: ")]
    dict_para['day'] = [int(input("Day of the trip (YYYYMMDD e.g. 20180301 for March 1st, 2018): "))]
    dict_para['hour'] = [int(input("Hour of the trip: "))]
    dict_para['type_hour'] = [input("Departure hour or arrival hour (d/a): ")]

    dict_para['trip_duration'] = [int(input("Maximum duration of the travel (in minutes): "))]
    dict_para['transfer_duration'] = [int(input("Maximum duration of the transfer (in minutes): "))]

    df_para = pd.DataFrame.from_dict(dict_para)
    df_para.to_sql(name = 'parameters', 
            con = engine, if_exists='replace')

    return df_para




def iti_calcul(engine, file):
    """
    This function executes the sql queries that return the possible itineraries
    for the desired journey
    """

    with engine.connect() as con:

        with open(file,'r') as f:

            # Deletion of comments in the sql queries file via re.sub
            # Deletion of EOL, tabs 
            # Split accoring to ; to have a list of queries

            lines = [re.sub('/\*.*\*/','',line).strip() for line in f.read().replace('\n',' ').replace('\t',' ').split(';')[:-1]]

            for query in lines:
                if query[:6] != 'SELECT': # if DROP, CREQTE or INSERT, query only executed
                    con.execute(query)
                else: # if a SELECT query, result of the query converted to df and printed
                    print(pd.read_sql_query(query, con))

    

def transit_import(chemin, dict_files, engine):
    """Returns a dictionnary with dataframes containing the 
    transit feed data describing the network.
    """

    df_base = {}

    for file, index in dict_files.items():
        df_base[file + "_df"] = pd.read_csv(chemin_data + "/" + file + ".txt",
                           delimiter = ',',
                           header = 0,
                           index_col = index)

        df_base[file + "_df"].to_sql(name = file, 
            con = engine, if_exists='replace')

    return df_base





# Creation/opening of the SQLite database
engine = sqlalchemy.create_engine('sqlite:///' + chemin + '/bart.db')

transit_import(chemin, transit_files(chemin_data), engine)


stop = False

while stop != True:

    df_para = ask_input(engine)

    print("Possible od between {} and {} on {}".format(
        df_para.loc[0,'origin'],
        df_para.loc[0,'destination'],
        df_para.loc[0,'day']))

    iti_calcul(engine, chemin + '/itinerary_queries.sql')


    tmp = input("Encore? (y/n)")
    if tmp == "n":
        stop = True
    


# Functions for downloading Reddit stuff from Pushshift

import pandas as pd
import sqlite3
import psaw
import os
import datetime

api = psaw.PushshiftAPI()

def get_comments(subreddit, n_comments, after):

    # Gets a certain number of comments from a subreddit
    # and returns them in a dataframe
    search = api.search_comments(
        subreddit = subreddit,
        sort = 'asc',
        sort_type = 'created_utc',
        filter=['author','body','subreddit',
                'score', 'level', 'submit_text',
                'parent_id', 'link_id','id'],
        after = after,
        limit = n_comments,
    )

    # Extract data as a list of dictionaries
    data = [entry.d_ for entry in search]

    # Return search results as a pandas DataFrame
    return pd.DataFrame(data)

def get_submissions(subreddit, n_submissions, after):

    # Gets a certain number of submissions froma subreddit
    # and returns them in a dataframe
    search = api.search_submissions(
        subreddit = subreddit,
        sort = 'asc',
        sort_type = 'created_utc',
        filter = ['id', 'url','author',
            'title', 'subreddit', 'score',
            'num_comments'],
        after = after,
        limit = n_submissions,
    )

    # Extract data as a list of dictionaries
    data = [entry.d_ for entry in search]

    # Return search results as a pandas DataFrame
    return pd.DataFrame(data)

def subreddit_comments(subreddit, chunk_size, after, data_path = '', verbose = True):

    # Downloads all comments from a subreddit
    # beginning from after

    after = after

    while True:

        print(chunk_size)

        df = get_comments(subreddit, chunk_size, after)

        start = df.iloc[0]['created_utc']
        end = df.iloc[-1]['created_utc']

        filename = data_path + subreddit + '_comments_' + str(start) + '_' + str(end) + '.json'
        df.to_json(filename, orient='records')

        after = end

        if verbose:
            print('Now at ' + str(datetime.datetime.utcfromtimestamp(after)))

        if len(df) < chunk_size:
            break

def subreddit_submissions(subreddit, chunk_size, after, data_path = '', verbose = True):

    # Downloads all submissions from a subreddit
    # beginning from after

    after = after

    while True:

        print(chunk_size)

        df = get_submissions(subreddit, chunk_size, after)

        start = df.iloc[0]['created_utc']
        end = df.iloc[-1]['created_utc']

        filename = data_path + subreddit + '_submissions_' + str(start) + '_' + str(end) + '.json'
        df.to_json(filename, orient='records')

        after = end

        if verbose:
            print('Now at ' + str(datetime.datetime.utcfromtimestamp(after)))

        if len(df) < chunk_size:
            break


def json_files_to_database(db_name: str, path: str = './'):
    """
    Function that puts all comments and submission files of in folder in one sqlite3 
    database containing two table "coments" and "submissions". If a database with the
    same name already exists, the old database is deleted first.
    :param db_name: Name of the database
    :param path: Path where the json files are and also outputdirectory for the new database
    """ 
    # Append the right ending for the directory path
    if not path.endswith('/'):
        path += '/' 
    # List of all json files       
    dir_list = [i for i in os.listdir(path) if i.endswith('.json')]

    # Append the right file extension
    if not db_name.endswith('.db'):
        db_name += '.db'

    # Delete the old data base, if it already exists
    if os.path.isfile(path + db_name):
        os.remove(path + db_name)

    # Build a connection to a new database.
    conn = sqlite3.connect(path + db_name)

    for entry in dir_list:       
        # Check if file contains 'comments' or 'submissions'
        if 'comments' in entry:
            table_name = 'comments'
        elif 'submissions' in entry:
            table_name = 'submissions'
        else:
            continue
        
        # Load data
        df = pd.read_json(path + entry)
        
        # Store in sqlite data base
        df.to_sql(
            name=table_name,
            con=conn,
            if_exists='append',
            index=False,
        )

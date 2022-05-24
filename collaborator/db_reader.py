from re import T
import pandas as pd
from pymongo import MongoClient



def connect_mongo_by_params(host, port, username, password, database_name):
    """ A util for making a connection to mongo """

    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, database_name)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)

    return conn[database_name]

def connect_mongo_by_uri(mongo_uri, database_name):
    conn = MongoClient(mongo_uri)
    return conn[database_name]

def read_mongo_by_query(uri, database_name, collection, query={}, host='localhost', port=27017, username=None, password=None, no_id=True):
    """ Read from Mongo and Store into DataFrame """
    try:
        # Connect to MongoDB
        if uri:
            db = connect_mongo_by_uri(uri, database_name)
        else:
            db = connect_mongo_by_params(host=host, port=port, username=username, password=password, database_name=database_name)
        
        # Make a query to the specific DB and Collection
        cursor = db[collection].find(query)

        # Expand the cursor and construct the DataFrame
        df =  pd.DataFrame(list(cursor))

        # Delete the _id
        if no_id:
            del df['_id']

        return df
    except Exception as e:
        print(f' read_mongo_by_query exception: {e}')
        return 

def read_mongo_collection(uri:str, database_name:str, collection_name:str) -> pd.DataFrame:
    try:
        db = connect_mongo_by_uri(uri, database_name)
        df = pd.DataFrame.from_records(db[collection_name].find()) 
        return df
    except Exception as e:
        print(f' read_mongo_collection exception: {e}')
        return 'error'

def write_mongo_collection(uri:str, database_name:str, collection_name:str, df:pd.DataFrame) -> bool:
    try:
        db = connect_mongo_by_uri(uri, database_name)
        db[collection_name].insert_many(df.to_dict('records'))
        return 'success'
    except Exception as e:
        print(f' write_mongo_collection exception: {e}')
        return 'error'
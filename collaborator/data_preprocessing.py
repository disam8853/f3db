import pymongo
import pandas as pd
from time import sleep
from f3db_pipeline import get_max_surrogate_number, generate_collection_version, compare_collection_version, build_data_node, build_pipeline
from dag import DAG
DATA_FOLDER = "./DATA_FOLDER/"

def basic_data_transform(df:pd.DataFrame) -> pd.DataFrame:

    return df

def long_data_transform(dag:DAG, df:pd.DataFrame, collection_name:str, pipeline_id:str, pipeline:list, who:str) -> pd.DataFrame:
    
    """
    get all data node, compare collection_name

    if no exist data with same collection_name:
        create data node with collection_name + surrogate_id

    if exist data with same collection_name, find data node, compare two dataset:
        if same:
            get src_id
        if not same:
            create data node with collection + surrogate_id
    

    """
    # get all data node, compare collection_name
    current_data_version = get_max_surrogate_number(DATA_FOLDER, collection_name)
    
    # if no exist data with same collection_name:
    if current_data_version == -1:
        # TODO
        build_data_node()

    # if exist data with same collection_name, find data node, compare two dataset:
    else:
        new_collection_version = generate_collection_version(df)
        current_node_id = collection_name + str(current_data_version)
        current_collection_version = dag.get_node_attr(current_node_id)['collection_version']

        # if same: get src_id
        if compare_collection_version(new_collection_version, current_collection_version):
            src_id = current_node_id
        # if not same: create data node with collection + surrogate_id
        else:
            src_id = collection_name + str(current_data_version + 1)
            # TODO
            build_data_node()
    
        

    # parse: pipeline dict to real pipline (chung)
    
    # do pipeline (chung)
    
    for i in range(5):
        print(f'sleep {i}')
        sleep(1)

    # TODO:
    # send post request to  localhost:8999/model/data
    return df


from utils import pickle_encode
import pandas as pd
from f3db_pipeline import build_child_data_node, get_max_surrogate_number, generate_collection_version, compare_collection_version, build_root_data_node
from dag import DAG
import requests
import json
from environs import Env
from parse import parse

env = Env()
env.read_env()
DATA_FOLDER = env('DATA_FOLDER')
GLOBAL_SERVER_URL=env('GLOBAL_SERVER_URL')


def basic_data_transform(df:pd.DataFrame) -> pd.DataFrame:

    return df

def long_data_transform(lock, dag:DAG, df:pd.DataFrame, collection_name:str, pipeline_id:str, pipeline:dict) -> pd.DataFrame:
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
    last_data_version = get_max_surrogate_number(DATA_FOLDER, collection_name)
    new_collection_version = generate_collection_version(df)
    # if no exist data with same collection_name:
    if last_data_version == -1:
        new_node_id = "_".join([collection_name, '0'])
        src_id = build_root_data_node(dag, df, collection_name, new_collection_version , pipeline_id, new_node_id=new_node_id)

    # if exist data with same collection_name, find data node, compare two dataset:
    else:
        last_node_id = "_".join([collection_name, str(last_data_version)])
        last_node = dag.get_node_attr(last_node_id)
        last_collection_version = last_node['collection_version']
        # if same: get src_id
        if compare_collection_version(new_collection_version, last_collection_version):
            src_id = last_node_id
        # if not same: create data node with collection + surrogate_id
        else:
            new_src_id = "_".join([collection_name, str(last_data_version + 1)]) 
            src_id = build_child_data_node(dag, df, collection_name, new_collection_version, last_node_id, new_src_id)
        

    # parse: pipeline dict to real pipline (chung)
    

    # do pipeline (chung)



    print(dag.nodes)
    print(dag.roots)
    import time
    for i in range(5):
        print(i)
        time.sleep(1)
    # send pipeline_id, json dag, dataframe to global-server
    url = f'{GLOBAL_SERVER_URL}/pipeline/merge'
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    data = {
        "pipeline_id": pipeline_id,
        "dag_json": dag.get_dict_graph(),
        "dataframe": pickle_encode(df)
    }

    
    r = requests.post(url, headers=headers, data=json.dumps(data))
    print("response: ", r.text)

    return 

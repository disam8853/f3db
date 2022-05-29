
from utils import pickle_encode
import pandas as pd
from f3db_pipeline import generate_node
from f3db_pipeline import save_data, get_max_surrogate_number, generate_collection_version, compare_collection_version, build_root_data_node
from dag import DAG
import requests
import json

DATA_FOLDER = "./DATA_FOLDER/"
from environs import Env
env = Env()
env.read_env()
GLOBAL_SERVER_URL=env('GLOBAL_SERVER_URL')

# read eisting DAG

def basic_data_transform(df:pd.DataFrame) -> pd.DataFrame:

    return df

def long_data_transform(lock, dag:DAG, df:pd.DataFrame, collection_name:str, pipeline_id:str, pipeline:list) -> pd.DataFrame:
    
    """
    bobodddd
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
    new_collection_version = generate_collection_version(df)
    # if no exist data with same collection_name:
    if current_data_version == -1:
        print('create root data node')
        new_node_id = "_".join([collection_name, '0'])
        src_id = build_root_data_node(dag, df, collection_name, new_collection_version , pipeline_id, new_node_id=new_node_id)

    # if exist data with same collection_name, find data node, compare two dataset:
    else:
        print('exist root data node')
        current_node_id = "_".join([collection_name, str(current_data_version)])
        exist_node = dag.get_node_attr(current_node_id)
        current_collection_version = exist_node['collection_version']
        # if same: get src_id
        print(new_collection_version, current_collection_version)
        if compare_collection_version(new_collection_version, current_collection_version):
            print('re-use node')
            src_id = current_node_id
        # if not same: create data node with collection + surrogate_id
        else:
            print('add node')
            new_src_id = "_".join([collection_name, str(current_data_version + 1)])
            src_id, node_info, node_filepath = generate_node(env('WHO'), env('USER'), collection_name, new_collection_version, type='data', node_id=new_src_id)
            save_data(node_filepath, df)
            dag.add_node(src_id, **node_info)
            dag.add_edge(current_node_id, src_id)
    
        

    # parse: pipeline dict to real pipline (chung)
    

    # do pipeline (chung)



    print(dag.nodes)
    print(dag.roots)
    

    # TODO:
    # send post request to  localhost:8999/model/data
    # pipeline_id, json dag, dataframe
    url = f'{GLOBAL_SERVER_URL}/pipeline/merge'
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    data = {
        "pipeline_id": pipeline_id,
        "dag_json": dag.get_dict_graph(),
        "dataframe": pickle_encode(df)
    }

    r = requests.post(url, headers=headers, data=json.dumps(data))
    print("response: ", r)
    return 

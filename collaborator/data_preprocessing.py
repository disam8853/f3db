from asyncio.windows_events import NULL
from utils import pickle_encode
import pandas as pd
from f3db_pipeline import build_child_data_node, get_max_surrogate_number, generate_collection_version, compare_collection_version, build_root_data_node, build_pipeline
from dag import DAG
import requests
import json
from environs import Env
from parse import parse, parse_param
from sklearn.model_selection import train_test_split


env = Env()
env.read_env()
DATA_FOLDER = env('DATA_FOLDER')
GLOBAL_SERVER_URL=env('GLOBAL_SERVER_URL')


def basic_data_transform(df:pd.DataFrame) -> pd.DataFrame:

    return df

def long_data_transform(lock, dag:DAG, df:pd.DataFrame, collection_name:str, pipeline_id:str, pipeline:dict) -> pd.DataFrame:
    lock.acquire()
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
    parsed_pipeline = parse(pipeline)

    # do pipeline (chung)
    first_pipe = NULL
    pipe_param_string = parse_param(pipeline,'global-server')
    # print(pipe_param_string)
    for sub_pipeline in parsed_pipeline:
        sub_pipeline_param_list = pipe_param_string[parsed_pipeline.index(sub_pipeline)]

       
        if(parsed_pipeline.index(sub_pipeline) == 0):
            first_pipe = build_pipeline(dag,1,sub_pipeline, first_pipe=first_pipe,param_list = sub_pipeline_param_list)
        else:
            if(sub_pipeline == 'train_test_split'):
                X = first_pipe[first_pipe.columns[0:-1]]
                y  = first_pipe[first_pipe.columns[-1]]
                X_train,X_test,y_trian,y_test = train_test_split(X,y, test_size=0.2, random_state=42)
                
                X_train = pd.DataFrame(X_train)
                X_test = pd.DataFrame(X_test)
                first_pipe = pd.concat([X_train,X_test],axis =0)
                # print('tttt', first_pipe)
            else:
                first_pipe = build_pipeline(dag,1,sub_pipeline, first_pipe=first_pipe,param_list = sub_pipeline_param_list)


    print(dag.nodes)
    print(dag.roots)
    import time
    for i in range(3):
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
    lock.release()
    return 

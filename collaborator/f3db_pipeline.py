import os
import pickle
from platform import node
from random import choice, randrange
import joblib
import networkx as nx
import numpy as np
import pandas as pd
from environs import Env
from joblib import dump, load
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.datasets import make_classification
from sklearn.decomposition import PCA
from sklearn.ensemble import *
from sklearn.linear_model import *
from sklearn.model_selection import *
from sklearn.model_selection import train_test_split
from sklearn.neighbors import *
from sklearn.neighbors import NearestNeighbors
# from bobo_pipeline import Pipeline
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import *
from sklearn.preprocessing import FunctionTransformer, StandardScaler
from sklearn.svm import SVC
from sklearn.impute import SimpleImputer
from sklearn.utils.validation import check_is_fitted

from dag import DAG
from parse import parse, parse_param
from utils import current_date, current_time

DATA_FOLDER = "./DATA_FOLDER/"
try:
    os.mkdir(DATA_FOLDER)
except:
    pass
WHO = 'global-server'
USER = "bobo"
TAG = "default-tag"
COLLECTION = "blood pressure"
COLLECTION_VERSION = 0
EXP_NUM = 0
env = Env()
env.read_env()

XHEADER = ['HBP_d_all_systolic','HBP_d_AM_systolic','HBP_d_PM_systolic','HBP_d_all_diastolic','HBP_d_AM_diastolic','HBP_d_PM_diastolic','HBP_d_systolic_D1_AM1','AGE','aspirin']
YHEADER = 'CV'

def compare_collection_version(version1, version2):
    if version1 == version2:
        return True
    return False

def generate_collection_version(dataframe):
    h = pd.util.hash_pandas_object
    hash = h(dataframe).sum()
    return str(hash)


def build_root_data_node(dag, dataframe, collection_name, collection_version, pipeline_id, experiment_number, new_node_id):
    x_header = ",".join(XHEADER)
    
    node_id, node_info, node_filepath = generate_node(
            who=env('WHO'), user=env('USER'), collection=collection_name, collection_version=collection_version, pipeline_id=pipeline_id, experiment_number=experiment_number, x_headers=x_header, y_headers=YHEADER, tag=TAG, type='root', folder=DATA_FOLDER, node_id=new_node_id)
    
    save_data(node_filepath, dataframe)
    dag.add_node(node_id, **node_info)

    return node_id

def build_child_data_node(dag, dataframe, collection_name, collection_version, experiment_number, src_id, new_src_id=""):
    
    new_src_id, node_info, node_filepath = generate_node(
        env('WHO'), env('USER'), collection_name, collection_version, experiment_number, type='data', node_id=new_src_id, src_id=src_id, dag=dag)
    save_data(node_filepath, dataframe)
    dag.add_node(src_id, **node_info)
    dag.add_edge(src_id, new_src_id)

    return new_src_id

# TODO: add funcion to build model node
def build_model_node(dag):
    pass

def get_max_surrogate_number(path, prefix) -> int:
    same_files = []
    for file in os.listdir(path):
        if file.startswith(prefix):
            same_files.append(file)

    version_nums = [ int(x.split("_")[-1].split(".")[0]) for x in same_files]

    if version_nums == []:
        return -1
    return max(version_nums)
    

def generate_node_id(type="", who="", user="", tag="", experiment_number="") -> str:
    date = current_date()
    node_id = '_'.join([type, who, user, date, experiment_number])
    version_num = "_" + str(get_max_surrogate_number(DATA_FOLDER, node_id) + 1)
    node_id += version_num
    return node_id

def generate_node_filepath(folder, node_id, type):
    if type == "model":
        format = '.joblib'
    else:
        format = '.csv'

    return os.path.join(folder, node_id + format)

def generate_node(who, user, collection="", collection_version="", experiment_number=EXP_NUM, pipeline_id="", tag=TAG, type='data', metrics="", operation="", folder=DATA_FOLDER, x_headers="", y_headers="", node_id="", src_id="", dag=None):
    if node_id == "":
        node_id = generate_node_id(type, who, user, tag, experiment_number)

    node_filepath = generate_node_filepath(folder, node_id, type)
    print(f"node_id : {node_id}")
    if src_id == "" and dag is None:
        node_info = {
                    'node_id': node_id,
                    'who': who,
                    'user': user,
                    'date': current_date(),
                    'time': current_time(),
                    'collection': collection,
                    'collection_version': collection_version,
                    'experiment_number': experiment_number,
                    'tag': tag,
                    'type': type, # data or model
                    'pipeline_id': pipeline_id, # comma seperate, global server has 1 id, collab has many id
                    'operation': operation, # comma seperate
                    'filepath': node_filepath,
                    'metrics': metrics,
                    'x_headers': x_headers, # comma seperate
                    'y_headers': y_headers,
                }
    else:
        node_info = dag.get_node_attr(src_id)
        node_info['node_id'] = node_id
        node_info['date'] = current_date()
        node_info['time'] = current_time()
        node_info['tag'] = tag
        node_info['type'] = type 
        node_info['experiment_number'] = experiment_number
        node_info['operation'] = operation
        node_info['filepath'] = node_filepath
        node_info['metrics'] = metrics
    
    return node_id, node_info, node_filepath



def build_pipeline(dag, src_id, ops, param_list, x_header=XHEADER,y_header=YHEADER,experiment_number=EXP_NUM, tag=TAG):

    data_path = dag.get_node_attr(src_id)['filepath']
    dataframe = pd.read_csv(data_path)
    X = dataframe.drop(y_header, axis=1, errors="ignore") # TODO: change header to number or catch exception or record the header change in pipeline (recommand)
    X = X.drop('_id', axis=1, errors="ignore")

    y =  dataframe[y_header] # sklearn will drop header after some data transformation
    pipe_string = parse_pipe_to_string(ops)
    print('HHHHHHHH',pipe_string)
    
    pipe = Pipeline(ops)

    # is model
    if (ops[-1][0] == 'model'):
        print('is model')

        if(len(ops) >  1):
            trans_pipe = Pipeline(ops[:-1])
            pipe.set_params(**param_list)
            trans_data = trans_pipe.fit_transform(X,y)
            X = pd.DataFrame(trans_data, columns = x_header)


        node_id, node_info, node_filepath = generate_node(
            who=env('WHO'), user=env('USER'), experiment_number=experiment_number, tag=TAG, type='model', src_id=src_id, dag=dag)
        pipe.set_params(**param_list)
        
        save_model(node_filepath, pipe.steps[-1][1].fit(X,y))
        dag.add_node(node_id, **node_info)
        dag.add_edge(src_id, node_id)

        
        return node_id

    # is data
    else:
    
        node_id, node_info, node_filepath = generate_node(
            who=env('WHO'), user=env('USER'), experiment_number=experiment_number, tag=TAG, type='data', src_id=src_id, dag=dag)
        pipe.set_params(**param_list)
        trans_data = pipe.fit_transform(X,y)
        trans_pd_data = pd.DataFrame(trans_data, columns = x_header) # TODO: if columns change, detect and do sth
        
        y = pd.DataFrame(y)
        final_data = pd.concat([trans_pd_data,y],axis=1)
        
        save_data(node_filepath, final_data)
        dag.add_node(node_id, **node_info)
        dag.add_edge(src_id, node_id)
        # print('EWW',final_data)
        return node_id

def save_data(filepath, trans_data) -> None:
    trans_data.to_csv(filepath, index = False)
    # dag.add_node(id, path=path)
    # dag.add_edge(rid , id

def save_model(filepath, clf) -> None:
    dump(clf, filepath) 

def read_model():
    clf = load('DATA_FOLDER/model_global-server_bobo_default-tag_2022-05-28_0.joblib')

def parse_pipe_to_string(ops):
    
    op_str = ""
    for step in ops:
        item = str(step[1])
        if(ops.index(step) == 0):
            op_str += item
        else:
            op_str += str(',') + item
    return op_str

if __name__ == "__main__":
    op_data = [('pca', PCA()), ('scaler', StandardScaler())]
    dag = DAG(nx.MultiDiGraph())
    dag = build_pipeline(dag, 1, op_data)

    op_model = [('pca', PCA()), ('scaler', StandardScaler()), ('svc', SVC())]
    dag = DAG(nx.MultiDiGraph())
    dag = build_pipeline(dag, 1, op_model)
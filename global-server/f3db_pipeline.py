
from asyncio.windows_events import NULL
from email import header
import os
import pickle
from platform import node
from random import choice, randrange
import pandas as pd
import networkx as nx
import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.datasets import make_classification
from sklearn.decomposition import PCA
from sklearn.model_selection import train_test_split
from sklearn.neighbors import NearestNeighbors
# from bobo_pipeline import Pipeline
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler
from sklearn.svm import SVC
from sklearn.utils.validation import check_is_fitted

from dag import DAG
from utils import current_date, current_time
from joblib import dump, load

from parse import parse, parse_param

"""
def psudocode():


    x, y = read_data(rid)
    op = [('pca', PCA()), ('scaler', StandardScaler())]
    pipe = Pipeline(op)
    # The pipeline can be used as any other estimator
    # and avoids leaking the test set into the train set
    pipe.fit(X_train, y_train)
    if op[-1] is model:
        x = pipe.model_weight
        id, path = save_model()
    else:
        x = pipe.transform_data
        id, path = save_data()
    
    dag.add_node(id, path=path)
    dag.add_edge(rid , id)

""" 

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

def compare_collection_version(version1, version2):
    if version1 == version2:
        return True
    return False

def generate_collection_version(dataframe):
    h = pd.util.hash_pandas_object
    hash = h(dataframe).sum()
    return hash


def build_data_node(dag, dataframe, collection="", collection_version="", experiment_number="", tag=""):


    node_id, node_info, node_filepath = generate_node(
            who=WHO, user=USER, collection=COLLECTION, collection_version=COLLECTION_VERSION, experiment_number=EXP_NUM, tag=TAG, type='data', folder=DATA_FOLDER)
    save_data(node_filepath, dataframe)
    dag.add_node(node_id, **node_info)

    return dag


def get_surrogate_number(path, prefix) -> str:
    same_files = []
    for file in os.listdir(path):
        if file.startswith(prefix):
            same_files.append(file)

    version_nums = [ int(x.split("_")[-1].split(".")[0]) for x in same_files]

    if version_nums == []:
        return str(0)
    return str(max(version_nums) + 1)
    

def generate_node_id(type="", who="", user="", tag="") -> str:
    date = current_date()
    node_id = '_'.join([type, who, user, tag, date])
    version_num = "_" + get_surrogate_number(DATA_FOLDER, node_id)
    node_id += version_num
    return node_id

def generate_node_filepath(folder, node_id, type):
    if type == "model":
        format = '.joblib'
    if type == "data":
        format = '.csv'

    return os.path.join(folder, node_id + format)
    
def generate_node(who, user, collection, collection_version, experiment_number, tag, type, folder):
    node_id = generate_node_id(type, who, user, tag)
    node_filepath = generate_node_filepath(folder, node_id, type)
    node_info = {
                'who': who,
                'user': user,
                'date': current_date(),
                'time': current_time(),
                'collection': collection,
                'collection_version': collection_version,
                'experiment_number': experiment_number,
                'tag': tag,
                'type': type, # data or model
                'filepath': node_filepath
            }
    
    return node_id, node_info, node_filepath


def build_pipeline(dag, src_id, ops, first_pipe, param_list, experiment_number=EXP_NUM, tag=TAG  ):
    if(first_pipe is NULL):
        X = np.array([[-1, -1], [-2, -1], [1, 1], [2, 1]])
        y = np.array([1, 1, 2, 2])
    else:
        X = first_pipe[first_pipe.columns[0:-1]]
        y  = first_pipe[first_pipe.columns[-1]]
   
    pipe_string = parse_pipe_to_string(ops)
    
    pipe = Pipeline(ops)
    # is model
    if (ops[-1][0] == 'model'):
        print('is model')
        node_id, node_info, node_filepath = generate_node(
            who=WHO, user=USER, collection=COLLECTION, collection_version=COLLECTION_VERSION, experiment_number=EXP_NUM, tag=TAG, type='model', folder=DATA_FOLDER)
        pipe.set_params(**param_list)
        print('model',pipe.get_params())
        
        save_model(node_filepath, pipe.steps[-1][1].fit(X,y))
        dag.add_node(node_id, **node_info)
        # final_data = 
        # dag.add_edge(src_id, node_id)
        return final_data
    # is data
    else:
    
        node_id, node_info, node_filepath = generate_node(
            who=WHO, user=USER, collection=COLLECTION, collection_version=COLLECTION_VERSION, experiment_number=EXP_NUM, tag=TAG, type='data', folder=DATA_FOLDER)
        
        print('is data')
        # print(node_filepath)
        pipe.set_params(**param_list)
        print(pipe.get_params())
        trans_data = pipe.fit_transform(X,y)
        trans_pd_data = pd.DataFrame(trans_data)
        y = pd.DataFrame(y)
        final_data = pd.concat([trans_pd_data,y],axis=1)
        final_data.columns = [np.arange(0,final_data.shape[1])]
        save_data(node_filepath, final_data)
        dag.add_node(node_id, **node_info)
        # dag.add_edge(src_id, node_id)
        # print('EWW',final_data)
        return final_data

def save_data(filepath, trans_data):
    trans_data.to_csv(filepath, index = False)
    # dag.add_node(id, path=path)
    # dag.add_edge(rid , id

def save_model(filepath, clf):
    dump(clf, filepath) 

def read_model():
    clf = load('DATA_FOLDER/model_global-server_bobo_default-tag_2022-05-28_0.joblib')
    # print(clf.classes_)

def parse_pipe_to_string(ops):
    op_str = ""
    for step in ops:
        item = str(step[1])
        if(ops.index(step) == 0):
            op_str += item
        else:
            op_str += str(',') + item
    # print(op_str)
    return op_str

if __name__ == "__main__":
    # op_data = [('pca', PCA()), ('scaler', StandardScaler())]
    # dag = DAG(nx.MultiDiGraph())
    # build_pipeline(dag, 1, op_data)
    # op_data = [('pca', PCA()), ('scaler', StandardScaler())]
    # dag = DAG(nx.MultiDiGraph())
    # dag = build_pipeline(dag, 1, op_data)
    # print(dag.nodes_info)

    # op_model = [('pca', PCA()), ('scaler', StandardScaler()), ('svc', SVC())]
    raw_pipe = {
    "collaborator": [
        {
            "name": "StandardScaler",
            "parameter": []
        },
        {
            "name": "SaveData",
            "parameter": []
        },
        {
            "name": "StandardScaler",
            "parameter": []
        }
    ],
    "global-server": [
        {
            "name": "StandardScaler",
            "parameter": []
        },

        {
            "name": "PCA",
            "parameter": [
                {

                    "random_state":42
                }   
            ]
        },
        {
            "name": "SaveData",
            "parameter": []
        },
        {
            "name": "StandardScaler",
            "parameter": []
        },
        {
            "name": "SVC",
            "parameter": [
                {
                    "kernel":"linear",
                    "gamma":"auto" ,
                    "random_state":42
                }   
            ]   
        }
    ]
}
    main_pipeline = parse(raw_pipe, 'global-server')
    # print(main_pipeline)
    dag = DAG(nx.MultiDiGraph())
    first_pipe = NULL
    pipe_param_string = parse_param(raw_pipe,'global-server')
    # print(pipe_param_string)
    for sub_pipeline in main_pipeline:
        sub_pipeline_param_list = pipe_param_string[main_pipeline.index(sub_pipeline)]

        print('--------------------',sub_pipeline,'--------------------')
        print('xxxxxxxxxxxxxxxxxxxx',sub_pipeline_param_list,'xxxxxxxxxxxxxxxxxxxx')
        if(main_pipeline.index(sub_pipeline) == 0):
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
    # build_pipeline(dag, 1, op_str[0])
        
    # print(dag.nodes_info)

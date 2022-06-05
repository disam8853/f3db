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
from sklearn.preprocessing import (FunctionTransformer, MinMaxScaler,
                                   StandardScaler)
from sklearn.svm import *
from sklearn.svm import SVC
from sklearn.utils.validation import check_is_fitted

from dag import DAG

from utils import current_date, current_time, predict_and_convert_to_metric_str
from joblib import dump, load
from environs import Env
from parse import check_fitted, parse, parse_global_param

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
    
    node_id, node_info, node_filepath = generate_node(
            who=env('WHO'), user=env('USER'), collection=collection_name, collection_version=collection_version, pipeline_id=pipeline_id, experiment_number=experiment_number, tag=TAG, type='root', folder=DATA_FOLDER, node_id=new_node_id)
    
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

def generate_node(who, user, collection="", collection_version="", experiment_number=EXP_NUM, pipeline_id="", tag=TAG, type='data', metrics="", operation="", folder=DATA_FOLDER, XHEADER="", YHEADER="", node_id="", src_id="", dag=None):
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
                    'XHEADER': XHEADER, # comma seperate
                    'YHEADER': YHEADER,
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
    # print('Origin',X)
    pipe_string = parse_pipe_to_string(ops)
    
    pipe = Pipeline(ops)
    # print('opssss:',ops)
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

                # test model
        # loaded_model = joblib.load('model.joblib') # TODO: model path need to be modified
        y_pred = pipe.predict(X)
        # accuracy = 
        test_results = predict_and_convert_to_metric_str(y,y_pred)

        print('testing data result : ', test_results)
        return node_id

    # is data
    else:
    
        node_id, node_info, node_filepath = generate_node(
            who=env('WHO'), user=env('USER'), experiment_number=experiment_number, tag=TAG, type='data', src_id=src_id, dag=dag)
        print('is data')
        pipe.set_params(**param_list)
        trans_data = pipe.fit_transform(X,y)
        
        trans_pd_data = pd.DataFrame(trans_data, columns = x_header) # TODO: if columns change, detect and do sth

        
        y = pd.DataFrame(y)
        final_data = pd.concat([trans_pd_data,y],axis=1)
        # print(final_data.shape)
        # final_data.columns = x_header.append(y_header)
        
        # print('after transform',final_data)
        # print('stephanie', node_filepath)
        save_data(node_filepath, final_data)
        dag.add_node(node_id, **node_info)
        dag.add_edge(src_id, node_id)
        # print('EWW',final_data)
        return node_id

def run_pipeline(dag, src_id, experiment_number, pipeline, from_begin=False):
# def run_pipeline(rawpipe):
    # 進到split 不進行save data
    # 後面直接併成一整條pipeline


    # # do pipeline (chung)
    pipe_param_string = []
    if from_begin == True:
        pipe_param_string += parse_global_param(pipeline, 'collaborator')
    pipe_param_string += parse_global_param(pipeline, 'global-server')
    
    parsed_pipeline = []
    if from_begin == True:
        parsed_pipeline += parse_global_pipeline(pipeline, 'collaborator')
    parsed_pipeline += parse_global_pipeline(pipeline, "global-server")
    
    # parsed_pipeline = parse_global_pipeline(raw_pipe, "global-server")
    print('PipeParam',pipe_param_string,parsed_pipeline)

    for sub_pipeline_idx in range(len(parsed_pipeline)):
        sub_pipeline = parsed_pipeline[sub_pipeline_idx]
        sub_pipeline_param_list = pipe_param_string[sub_pipeline_idx]
        if(sub_pipeline == 'train_test_split'):
            src_id = train_test_split_training(dag, parsed_pipeline[sub_pipeline_idx+1],src_id, sub_pipeline_param_list)
            break # train test split 之後便直接後面剩下的pipeline 直到 model train完成
        else:
            # train_test_split 之前的node要儲存資料
            # sub_pipeline_param_list = pipe_param_string[parsed_pipeline.index(sub_pipeline)]
            src_id = build_pipeline(dag, src_id, sub_pipeline, param_list=sub_pipeline_param_list, experiment_number=experiment_number)

    return parsed_pipeline


def parse_global_pipeline(raw_pipe_data,character):
    final_pipeline = []
    sub_pipeline = []
    pipe = raw_pipe_data[character]
    for idx in range(len(pipe)):
        # print(pipe[idx])
        if(pipe[idx]['name'] != 'train_test_split' and pipe[idx]['name'] != 'SaveData'):
            strp = pipe[idx]['name']+'()'
            if check_fitted(eval(strp)):
                sub_pipeline.append(('model',eval(strp)))
            else:
                sub_pipeline.append((pipe[idx]['name'],eval(strp)))
        # elif(pipe[idx]['name'] == 'train_test_split'):
        elif (pipe[idx]['name'] == 'train_test_split' and (idx == 0)):
            # final_pipeline.append(sub_pipeline)
            # sub_pipeline = []
            final_pipeline.append('train_test_split')
        elif(pipe[idx]['name'] == 'train_test_split' and (idx != 0)):
            final_pipeline.append(sub_pipeline)
            sub_pipeline = []
            final_pipeline.append('train_test_split')
        elif(pipe[idx]['name'] == 'SaveData'):
            final_pipeline.append(sub_pipeline)
            sub_pipeline = []
   

    final_pipeline.append(sub_pipeline)
    return final_pipeline


def train_test_split_training(dag, model_pipeline, src_id, param_list):  # TODO : Model parameter (train_test_split)
    print("start train test split")
    data_path = dag.get_node_attr(src_id)['filepath']
    experiment_number = dag.get_node_attr(src_id)['experiment_number']

    data = pd.read_csv(data_path)


    X = data[XHEADER]
    y = data[YHEADER]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42, stratify=y)

    pipe = Pipeline(model_pipeline)

    # before model
    if(len(model_pipeline) >  1):
        trans_pipe = Pipeline(model_pipeline[:-1])
        
        trans_data = trans_pipe.fit_transform(X_train,y_train)
        X_train = pd.DataFrame(trans_data, columns = XHEADER)
    pipe.set_params(**param_list)
    # print('pppppp', param_list)
    clf = pipe.steps[-1][1].fit(X_train, y_train)
    # test model
    y_pred = pipe.predict(X_test)
    # evaluation
    test_results = predict_and_convert_to_metric_str(y_test,y_pred)

    # add to dag
    node_id, node_info, node_filepath = generate_node(
            who=env('WHO'), user=env('USER'), experiment_number=experiment_number, metrics=test_results, tag=TAG, type='model', src_id=src_id, dag=dag)
    
    save_model(node_filepath, clf)
    dag.add_node(node_id, **node_info)
    dag.add_edge(src_id, node_id)


    print('testing data result : ', test_results)


     


def save_data(filepath, trans_data) -> None:
    trans_data.to_csv(filepath, index = False)
    # dag.add_node(id, path=path)
    # dag.add_edge(rid , id

def save_model(filepath, clf) -> None:
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
    op_data = [('pca', PCA()), ('scaler', StandardScaler())]
    dag = DAG(nx.MultiDiGraph())
    dag = build_pipeline(dag, 1, op_data)
    print(dag.nodes_info)

    op_model = [('pca', PCA()), ('scaler', StandardScaler()), ('svc', SVC())]
    dag = DAG(nx.MultiDiGraph())
    dag = build_pipeline(dag, 1, op_model)
    print(dag.nodes_info)

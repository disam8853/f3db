
import os
import pickle
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

def get_surrogate_number(path, prefix) -> str:
    same_files = []
    for file in os.listdir(path):
        if file.startswith(prefix):
            same_files.append(file)

    version_nums = [ int(x.split("_")[-1].split(".")[0]) for x in same_files]

    if version_nums == []:
        return str(0)
    return str(max(version_nums) + 1)
    


def generate_file_path(path="", mode="", who="", user="", tag="") -> str:
    date = current_date()
    filepath = '_'.join([mode, who, user, tag, date])
    version_num = "_" + get_surrogate_number(DATA_FOLDER, filepath)
    filepath += version_num
    filepath = os.path.join(path, filepath)
    return filepath

def build_pipeline(dag, src_id, ops):
    def check_fitted(clf): 
        return hasattr(clf, "classes_")
    
    # read data
    X, y = make_classification(random_state=0)

    pipe = Pipeline(ops)
    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=0)
    pipe.fit(X_train, y_train)

    # is model
    if check_fitted(pipe.steps[-1][1]):
        print('is model')
        filepath = generate_file_path(DATA_FOLDER, 'model', WHO, USER, TAG)
        print(filepath)
        save_model(filepath, pipe.steps[-1][1])
        # dag.add_node()
        # dag.add_edge(src_id, node_id)
    # is data
    else:
        print('is data')
        filepath = generate_file_path(DATA_FOLDER, 'data', WHO, USER, TAG)
        print(filepath)
        trans_data = pipe.fit_transform(X, y)
        tras_pd_data = pd.DataFrame(trans_data)
        id = save_data(filepath, tras_pd_data)
        # dag.add_node()
        # dag.add_edge(src_id, node_id)



def save_data(filepath, trans_data):
    trans_data.to_csv(filepath + '.csv', index = False)
    # dag.add_node(id, path=path)
    # dag.add_edge(rid , id

def save_model(filepath, clf):
    
    dump(clf, filepath+ '.joblib') 

def read_model():
    clf = load('DATA_FOLDER/model_global-server_bobo_default-tag_2022-05-28_0.joblib')
    print(clf.classes_)

if __name__ == "__main__":
    op_data = [('pca', PCA()), ('scaler', StandardScaler())]
    dag = DAG(nx.MultiDiGraph())
    build_pipeline(dag, 1, op_data)

    op_model = [('pca', PCA()), ('scaler', StandardScaler()), ('svc', SVC())]
    dag = DAG(nx.MultiDiGraph())
    build_pipeline(dag, 1, op_model)

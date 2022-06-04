import json
from tabnanny import check
from sklearn.preprocessing import FunctionTransformer, StandardScaler
import pip 
from sklearn.svm import SVC
from sklearn.decomposition import PCA
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import *
from sklearn.ensemble import *

def read_raw_pipe():
    with open('test.json') as f:
        data = json.load(f)
        # print(data)
        return data

def check_fitted(clf): 
    X = np.array([[-1, -1], [-2, -1], [1, 1], [2, 1]])
    y = np.array([1, 1, 2, 2])
    # print(clf,hasattr(clf, "predict"))
    return hasattr(clf, "predict")

def parse(raw_pipe_data, character):
    final_pipeline = []
    param_pipeline = []
    sub_pipeline = []
    pipe = raw_pipe_data[character]
    for idx in range(len(pipe)):
        # print(pipe[idx])
        if(pipe[idx]['name'] != 'SaveData'):
            strp = pipe[idx]['name']+'()'
            if check_fitted(eval(strp)):
                sub_pipeline.append(('model',eval(strp)))
            else:
                sub_pipeline.append((pipe[idx]['name'],eval(strp)))
        elif(pipe[idx]['name'] == 'SaveData'):
            final_pipeline.append(sub_pipeline)
            sub_pipeline = []
        else:
            final_pipeline.append(sub_pipeline)
            final_pipeline.append(pipe[idx]['name'])
            sub_pipeline = []      

    final_pipeline.append(sub_pipeline)
    return final_pipeline

def parse_param(raw_pipe_data, character):
    final_pipeline_param = []
    param_pipeline = {}
    # sub_pipeline = []
    pipe = raw_pipe_data[character]
    for idx in range(len(pipe)):
        # print('yeeeeeeeeeeeeeeeeeeeeee',pipe[idx])
        if(pipe[idx]['name'] != 'SaveData' and pipe[idx]['parameter']):
            
            # for param in pipe[idx]['parameter'][0]:
            #     newkey = pipe[idx]['name']+'__'+ param
            #     param_pipeline[newkey] = pipe[idx]['parameter'][0][param]


            strp = pipe[idx]['name']+'()'
            if check_fitted(eval(strp)):
                for param in pipe[idx]['parameter'][0]:
                    newkey = 'model__'+ param
                    param_pipeline[newkey] = pipe[idx]['parameter'][0][param]
            else:
                for param in pipe[idx]['parameter'][0]:
                    newkey = pipe[idx]['name']+'__'+ param
                    param_pipeline[newkey] = pipe[idx]['parameter'][0][param]

        elif(pipe[idx]['name'] != 'SaveData' and not pipe[idx]['parameter']):
            # print('NOT', pipe[idx]['name'])
            continue
        else:
            final_pipeline_param.append(param_pipeline)
            param_pipeline = {}
            continue

    # print(final)
    final_pipeline_param.append(param_pipeline)
    return final_pipeline_param
       
    
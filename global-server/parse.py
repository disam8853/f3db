import json
from tabnanny import check
from sklearn.preprocessing import FunctionTransformer, StandardScaler
import pip 
from sklearn.svm import SVC
from sklearn.decomposition import PCA
import numpy as np
from sklearn.linear_model import LinearRegression

def read_raw_pipe():
    with open('test.json') as f:
        data = json.load(f)
        # print(data)
        return data
def check_fitted(clf): 
    X = np.array([[-1, -1], [-2, -1], [1, 1], [2, 1]])
    y = np.array([1, 1, 2, 2])
    print(clf,hasattr(clf, "predict"))
    return hasattr(clf, "predict")
def parse(raw_pipe_data, character):
    final_pipeline = []
    sub_pipeline = []
    pipe = raw_pipe_data[character]
    for idx in range(len(pipe)):
        # print(pipe[idx])
        if(pipe[idx]['name'] != 'SaveData' and pipe[idx]['name'] != 'train_test_split'):
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

            
    
if __name__ == "__main__":

    raw_pipe_data = read_raw_pipe()
    final = parse(raw_pipe_data, 'global-server')
    print(final)
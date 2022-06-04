import base64
import pickle
import pandas as pd
import traceback
import sys
from sklearn.metrics import precision_recall_fscore_support, accuracy_score

def pickle_encode(df) -> str:
    pickled = pickle.dumps(df)
    pickled_b64 = base64.b64encode(pickled)
    return pickled_b64.decode('utf-8')

def pickle_decode(s) -> pd.DataFrame:
    p = pickle.loads(base64.b64decode(s.encode()))
    df = pd.DataFrame(p)
    return df

import time
def current_time():
    t = time.localtime()
    return time.strftime("%H:%M:%S", t)
    

def current_date():
    t = time.localtime()
    return time.strftime("%Y-%m-%d", t)

def getexception(e):
    error_class = e.__class__.__name__ #取得錯誤類型
    detail = e.args[0] #取得詳細內容
    cl, exc, tb = sys.exc_info() #取得Call Stack
    lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
    fileName = lastCallStack[0] #取得發生的檔案名稱
    lineNum = lastCallStack[1] #取得發生的行號
    funcName = lastCallStack[2] #取得發生的函數名稱
    
    errMsg = "File \"{}\", line {}, in {}: [{}] {}".format(fileName, lineNum, funcName, error_class, detail)
    print(errMsg)

def parse_condition_dict_to_tuple_list(condition_dict):
    condition = []
    for k, v in condition_dict.items():
        condition.append((k,v))
    return condition

def predict_and_convert_to_metric_str(y_test, y_pred):
    
    metrics = []
    metrics.append(accuracy_score(y_test, y_pred))
    metrics.extend(precision_recall_fscore_support(y_test, y_pred, beta=1, average='macro')[:-1])

    result_str = ",".join([str(x.round(3)) for x in metrics])
        
    return result_str


def metric_str_to_dict(result_str):
    return dict(zip(["accuracy","precision", "recall","f1"], map(lambda x: float(x), result_str.split(","))))
    
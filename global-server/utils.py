import base64
import pickle
import pandas as pd

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
    
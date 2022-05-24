from crypt import methods
from flask import Flask, request, abort, jsonify, Response
from db_reader import read_mongo_by_query, read_mongo_collection, write_mongo_collection
from data_preprocessing import basic_data_transform, long_data_transform
from environs import Env
from utils import pickle_encode
import pandas as pd
from multiprocessing import Process

app = Flask(__name__)

env = Env()
env.read_env()

@app.route("/", methods=['GET'])
def get():
    return 'OK'

@app.route("/basic_write", methods=['POST'])
def basic_write():
    req_json = request.get_json(force=True)
    collection = req_json["collection"]
    data = req_json["data"]
    df = pd.DataFrame(data=data)

    print(df)
    result = write_mongo_collection(env('MONGODB_URL'), database_name=env('db_name'), collection_name=collection, df=df)
    return jsonify(
        result=result
    ) 

@app.route("/basic_transform", methods=['POST'])
def basic_transform():
    req_json = request.get_json(force=True)
    collection = req_json["collection"]
    query = req_json["query"]
    if query:
        df = read_mongo_by_query(env('MONGODB_URL'), database_name=env('db_name'), collection_name=collection, query=query)
    else:
        df = read_mongo_collection(env('MONGODB_URL'), database_name=env('db_name'), collection_name=collection)
    df = basic_data_transform(df)
    print(df)
    return jsonify(
        dataframe=pickle_encode(df)
    )
@app.route("/data/process", methods=['POST'])
def process_data():
    req_json = request.get_json(force=True)
    collection = req_json["collection"]
    query = req_json["query"]
    if query:
        df = read_mongo_by_query(env('MONGODB_URL'), database_name=env('db_name'), collection_name=collection, query=query)
    else:
        df = read_mongo_collection(env('MONGODB_URL'), database_name=env('db_name'), collection_name=collection)
    
    heavy_process = Process(  # Create a daemonic process with heavy "my_func"
        target=long_data_transform,
        args=(df,),
        daemon=True
    )
    heavy_process.start()
    return jsonify(
        response='ack'
    )

if __name__ == "__main__":
    app.run(host=env("flask_host"), port=env("flask_port"))
    # from waitress import serve
    # serve(app, host=config['ENV']['HOST'], port=config['ENV']['PORT']) 
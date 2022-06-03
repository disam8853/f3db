
from bson import ObjectId
from flask import Flask, request, abort, jsonify, Response, make_response
from db_reader import read_mongo_by_query, read_mongo_collection, write_mongo_collection
from data_preprocessing import basic_data_transform, long_data_transform
from environs import Env
from utils import pickle_encode
import pandas as pd
from multiprocessing import Process
from threading import Thread, Lock
from pymongo import MongoClient
import networkx as nx
from dag import DAG
from networkx.readwrite import json_graph
import json
import numpy as np
import os
app = Flask(__name__)

env = Env()
env.read_env()
lock = Lock()

dag = DAG("./DATA_FOLDER/graph.gml.gz")

db_client = MongoClient(env("MONGODB_URL"))
pipelines_db = db_client['f3db'].pipelines


@app.route("/", methods=['GET'])
def get():
    return 'OK'


@app.route("/dag", methods=['GET'])
def get_dag():
    class NpEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, np.integer):
                return int(obj)
            if isinstance(obj, np.floating):
                return float(obj)
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            return super(NpEncoder, self).default(obj)

    data1 = json_graph.node_link_data(dag.G)
    print(data1)
    s1 = json.dumps(data1, cls=NpEncoder)

    return s1


@app.route("/dag", methods=['POST'])
def create_dag():
    data = request.json
    new_dag = DAG(data)
    new_dag.add_edge('1', '2')

    data = new_dag.get_json_graph()
    return data

@app.route("/clear", methods=['GET'])
def clear_volumn():
    root = "./DATA_FOLDER/"
    filenames = next(os.walk(root), (None, None, []))[2]
    print(filenames)
    for f in filenames:
        os.remove(os.path.join(root, f))
    global dag 
    dag = DAG("./DATA_FOLDER/graph.gml.gz")
    
    return 'clear'

@app.route("/basic_write", methods=['POST'])
def basic_write():
    req_json = request.get_json(force=True)
    collection = req_json["collection"]
    data = req_json["data"]
    df = pd.DataFrame(data=data)

    print(df)
    result = write_mongo_collection(env('MONGODB_URL'), database_name=env(
        'db_name'), collection_name=collection, df=df)
    return jsonify(
        result=result
    )


@app.route("/basic_transform", methods=['POST'])
def basic_transform():
    req_json = request.get_json(force=True)
    collection = req_json["collection"]
    query = req_json["query"]
    if query:
        df = read_mongo_by_query(env('MONGODB_URL'), database_name=env(
            'db_name'), collection_name=collection, query=query)
    else:
        df = read_mongo_collection(env('MONGODB_URL'), database_name=env(
            'db_name'), collection_name=collection)
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
    pipeline_id = req_json['pipeline_id']
    experiment_number = str(req_json['experiment_number'])

    if query:
        df = read_mongo_by_query(env('MONGODB_URL'), database_name=env(
            'db_name'), collection_name=collection, query=query)
    else:
        df = read_mongo_collection(env('MONGODB_URL'), database_name=env(
            'db_name'), collection_name=collection)

    try:
        pipeline = pipelines_db.find_one(
            {'_id': ObjectId(pipeline_id)}, {"_id": 0})
        # pipeline = pipeline['collaborator']
    except Exception as e:
        return make_response(jsonify(error='pipeline not found'), 404)

    # df = pd.DataFrame([[1,2],[3,4]])
    # long proces
    # heavy_process = Process(  # Create a daemonic process with heavy "my_func"
    #     target=long_data_transform,
    #     args=(dag, df, collection, pipeline_id, pipeline),
    #     daemon=True
    # )

    import time

    heavy_process = Thread(
        target=long_data_transform,
        args=(lock, dag, df, collection, pipeline_id,
              pipeline, experiment_number),
        daemon=True
    )
    heavy_process.start()
    # heavy_process.join()

    return jsonify(
        response='ack'
    )

############ Pipeline ############


@app.route('/pipeline', methods=['POST'])
async def create_pipeline():
    print(request.json)
    try:
        pipeline_id = pipelines_db.insert_one(request.json).inserted_id
    except Exception as e:
        return Response('Failed to insert into db!' + str(e), 500)
    return jsonify({"id": str(pipeline_id)})

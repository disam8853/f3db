from crypt import methods
from bson import ObjectId
from flask import Flask, request, abort, jsonify, Response
from db_reader import read_mongo_by_query, read_mongo_collection, write_mongo_collection
from data_preprocessing import basic_data_transform, long_data_transform
from environs import Env
from utils import pickle_encode
import pandas as pd
from multiprocessing import Process
from pymongo import MongoClient

app = Flask(__name__)

env = Env()
env.read_env()

db_client = MongoClient(env("MONGODB_URL"))
pipelines_db = db_client['f3db'].pipelines


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
    if query:
        df = read_mongo_by_query(env('MONGODB_URL'), database_name=env(
            'db_name'), collection_name=collection, query=query)
    else:
        df = read_mongo_collection(env('MONGODB_URL'), database_name=env(
            'db_name'), collection_name=collection)
    pipeline = pipelines_db.find_one(
        {'_id': ObjectId(pipeline_id)}, {"_id": 0})
    heavy_process = Process(  # Create a daemonic process with heavy "my_func"
        target=long_data_transform,
        args=(df, pipeline,),
        daemon=True
    )
    heavy_process.start()
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

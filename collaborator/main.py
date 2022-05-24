from flask import Flask, request, abort, jsonify
from db_reader import read_mongo
from data_preprocessing import basic_data_transform
from environs import Env
from utils import pickle_encode
import pandas as pd
import json

app = Flask(__name__)

env = Env()
env.read_env()

@app.route("/", methods=['GET'])
def get():
    return 'OK'

@app.route("/basic_transform", methods=['POST'])
def basic_transform():
    req_json = request.get_json(force=True)
    collection = req_json["collection"]
    query = req_json["query"]
    # df = read_mongo(db=env('db_name'), collection=collection, query=query, host=env("db_host"), port=env("db_port"), username=env("db_username"), password=env("db_password"), no_id=True)
    df = pd.DataFrame([[1,2],[3,4]])
    df = basic_data_transform(df)

    return jsonify(
        dataframe=pickle_encode(df)
    )


if __name__ == "__main__":
    app.run(host=env("flask_host"), port=env("flask_port"))
    # from waitress import serve
    # serve(app, host=config['ENV']['HOST'], port=config['ENV']['PORT']) 
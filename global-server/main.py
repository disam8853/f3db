from typing import Type
from flask import Flask, request, abort, Response, jsonify, make_response
from environs import Env
import pandas as pd
from pymongo import MongoClient
from bson.objectid import ObjectId
from sklearn.model_selection import train_test_split
import aiohttp
import asyncio
from merge import merge_pipeline
from dag import DAG
import pandas as pd
from parse import parse, parse_param, check_fitted
from f3db_pipeline import run_pipeline, build_child_data_node, get_max_surrogate_number, generate_collection_version, compare_collection_version, build_root_data_node, build_pipeline
from sklearn.svm import SVC
from sklearn.decomposition import PCA
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import FunctionTransformer, StandardScaler, MinMaxScaler
from sklearn.pipeline import Pipeline
from sklearn.ensemble import *
import numpy as np
import joblib
import os
import requests
from joblib import dump, load
from utils import getexception, predict_and_convert_to_metric_str, parse_condition_dict_to_tuple_list

XHEADER =  ['AGE','HBP_d_all_systolic', 'HBP_d_AM_systolic',
       'HBP_d_PM_systolic', 'HBP_d_all_diastolic', 'HBP_d_AM_diastolic',
       'HBP_d_PM_diastolic', 'HBP_d_systolic_D1_AM1', 'HBP_d_systolic_D1_AM2',
       'aspirin']
YHEADER = 'CV'

env = Env()
env.read_env()

app = Flask(__name__)
collaborators = env("COLLABORATORS_URL").split(',')
print(collaborators)

db_client = MongoClient(env("MONGODB_URL"))
pipelines_db = db_client['f3db'].pipelines

dag = DAG("./DATA_FOLDER/graph.gml.gz")

WAITING_PIPELINE = {}
DATA = {}


@app.route("/", methods=['GET'])
def get():
    return 'OK'


@app.route("/clear", methods=['GET'])
def clear_volumn():
    root = "./DATA_FOLDER/"
    filenames = next(os.walk(root), (None, None, []))[2]
    for f in filenames:
        os.remove(os.path.join(root, f))
    global dag
    dag = DAG("./DATA_FOLDER/graph.gml.gz")

    response = []
    for url in collaborators:

        payload = {}
        headers = {}

        response.append(requests.request(
            "GET", url+"/clear", headers=headers, data=payload))

    return jsonify(response=str(response))


async def post(url, data, session):
    try:
        async with session.post(url=url, json=data) as response:
            if response.status == 200:
                print("Status:", response.status)
                print("Content-type:", response.headers['content-type'])
                return await response.json()
            else:
                print(response.status)
                raise Exception('connection failed!')
    except aiohttp.ClientConnectorError as e:
        print('Connection Error', str(e))
        raise e
    except Exception as e:
        print(f"Unable to get url {url} due to {e.__class__}.")
        raise e

############ Pipeline ############


@app.route('/pipeline', methods=['POST'])
async def create_pipeline():
    try:
        async with aiohttp.ClientSession() as session:
            res = await asyncio.gather(*[post(f'{url}/pipeline', {"collaborator": request.json['collaborator']}, session) for url in collaborators])
    except Exception as e:
        return Response('Request to create pipeline failed!' + str(e), 500)
    print("All collaborators have created pipeline")

    pipeline = request.json
    pipeline['collaborator_pipieline_ids'] = [
        {**r, 'address': collaborators[idx]} for idx, r in enumerate(res)]
    try:
        pipeline_id = pipelines_db.insert_one(pipeline).inserted_id
    except Exception:
        return Response('Failed to insert into db!', 500)

    return jsonify({"id": str(pipeline_id), "collaborator": res})


@app.route('/pipeline/<pipeline_id>', methods=['GET'])
async def get_pipeline(pipeline_id):
    try:
        pipeline = find_pipeline_by_id(pipeline_id)
        if pipeline is None:
            return Response('pipeline not found', 404)
    except Exception:
        return Response('Failed to get pipeline!', 400)

    return jsonify(pipeline)


@app.route("/pipeline/fit", methods=["POST"])
async def train_model():
    data = request.json

    for attr in ['pipeline_id', 'collection', 'query']:
        if attr not in data:
            return Response(f'Must provide correct {attr}!', 400)

    pipeline_id = data['pipeline_id']
    # get newest experiment number
    experiment_number = find_greatest_exp_num(
        dag, pipeline_id, data['collection']) + 1
    if pipeline_id in WAITING_PIPELINE:
        return make_response(jsonify(error=f'Pipeline {pipeline_id} has started fitting.', pipeline=WAITING_PIPELINE[pipeline_id]), 400)

    try:
        pipeline = find_pipeline_by_id(pipeline_id)
        collaborator_pipieline_ids = pipeline['collaborator_pipieline_ids']
    except Exception:
        return Response('pipeline not found', 404)

    try:
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*[post(f'{col["address"]}/data/process', {**request.json, "pipeline_id": col['id'], "experiment_number": experiment_number}, session) for col in collaborator_pipieline_ids])
    except Exception:
        return Response('Request a train failed!', 500)

    WAITING_PIPELINE[pipeline_id] = {
        "experiment_number": experiment_number,
        "collaborators": collaborator_pipieline_ids,
        "collection": data['collection']}
    print("All collaborators have been noticed")
    return jsonify({'pipeline_id': pipeline_id, **WAITING_PIPELINE[pipeline_id]})


@app.route('/pipeline/merge', methods=['POST'])
def merge_pipeline_api():
    data = request.json

    for attr in ['pipeline_id', 'dataframe', 'dag_json']:
        if attr not in data:
            return Response(f'Must provide correct {attr}!', 400)

    col_pipeline_id = data['pipeline_id']
    try:
        pipeline = pipelines_db.find_one(
            {"collaborator_pipieline_ids.id": col_pipeline_id})
        pipeline_id = str(pipeline['_id'])
    except Exception:
        return Response('Failed to get pipeline!', 400)

    if pipeline_id not in WAITING_PIPELINE:
        return Response('Pipeline has not started fitting', 400)
    col_pipeline_in_waiting = next(
        (item for item in WAITING_PIPELINE[pipeline_id]['collaborators'] if item["id"] == col_pipeline_id), None)
    if col_pipeline_in_waiting is None:
        return Response(f'Collaborator {col_pipeline_id} has submitted.', 400)
    if pipeline_id not in DATA:
        DATA[pipeline_id] = []
    DATA[pipeline_id].append(data)

    # remove collaborator that has submitted df from waiting list
    WAITING_PIPELINE[pipeline_id]['collaborators'] = [
        p for p in WAITING_PIPELINE[pipeline_id]['collaborators'] if p.get('id') != col_pipeline_id]

    if len(WAITING_PIPELINE[pipeline_id]['collaborators']) == 0:
        try:
            experiment_number = WAITING_PIPELINE[pipeline_id]['experiment_number']
            collection = WAITING_PIPELINE[pipeline_id]['collection']
            src_id = merge_pipeline(dag, DATA[pipeline_id],
                                    pipeline_id, experiment_number, collection)
            try:
                # print('chungggg', src_id)
                pipeline_id = dag.get_node_attr(src_id)['pipeline_id']
                pipeline = find_pipeline_by_id(pipeline_id)
                run_pipeline(dag, src_id, experiment_number, pipeline)
                
            except Exception as e:
                getexception(e)
        except Exception as e:
            return Response('Merge failed.\n' + str(e), 400)
        del WAITING_PIPELINE[pipeline_id]
        del DATA[pipeline_id]
        return jsonify(model_id=src_id)

    del pipeline['_id']
    return jsonify(collaborators=pipeline['collaborator_pipieline_ids'])


@app.route('/pipeline/<pipeline_id>/status', methods=['GET'])
async def get_pipeline_status(pipeline_id):
    experiment_number = request.args.get('experiment_number')
    collection = request.args.get('collection')
    for attr in [experiment_number, collection]:
        if attr is None:
            return Response(f'Must provide correct {attr} in query parameter!', 400)
    cond = [("collection", collection), ("experiment_number",
                                         int(experiment_number)), ("pipeline_id", pipeline_id), ("type", "model")]
    nids = dag.get_nodes_with_condition(cond)
    if len(nids) == 0:
        return Response('there is no model found', 400)
    elif len(nids) != 1:
        return Response('multiple models detected!', 500)

    return jsonify(model_id=nids[-1])


@app.route('/model/<model_id>/predict', methods=['POST'])
def predict_model(model_id):
    data = request.json

    for attr in ['data', 'pipeline_id']:
        if attr not in data:
            return Response(f'Must provide correct {attr}!', 400)

    row_data = data['data']
    pipeline_id = data['pipeline_id']
    try:
        pipeline = find_pipeline_by_id(pipeline_id)
        if pipeline is None:
            return Response('pipeline not found', 404)
    except Exception:
        return Response('pipeline not found', 404)
    df = pd.DataFrame.from_dict(row_data)

    try:
        result = transform_and_predict(dag, df, model_id, pipeline)
    except NameError:
        return Response('model not found', 404)
    except Exception as e:
        return Response('predict error\n' + str(e), 500)

    return jsonify(result=result.tolist())

from search import get_k_best_models, find_match_nodes
@app.route('/model/get_k_best', methods=["POST"])
def get_top_k_models():
    data = request.json

    for attr in ['k', 'metric', 'condition']:
        if attr not in data:
            return Response(f'Must provide correct {attr}!', 400)

    k = data['k']
    metric = data['metric']
    condition_dict = data['condition']
    condition = parse_condition_dict_to_tuple_list(condition_dict)

    node_id_list = get_k_best_models(dag, k, metric, condition)
    return jsonify(node_id_list)

def find_pipeline_by_id(pipeline_id):
    return pipelines_db.find_one({'_id': ObjectId(pipeline_id)}, {"_id": 0})


@app.route("/pipeline/get_match_node_id", methods=["POST"])
def get_match_nodes():
    data = request.json

    for attr in ['condition']:
        if attr not in data:
            return Response(f'Must provide correct {attr}!', 400)

    condition_dict = data['condition']
    condition = parse_condition_dict_to_tuple_list(condition_dict)
    node_id_list = find_match_nodes(dag, condition)

    return jsonify(node_id_list)


# TODO: function parameter -> dag, df, model_id, raw_pipe_data:dict
def transform_and_predict(dag: DAG, df, model_id, pipeline):
    """
    1. parse pipeline_dict to sklearn pipeline
    2. use the pipeline to transform the data
    3. pipeline.predict(X)

    """
    client_pipeline = parse_client_pipeline(pipeline)
    pipe = Pipeline(client_pipeline)
    trans_data = pipe.fit_transform(df)

    dag_node = dag.get_node_attr(model_id)
    if dag_node is False:
        raise NameError('model not found')
    data_path = dag_node['filepath']
    loaded_model = joblib.load(data_path)
    result = loaded_model.predict(trans_data)
    # print('res',result)
    return result


def parse_client_pipeline(raw_pipe_data):
    sub_pipeline = []
    ref_list = []
    if raw_pipe_data == []:
        return raw_pipe_data

    for character in ['collaborator', 'global-server']:
        pipe = raw_pipe_data[character]

        for idx in range(len(pipe)):
            # print(pipe[idx])
            if(pipe[idx]['name'] != 'SaveData'):

                strp = pipe[idx]['name']+'()'
                if check_fitted(eval(strp)) or (pipe[idx]['name'] in ref_list):
                    continue
                else:
                    ref_list.append(pipe[idx]['name'])
                    sub_pipeline.append((pipe[idx]['name'], eval(strp)))
            else:  # SaveData
                continue
    return sub_pipeline
 

        
def find_greatest_exp_num(dag, pipeline_id, collection) -> int:
    # nids = dag.get_nodes_with_two_attributes(
    #     'pipeline_id', pipeline_id, 'collection', collection)
    # return dag.get_max_attribute_node(nids, 'experiment_number')
    try:
        condition = [('pipeline_id', pipeline_id), ('collection', collection)]
        nids = dag.get_nodes_with_condition(condition)
        return dag.get_max_attribute_node(nids, 'experiment_number')
    except TypeError:
        return 0
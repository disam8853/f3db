from flask import Flask, request, abort, Response, jsonify, make_response
from environs import Env
import pandas as pd
from pymongo import MongoClient
from bson.objectid import ObjectId
import aiohttp
import asyncio
from merge import merge_pipeline
from dag import DAG
import pandas as pd
from parse import parse, parse_param
from f3db_pipeline import build_child_data_node, get_max_surrogate_number, generate_collection_version, compare_collection_version, build_root_data_node, build_pipeline


env = Env()
env.read_env()

app = Flask(__name__)
collaborators = env("COLLABORATORS_URL").split(',')
print(collaborators)

db_client = MongoClient(env("MONGODB_URL"))
pipelines_db = db_client['f3db'].pipelines

dag = DAG('graph.gml.gz')

WAITING_PIPELINE = {}
DATA = {}


@app.route("/", methods=['GET'])
def get():
    return 'OK'


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
            model_id = run_pipeline(dag, src_id, experiment_number)
        except Exception as e:
            return Response('Merge failed.\n' + str(e), 400)
        del WAITING_PIPELINE[pipeline_id]
        del DATA[pipeline_id]
        return jsonify(model_id=model_id)

    del pipeline['_id']
    return jsonify(collaborators=pipeline['collaborator_pipieline_ids'])


@app.route('/pipeline/<pipeline_id>/status', methods=['GET'])
async def get_pipeline_status(pipeline_id):
    experiment_number = request.args.get('experiment_number')
    collection = request.args.get('collection')
    for attr in [experiment_number, collection]:
        if attr is None:
            return Response(f'Must provide correct {attr} in query parameter!', 400)

    return jsonify(pipeline_id=pipeline_id, experiment_number=experiment_number)


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

    result = transform_and_predict(dag, df, model_id, pipeline)

    return jsonify(result)


def find_pipeline_by_id(pipeline_id):
    return pipelines_db.find_one({'_id': ObjectId(pipeline_id)}, {"_id": 0})


def run_pipeline(dag, src_id, experiment_number):
    pipeline_id = dag.get_node_attr(src_id)['pipeline_id']
    pipeline = find_pipeline_by_id(pipeline_id)

    parsed_pipeline = parse(pipeline, 'global-server')
    # do pipeline (chung)
    pipe_param_string = parse_param(pipeline, 'global-server')
    for sub_pipeline in parsed_pipeline:
        sub_pipeline_param_list = pipe_param_string[parsed_pipeline.index(
            sub_pipeline)]
        src_id = build_pipeline(
            dag, src_id, sub_pipeline, param_list=sub_pipeline_param_list, experiment_number=experiment_number)

    return src_id


def find_greatest_exp_num(dag, pipeline_id, collection):
    nids = dag.get_nodes_with_two_attributes(
        'pipeline_id', pipeline_id, 'collection', collection)
    return dag.get_max_attribute_node(nids, 'experiment_number')

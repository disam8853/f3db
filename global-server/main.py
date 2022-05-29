from flask import Flask, request, abort, Response, jsonify, json
from environs import Env
from pymongo import MongoClient
from bson.objectid import ObjectId
import aiohttp
import asyncio

env = Env()
env.read_env()

app = Flask(__name__)
collaborators = env("COLLABORATORS_URL").split(',')
print(collaborators)

db_client = MongoClient(env("MONGODB_URL"))
pipelines_db = db_client['f3db'].pipelines

WAITING_PIPELINE = {}


@app.route("/", methods=['GET'])
def get():
    return 'OK'


############# model #################
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
    except Exception:
        return Response('Failed to get pipeline!', 400)

    return jsonify(pipeline)


@app.route("/pipeline/fit", methods=["POST"])
async def train_model():
    data = request.json

    if 'pipeline_id' not in data:
        return Response('Must provide correct pipeline ID!', 400)
    elif 'collection' not in data:
        return Response('Must provide correct collection!', 400)
    elif 'query' not in data:
        return Response('Must provide query!', 400)

    pipeline_id = data['pipeline_id']
    try:
        pipeline = find_pipeline_by_id(pipeline_id)
        collaborator_pipieline_ids = pipeline['collaborator_pipieline_ids']
    except Exception:
        return Response('pipeline not found', 404)

    try:
        async with aiohttp.ClientSession() as session:
            res = await asyncio.gather(*[post(f'{col["address"]}/data/process', {**request.json, "pipeline_id": col['id']}, session) for col in collaborator_pipieline_ids])
    except Exception:
        return Response('Request a train failed!', 500)

    WAITING_PIPELINE[pipeline_id] = collaborator_pipieline_ids
    print("All collaborators have been noticed")
    return jsonify(res_from_col=res)


@app.route('/pipeline/merge', methods=['POST'])
def merge_pipeline():
    return 'ok'


def find_pipeline_by_id(pipeline_id):
    return pipelines_db.find_one({'_id': ObjectId(pipeline_id)}, {"_id": 0})

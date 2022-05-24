from flask import Flask, request, abort, Response
from environs import Env
import aiohttp
import asyncio

env = Env()
env.read_env()

app = Flask(__name__)
collaborators = env("COLLABORATORS_URL").split(',')
print(collaborators)


@app.route("/", methods=['GET'])
def get():
    return 'OK'


############# model #################
async def post(url, data, session):
    try:
        async with session.post(url=url, data=data) as response:
            resp = await response.read()
            return resp
    except Exception as e:
        print(f"Unable to get url {url} due to {e.__class__}.")
        raise e


@app.route("/model/train", methods=["POST"])
async def train_model():
    try:
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*[post(f'{url}/data/process', request.data, session) for url in collaborators])
    except Exception:
        return Response('Request a train failed', 500)
    print("All collaborators have been noticed")
    return 'ok'

@app.route("/model/data", methods=["POST"])
def receive_data():
    return 'ok'

@app.route("/clip", methods=['POST'])
def callback():

    body = request.json
    txt = body['text']
    max_img_cnt = body['max_img_cnt']

    return {"max_img_cnt": max_img_cnt}


if __name__ == "__main__":
    app.run(host=env("flask_host"), port=env("flask_port"))
    # from waitress import serve
    # serve(app, host=config['ENV']['HOST'], port=config['ENV']['PORT']) 
from flask import Flask, request, abort
from environs import Env

env = Env()
env.read_env()

app = Flask(__name__)


@app.route("/", methods=['GET'])
def get():
    return 'OK'

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
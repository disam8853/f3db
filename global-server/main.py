from flask import Flask, request, abort

import configparser

app = Flask(__name__)

config = configparser.ConfigParser()
config.read('config.ini')

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
    app.run(host=config['ENV']['GLOBAL_HOST'], port=config['ENV']['GLOBAL_PORT'])
    # from waitress import serve
    # serve(app, host=config['ENV']['HOST'], port=config['ENV']['PORT']) 
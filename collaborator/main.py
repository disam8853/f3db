from flask import Flask, request, abort

import configparser

app = Flask(__name__)

config = configparser.ConfigParser()
config.read('config.ini')

@app.route("/", methods=['GET'])
def get():
    return 'OK'

if __name__ == "__main__":
    app.run(host=config['ENV']['COLAB_HOST'], port=config['ENV']['COLAB_PORT'])
    # from waitress import serve
    # serve(app, host=config['ENV']['HOST'], port=config['ENV']['PORT']) 
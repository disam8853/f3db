import requests
import json
import sys


def create_root_node():
    url = "http://localhost:20000/pipeline"

    payload = json.dumps({
    "collaborator": [],
    "global-server": []
    })
    headers = {
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.text)


def get_pipeline_by_id(arg):

    url = "http://localhost:20000/pipeline/" + arg[2]

    payload={}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    print(response.text)


def fit_PL_1(arg):
    url = "http://localhost:20000/pipeline/fit"

    payload = json.dumps({
    "pipeline_id": arg[2],
    "collection": "test",
    "query": "",
    "src_id": ""
    })
    headers = {
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.text)



url = "localhost:20000/pipeline"

payload = json.dumps({
  "collaborator": [
    {
      "name": "SimpleImputer",
      "parameter": [
        {
          "strategy": "most_frequent"
        }
      ]
    },
    {
      "name": "StandardScaler",
      "parameter": []
    },
    {
      "name": "SaveData",
      "parameter": []
    },
    {
      "name": "MinMaxScaler",
      "parameter": []
    }
  ],
  "global-server": [
    {
      "name": "train_test_split",
      "parameter": []
    },
    {
      "name": "MinMaxScaler",
      "parameter": []
    },
    {
      "name": "LogisticRegression",
      "parameter": []
    }
  ]
})
headers = {
  'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)


eval(sys.argv[1])(sys.argv)
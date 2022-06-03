# TODO: merge collaborator data/pipeline into global
from webbrowser import get
from dag import DAG
from utils import pickle_decode,  getexception
import pandas as pd
from f3db_pipeline import generate_node, save_data

DATA_FOLDER = "./DATA_FOLDER/"
WHO = 'global-server'
USER = "bobo"
TAG = "default-tag"
COLLECTION_VERSION = 0
EXP_NUM = 0


def merge_pipeline(global_dag: DAG, collaborator_data: list, global_pipeline_id: str, experiment_number: int, collection: str) -> str:
    """
    1. get all collaborator post request data
    2. merge dag and dataframe
    3. save combined data
    4. return dag
    collaborator_data = [{
        "pipeline_id": pipeline_id,
        "dag_json": dag.get_dict_graph(),
        "dataframe": pickle_encode(df)
    }, {}]
    """

    # TODO: create empty dataframe
    global_df = pd.DataFrame()

    # TODO: crete new_data_node with empty file
    # TODO: change variables
    global_node_id, global_node_info, global_node_filepath = generate_node(
        who=WHO, user=USER, collection=collection, collection_version=COLLECTION_VERSION, experiment_number=experiment_number, pipeline_id=global_pipeline_id, tag=TAG, type='data', folder=DATA_FOLDER)
    global_dag.add_node(global_node_id, **global_node_info)
    # iter each collaborator post data
    for data in collaborator_data:
        try:
            # TODO: if data is json, turn data into dict

            colab_pipeline_id = data['pipeline_id']
            colab_dag = DAG(data['dag_json'])
            colab_df = pickle_decode(data['dataframe'])
            colab_last_node = data['last_node']

            # TODO: check collaborator dag

            # merge data
            global_df = pd.concat([global_df, colab_df])

            # find last colab_data_node by colab_pipeline_id
            # colab_node_id = colab_dag.get_nodes_with_attributes("pipeline_id", colab_pipeline_id)

            # merge global_dag & colab_dag
            global_dag.dag_compose(colab_dag.G)

            # add edge between collab_data_node & new_data_node
            global_dag.add_edge(colab_last_node, global_node_id)

        except Exception as e:
            getexception(e)

    save_data(global_node_filepath, global_df)
    print(global_dag.get_node_attr(global_node_id))

    return global_node_id

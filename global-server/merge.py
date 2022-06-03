# TODO: merge collaborator data/pipeline into global
from webbrowser import get
from dag import DAG
from utils import pickle_decode,  getexception
import pandas as pd
from f3db_pipeline import generate_node

DATA_FOLDER = "./DATA_FOLDER/"
WHO = 'global-server'
USER = "bobo"
TAG = "default-tag"
COLLECTION = "blood pressure"
COLLECTION_VERSION = 0
EXP_NUM = 0


def merge_pipeline(global_dag: DAG, collaborator_data: list, global_pipeline_id: str) -> None:
    
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
    global_node_id, global_node_info, global_node_filepath = generate_node(
            who=WHO, user=USER, collection=COLLECTION, collection_version=COLLECTION_VERSION, experiment_number=EXP_NUM, tag=TAG, type='data', folder=DATA_FOLDER)

    # iter each collaborator post data
    for data in collaborator_data:
        try:
            # TODO: if data is json, turn data into dict

            colab_pipeline_id = data['pipeline_id']
            colab_dag = DAG(data['dag_json'])
            colab_df = pickle_decode(data['dataframe'])
            # colab_dag_leaf = data['']

            # TODO: check collaborator dag
            
            # merge data
            global_df = pd.concat([global_df, colab_df])

            # find last colab_data_node by colab_pipeline_id
            colab_node_id = colab_dag.get_nodes_with_attributes("pipeline_id", colab_pipeline_id)

            
            # merge global_dag & colab_dag
            global_dag.dag_compose(colab_dag.G)

            # add edge between collab_data_node & new_data_node
            global_dag.add_edge(global_node_id, colab_node_id)

        except Exception as e:
            getexception(e)


    return None
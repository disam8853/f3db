# TODO: merge collaborator data/pipeline into global
from dag import DAG
from utils import pickle_decode

def merge_pipeline(global_dag: DAG, collaborator_data: list, global_pipeline_id: str) -> None:
    
    """
    1. get all collaborator post request data
    2. merge dag and dataframe
    3. save combined data
    4. return dag
    data = {
        "pipeline_id": pipeline_id,
        "dag_json": dag.get_dict_graph(),
        "dataframe": pickle_encode(df)
    }
    """

    # TODO: create empty dataframe

    # TODO: crete new_data_node with empty file
    
    # iter each collaborator post data
    for data in collaborator_data:

        # TODO: if data is json, turn data into dict

        colab_pipeline_id = data['pipeline_id']
        colab_dag = DAG(data['dag_json'])
        colab_dataframe = pickle_decode(data['dataframe'])

        # TODO: check collaborator dag

        # TODO: merge data

        # TODO: find last colab_data_node by colab_pipeline_id

        # TODO: add edge between collab_data_node & new_data_node

        # TODO: merge global_dag & colab_dag


    return None
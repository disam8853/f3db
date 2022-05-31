# TODO: merge collaborator data/pipeline into global
from dag import DAG
from utils import pickle_decode

def merge_pipeline(data, pipeline_id):
    
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

    # TODO: turn data type into dict
    pipeline_id = data['pipeline_id']
    dag = DAG(data['dag_json'])
    dataframe = pickle_decode(data['dataframe'])

    # TODO: check all dag
    
    # TODO: decode data and turn into dataframe
    
    # TODO: merge data
    
    # TODO: crete new data node

    # TODO: add data node to collaborator dag

    # TODO: merge dag

    
    return None
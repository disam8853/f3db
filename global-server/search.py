# TODO: function to search dag
from utils import metric_str_to_dict
from dag import DAG

# get_k_best_models(dag, k=2, metric="accuracy", condition=[('user','bobo')])
def get_k_best_models(dag:DAG, k:int, metric:str, condition=None, **kwargs) -> list:
    if condition is None:
        for k, v in kwargs.items():
            condition.append((k, v))

    # only search model
    if ('type', 'model') not in condition:
        condition.append(('type', 'model'))

    node_list = dag.get_nodes_with_condition(condition, return_info=True)
    k = min(len(node_list), k)
    sorted_node_list = sorted(node_list, key=lambda n: metric_str_to_dict(n['metrics'])[metric], reverse=True)[:k]

    return [(n['node_id'], metric_str_to_dict(n['metrics'])[metric]) for n in sorted_node_list]



def find_match_nodes(dag: DAG, condition) -> list:
    node_list = dag.get_nodes_with_condition(condition, return_info=True)
    return node_list



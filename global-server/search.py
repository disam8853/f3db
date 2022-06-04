# TODO: function to search dag
from utils import metric_str_to_dict
from dag import DAG

def get_k_best_models(dag:DAG, k:int, metric:str, condition=None, sorted=True, **kwargs) -> list:
    if condition is None:
        condition = []
        for k, v in kwargs.items():
            condition.append((k, v))

    node_list = dag.get_nodes_with_condition(condition, return_info=True)
    sorted_node_list = sorted(node_list, key=lambda n: metric_str_to_dict(n['metrics'])[metric])[:k]

    return sorted_node_list

from __future__ import annotations
# TODO: maintain consistency between global & local dag
# TODO: compare two dag is same or not

import networkx as nx
from networkx.readwrite import json_graph
from utils import current_time, current_date, getexception, comma_str_to_list
from functools import singledispatch, update_wrapper
import json
import numpy as np
from operator import ge, le
from time import sleep

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

class DAG():
    class NodeNotFound(Exception):
        def __init__(self, node_id, message="node not found in dag"):
            self.node_id = node_id
            self.message = message
            super().__init__(f"{self.message}, node id: {self.node_id}")

    class GraphUsing(Exception):
        def __init__(self, who, message="dag is freezed"):
            self.who = who
            self.message = message
            super().__init__(f"{self.message}, who: {self.who}")

    def dag_refresh(func):
        # if change graph structure, refresh dag metadata
        def wrapper(self, *args, **kwargs):
            output = func(self, *args, **kwargs)
            self.roots = [n for n,d in self.G.in_degree() if d==0]
            self.nodes = list(self.G.nodes())
            self.leaves = [n for n in self.G.nodes() if self.G.out_degree(n)==0 and self.G.in_degree(n)>=1]
            self.nodes_info = dict(self.G.nodes(data=True))
            self.number_of_edges = nx.number_of_edges(self.G)
            self.number_of_nodes = nx.number_of_nodes(self.G)
            self.type = type(self.G)

            self.save_graph("./DATA_FOLDER/graph.gml.gz")
            return output
        return wrapper

    def dispatchmethod(func):
        dispatcher = singledispatch(func)
        def wrapper(*args, **kw):
            return dispatcher.dispatch(args[1].__class__)(*args, **kw)
        wrapper.register = dispatcher.register
        update_wrapper(wrapper, func)
        return wrapper

    

    @dag_refresh
    def __init__(self, graph_object):
        self.roots = []
        self.nodes = []
        self.leaves = []
        self.nodes_info = []
        self.number_of_edges = 0
        self.number_of_nodes = 0
        self.type = None
        self.comma_attributes = ['operation', 'x_headers', 'metrics']
        self.integer_attributes = ['experiment_number']
        self.init_attributes = {
                'node_id': "",
                'who': 'global-server',
                'user': 'bobo',
                'date': current_date(),
                'time': current_time(),
                'collection': 'default dataset',
                'collection_version': 0,
                'experiment_number': 0,
                'tag': 'test-tag',
                'type': 'data', # data or model
                'pipeline_id': "",
                'operation': "", # comma seperate
                'filepath':'default filepath',
                'x_headers': "", # comma seperate, global server has 1 id, collab has many id -> list of strings
                'y_headers': "", # 1 string
                'metrics':"" # model result
            }
        
        try:
            if type(graph_object) is str:
                self.G = nx.read_gml(graph_object)
            elif type(graph_object) is dict:
                self.G = json_graph.node_link_graph(graph_object)
            else:
                self.G = graph_object
        except Exception as e:
            getexception(e)
            self.G = nx.MultiDiGraph()

    @dag_refresh
    def add_edge(self, src_idx, dest_idx) -> bool:
        try:
            self.G.add_edge(src_idx, dest_idx)
            # if add edge formed cycle, remove edge
            if not nx.is_directed_acyclic_graph(self.G):
                self.G.remove_edge(src_idx, dest_idx)
                return False
            return True
        except nx.NetworkXError as err:
            print(str(err))
            return False

    @dag_refresh
    def add_node(self, index, **attr) -> bool:
        try:
            # fully copy a dict
            node = self.init_attributes.copy()

            for key, value in attr.items():
                node[key] = value

            self.G.add_node(index, **node)
            return True
        except nx.NetworkXError as err:
            print(str(err))
            return False
    
    @dag_refresh
    def remove_edge(self, src_idx, dest_idx) -> bool:
        try:
            self.G.remove_edge(src_idx, dest_idx)
            return True
        except nx.NetworkXError as err:
            print(str(err))
            return False

    @dag_refresh
    def remove_node(self, index) -> bool:
        try:
            self.G.remove_node(index)
            return True
        except nx.NetworkXError as err:
            print(str(err))
            return False

    @dispatchmethod
    @dag_refresh
    def dag_compose(self, other_dag: DAG) -> bool:
        C = nx.compose(self.G, other_dag)
        self.G = C

    @dag_compose.register(list)
    @dag_refresh
    def _(self, other_dags: list) -> bool:
        all_dag = [self.G] + other_dags
        C = nx.compose_all(all_dag)
        self.G = C


    def get_node_edges(self, index) -> list:
        # index can be list of nodes indicies or one index 
        return self.G.edges(index).copy()


    def get_topological_sort(self) -> list:
        return list(nx.topological_sort(self.G))

    def get_node_attr(self, index) -> dict:
        if not self.G.has_node(index):
            # return False
            raise self.NodeNotFound
        # shallow copy
        return self.G.nodes[index].copy()

    def set_all_nodes_attr(self, values, name=None) -> None:
        nx.set_node_attributes(self.G, values, name)
    
    def get_all_ancestors(self,index) -> list:
        return sorted(list(nx.ancestors(self.G, index)))

    def get_all_descendants(self,index) -> list:
        return sorted(list(nx.ancestors(self.G, index)))

    def freeze_graph(self) -> None:
        nx.freeze(self.G)

    def unfreeze_graph(self) -> None:
        newgraph = self.type
        self.G = newgraph(self.G)

    def save_graph(self, filepath) -> None:
        nx.write_gml(self.G, filepath)

    def get_json_graph(self) -> json:
        data = json_graph.node_link_data(self.G)
        return json.dumps(data, cls=NpEncoder)

    def get_dict_graph(self) -> dict:
        data = json_graph.node_link_data(self.G)
        return data

    def get_subgraph(self, src_id) -> DAG:
        s = self.G.subgraph(nx.dfs_tree(self.G, src_id="1").nodes()).copy()
        return s

    # def get_nodes_with_attributes(self, attribute, value) -> list:
    #     selected_data = [ n for n,d in self.G.nodes().items() if d[attribute] == value]
    #     return selected_data

    # def get_nodes_with_two_attributes(self, a1, v1, a2, v2) -> list:
    #     selected_data = [ n for n,d in self.G.nodes().items() if ((d[a1] == v1) and (d[a2] == v2))]
    #     return selected_data

    def get_nodes_with_condition(self, condition, return_info=False) -> list:
        # condition = [("who", 'global-server'), ("user", 'bobo'), ("collection_version", 3)] 
        def check(n, d, con):
            for attr, val in con:
                if attr in self.comma_attributes:
                    if val not in comma_str_to_list(d[attr]):
                        return None
                elif d[attr] != val:
                    return None
            return n

        node_list = []
            
        for n, d in self.G.nodes().items():
            target_node = check(n, d, condition)
            if target_node is not None:
                if return_info:
                    info = self.get_node_attr(target_node)
                    node_list.append(info)
                else:
                    node_list.append(target_node)

        return node_list

    def get_max_attribute_node(self, node_id_list, attribute) -> str:

        max_attribute = "0"
        for node_id in node_id_list:
            node = self.get_node_attr(node_id)
            attr_value = node[attribute]
            if attr_value > max_attribute:
                max_attribute = attr_value

        return max_attribute

    def get_best_node_by_attribute(self, node_id_list: list, attribute:str, sign:function=None):
        if sign is None:
            sign = ge # dafault is greate than
        if type(sign) is str:
            if sign == ">":
                sign = ge
            elif sign == "<":
                sign = le
        best_value = 0
        for index, node_id in enumerate(node_id_list):
            node = self.get_node_attr(node_id)

            if index == 0:
                best_value = node[attribute]
            attr_value = node[attribute]

            if attribute in self.integer_attributes:
                if index == 0:
                    best_value = int(node[attribute])
                attr_value = int(node[attribute])
            if not sign(best_value, attr_value):
                best_value = attr_value

        return best_value


            




if __name__ == "__main__":
    print("\n**** create graph ****")
    dag = DAG(nx.MultiDiGraph())
    print(DAG)

    print("\n**** add/remove node/edge ****")
    print("add node:", dag.add_node("bobo1", filepath="./bobo1"))
    print("add edge: ", dag.add_edge("bobo1", "bobo2"))
    print("add edge: ", dag.add_edge("bobo2", "bobo3"))
    print("add edge: ", dag.add_edge("bobo3", "bobo1"))
    print("add edge: ", dag.add_edge("bobo4", "bobo5"))
    print("remove node: ", dag.remove_node("bobo4"))
    print("remove edge: ", dag.remove_edge("bobo4", "bobo5"))


    print("\n**** save/load graph ****")
    print("save graph to gml.gz: ", dag.save_graph('test.gml.gz'))
    dag = DAG('test.gml.gz')

    print("\n**** show graph data ****")
    print("show all roots: ", dag.roots)
    print("show all nodes: ", dag.nodes)
    print("show topological sort: ", dag.get_topological_sort())

    print("\n**** set/get node attributes ****")
    print("set attributes to bobo2: ", dag.set_all_nodes_attr({'bobo2': {'time':current_time(), 'filepath':"./bobo2"}}))
    print("get node bobo1 attribute: ", dag.get_node_attr('bobo1'))
    print("get node bobo2 attribute: ", dag.get_node_attr('bobo2'))
    print("get node bobo3 attribute: ", dag.get_node_attr('bobo3'))


    print("\n**** freeze graph ****")
    print("freeze graph: ", dag.freeze_graph())
    print("check graph is freeze: ", nx.is_frozen(dag.G))
    print("add edge: ", dag.add_edge("bobo6", "bobo7"))
    print("freeze graph: ", dag.unfreeze_graph())
    print("check graph is freeze: ", nx.is_frozen(dag.G))

    print("\n**** dag compose ****")
    G1 = DAG(nx.MultiDiGraph())
    G1.add_edge(1, 2)
    G1.add_edge(1, 3)
    G1.add_edge(5, 6)

    G2 = DAG(nx.MultiDiGraph())
    G2.add_edge(0, 1)

    G1.dag_compose(G2.G)
    print("show G1, G2 nodes: ", G1.nodes)

    G3 = DAG(nx.MultiDiGraph())
    G3.add_edge(10, 11)
    G1.dag_compose([G2.G, G3.G])
    print("show G1, G2, G3 nodes: ", G1.nodes)
            

        

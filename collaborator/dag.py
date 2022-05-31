from __future__ import annotations
# TODO: maintain consistency between global & local dag
# TODO: compare two dag is same or not

import networkx as nx
from networkx.readwrite import json_graph
from utils import current_time, current_date
from functools import singledispatch, update_wrapper
import json
import numpy as np


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
    def dag_refresh(func):
        # if change graph structure, refresh dag metadata
        def wrapper(self, *args, **kwargs):
            output = func(self, *args, **kwargs)
            self.roots = [n for n,d in self.G.in_degree() if d==0]
            self.nodes = self.G.nodes()
            self.nodes_info = self.G.nodes(data=True)
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
        self.nodes_info = []
        self.number_of_edges = 0
        self.number_of_nodes = 0
        self.type = None
        self.init_attributes = {
                'who': 'global-server',
                'user': 'bobo',
                'date': current_date(),
                'time': current_time(),
                'collection': 'default dataset',
                'collection_version': 0,
                'experiment_number': 0,
                'tag': 'test-tag',
                'type': 'data', # data or model
                'pipeline_id': "", # comma seperate, global server has 1 id, collab has many id
                'operation': "", # comma seperate
                'filepath':'default filepath'
            }
        
        try:
            if type(graph_object) is str:
                self.G = nx.read_gml(graph_object)
            elif type(graph_object) is dict:
                self.G = json_graph.node_link_graph(graph_object)
            else:
                self.G = graph_object
        except:
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
        return self.G.edges(index)


    def get_topological_sort(self) -> list:
        return list(nx.topological_sort(self.G))

    def get_node_attr(self, index) -> dict:
        if not self.G.has_node(index):
            return False
        return self.G.nodes[index]

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
        s = self.G.subgraph(nx.dfs_tree(self.G, src_id="1").nodes())
        return s

    def get_nodes_with_attributes(self, attribute, value) -> dict:
        selected_data = dict( (n,d) for n,d in self.G.nodes().items() if d[attribute] == value)
        return selected_data

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
            

        

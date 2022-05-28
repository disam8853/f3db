import networkx as nx
from utils import current_time

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
            return output
        return wrapper

    @dag_refresh
    def __init__(self, graph_object):
        self.roots = []
        self.nodes = []
        self.nodes_info = []
        self.number_of_edges = 0
        self.number_of_nodes = 0
        self.type = None
        if type(graph_object) is str:
            self.G = nx.read_gml(graph_object)
        else:
            self.G = graph_object

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
            self.G.add_node(index, **attr)
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

#TODO: implement stage dag
class STAGE(DAG):
    def __init__(self):
        self.G = nx.DiGraph()



if __name__ == "__main__":
    print("\n**** create graph ****")
    dag = DAG(nx.MultiDiGraph())
    print(DAG)

    print("\n**** add/remove node/edge ****")
    print("add node:", dag.add_node("bobo1", time=current_time(), filepath="./bobo1"))
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
        

from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split
# from bobo_pipeline import Pipeline
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler
from sklearn.svm import SVC
from sklearn.decomposition import PCA
from sklearn.preprocessing import LabelEncoder
import numpy as np
from random import randrange, choice
from sklearn.neighbors import NearestNeighbors

def basic_classification_pipeline():
    def check_fitted(clf): 
        return hasattr(clf, "classes_")
    print("start")
    X, y = make_classification(random_state=0)
    X_train, X_test, y_train, y_test = train_test_split(X, y,
                                                            random_state=0)
    op_data = [('pca', PCA()), ('scaler', StandardScaler())]
    pipe = Pipeline(op_data)
    pipe.fit(X_train, y_train)

    last = pipe.steps[-1][1]
    print(last)
    print(check_fitted(last))

    op_model = [('pca', PCA()), ('scaler', StandardScaler()), ('svc', SVC())]
    pipe = Pipeline(op_model)
    pipe.fit(X_train, y_train)

    last = pipe.steps[-1][1]
    print(last)
    print(check_fitted(last))

    
    # print("\n***** dag result *****\n")
    # print("show all roots: ", pipe.dag.roots)
    # print("show all nodes: ", pipe.dag.nodes)
    # print("show nodes_info: ", pipe.dag.nodes_info)
    # print("show number_of_nodes: ", pipe.dag.number_of_nodes)
    # print("show number_of_edges: ", pipe.dag.number_of_edges)
    # print("show edges: ", pipe.dag.get_node_edges(pipe.dag.get_topological_sort()))
    # print("finish")

class FillNa(BaseEstimator, TransformerMixin):

    def transform(self, x, y=None):
            non_numerics_columns = x.columns.difference(
                x._get_numeric_data().columns)
            for column in x.columns:
                if column in non_numerics_columns:
                    x.loc[:, column] = x.loc[:, column].fillna(
                        x.loc[:, column].value_counts().idxmax())
                else:
                    x.loc[:, column] = x.loc[:, column].fillna(
                        x.loc[:, column].mean())
            return x

    def fit(self, x, y=None):
        return self


class CategoricalToNumerical(BaseEstimator, TransformerMixin):

    def transform(self, x, y=None):
        non_numerics_columns = x.columns.difference(
            x._get_numeric_data().columns)
        le = LabelEncoder()
        for column in non_numerics_columns:
            x.loc[:, column] = x.loc[:, column].fillna(
                x.loc[:, column].value_counts().idxmax())
            le.fit(x.loc[:, column])
            x.loc[:, column] = le.transform(x.loc[:, column]).astype(int)
        return x

    def fit(self, x, y=None):
        return self


class FeatureSelector(BaseEstimator, TransformerMixin):
    def __init__(self, feature_map: dict=None,  feature_keys: list=None):
        self.feature_map = feature_map
        self.feature_keys = feature_keys

    def keys_to_col(self, dictionary, keys):
        res = []
        for v in list(map(dictionary.get, keys)):
            res += v
        return res

    #Return self nothing else to do here
    def fit(self, X, y=None):
        return self

    #Method that describes what we need this transformer to do
    def transform(self, X, y=None):
        if self.feature_map and self.feature_keys:
            cols = self.keys_to_col(self.feature_map, self.feature_keys)
            return X[cols]
        elif self.feature_keys:
            return X[cols]
        else:
            return X


class Smote(BaseEstimator, TransformerMixin):
    def __init__(self, percentile=None):
        self.percentile = percentile

    def SMOTE(self, T, N, k, h = 1.0):
        """
        Returns (N/100) * n_minority_samples synthetic minority samples.
        Parameters
        ----------
        T : array-like, shape = [n_minority_samples, n_features]
            Holds the minority samples
        N : percetange of new synthetic samples: 
            n_synthetic_samples = N/100 * n_minority_samples. Can be < 100.
        k : int. Number of nearest neighbours. 
        Returns
        -------
        S : Synthetic samples. array, 
            shape = [(N/100) * n_minority_samples, n_features]. 
        """    
        n_minority_samples, n_features = T.shape
        
        if N < 100:
            #create synthetic samples only for a subset of T.
            #TODO: select random minortiy samples
            N = 100
            pass

        if (N % 100) != 0:
            raise ValueError("N must be < 100 or multiple of 100")
        
        N = N/100
        n_synthetic_samples = N * n_minority_samples
        S = np.zeros(shape=(n_synthetic_samples, n_features))
        
        #Learn nearest neighbours
        neigh = NearestNeighbors(n_neighbors = k)
        neigh.fit(T)
        
        #Calculate synthetic samples
        for i in range(n_minority_samples):
            nn = neigh.kneighbors(T[i], return_distance=False)
            for n in range(N):
                nn_index = choice(nn[0])
                #NOTE: nn includes T[i], we don't want to select it 
                while nn_index == i:
                    nn_index = choice(nn[0])
                    
                dif = T[nn_index] - T[i]
                gap = np.random.uniform(low = 0.0, high = h)
                S[n + i * N, :] = T[i,:] + gap * dif[:]
        
        return S

    #Return self nothing else to do here
    def fit(self, X, y=None):
        return self
    
    #Method that describes what we need this transformer to do
    def transform(self, X, y=None):
        # TODO: fix transform return value
        n_pos, n_neg = sum(y==1), sum(y==0)
        n_syn = (n_neg-n_pos)/float(n_pos) 
        X_pos = X[y==1]
        X_syn = self.SMOTE(X_pos, int(round(n_syn))*100, 5)
        y_syn = np.ones(X_syn.shape[0])
        X, y = np.vstack([X, X_syn]), np.concatenate([y, y_syn])
        return(X, y)


def fisher(X, y, percentile=None):
    # TODO: check function correctness
    X_pos, X_neg = X[y==1], X[y==0]
    X_mean = X.mean(axis=0)
    X_pos_mean, X_neg_mean = X_pos.mean(axis=0), X_neg.mean(axis=0)
    deno = (1.0/(X_pos.shape[0]-1))*X_pos.var(axis=0) +(1.0/(X_neg.shape[0]-1)*X_neg.var(axis=0))
    num = (X_pos_mean - X_mean)**2 + (X_neg_mean - X_mean)**2
    F = num/deno
    sort_F = np.argsort(F)[::-1]
    n_feature = (float(percentile)/100)*X.shape[1]
    ind_feature = sort_F[:np.ceil(n_feature)]
    return(ind_feature)



if __name__ == "__main__":
    # basic_classification_pipeline()
    pass


from sklearn.svm import SVC
from sklearn.preprocessing import StandardScaler
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
X, y = make_classification(random_state=0)
X_train, X_test, y_train, y_test = train_test_split(X, y,
                                                    random_state=0)
pipe = Pipeline([('scaler', StandardScaler()), ('SVC', SVC())])
# The pipeline can be used as any other estimator
# and avoids leaking the test set into the train set
data = {'SVC__kernel': 'linear', 'SVC__gamma': 'auto', 'SVC__random_state': 42}

pipe.set_params(**data)
pipe.fit(X_train, y_train)
print(pipe.get_params())
print(pipe.score(X_test, y_test))

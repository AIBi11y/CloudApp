# This Python 3 environment comes with many helpful analytics libraries installed
# It is defined by the kaggle/python docker image: https://github.com/kaggle/docker-python
# For example, here's several helpful packages to load in 


# Input data files are available in the "../input/" directory.
# For example, runninga this (by clicking run or pressing Shift+Enter) will list the files in the input directory
import pandas as pd
import numpy as np
from sklearn import ensemble, tree, linear_model
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import r2_score, mean_squared_error, make_scorer
from sklearn.utils import shuffle
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression, RidgeCV, LassoCV, ElasticNetCV
from sklearn.linear_model import Lasso

import warnings
warnings.filterwarnings('ignore')


train = pd.read_csv(r'CloudApp\Data\cleansed_listings.csv')
train = train[train.total_price > 0]



dataCols = [
'beds'
,'amenities_tv_ind'
,'amenities_cable_tv_ind'
,'amenities_internet_ind'
,'amenities_wifi_ind'
,'amenities_kitchen_ind'
,'amenities_paid_park_off_ind'
,'amenities_free_park_on_ind'
,'amenities_heating_ind'
,'nb_num'
,'rt_num'
]

train['neighbourhood'] = train['neighbourhood'].astype('category')
train['property_type'] = train['property_type'].astype('category')
train['room_type'] = train['room_type'].astype('category')

train['nb_num'] = train['neighbourhood'].cat.codes
train['pt_num'] = train['property_type'].cat.codes
train['rt_num'] = train['room_type'].cat.codes


numerical_features = train[dataCols].columns
train_num = train[numerical_features]
train_cat = train[categorical_features]

# Handle remaining missing values for numerical features by using median as replacement
train_num = train_num.fillna(train_num.median())

train2 = train_num

train['beds'] = train['beds'].fillna(train['beds'].median())


#split the data to train the model 

X_train,X_test,y_train,y_test = train_test_split(train2,train['total_price'],test_size = 0.2,random_state= 0)

X_train.shape,X_test.shape,y_train.shape,y_test.shape


#defining cross validation
n_folds = 5
from sklearn.metrics import make_scorer
from sklearn.model_selection import KFold
scorer = make_scorer(mean_squared_error,greater_is_better = False)
def rmse_CV_train(model):
    kf = KFold(n_folds,shuffle=True,random_state=42).get_n_splits(train.values)
    rmse = np.sqrt(-cross_val_score(model,X_train,y_train,scoring ="neg_mean_squared_error",cv=kf))
    return (rmse)
def rmse_CV_test(model):
    kf = KFold(n_folds,shuffle=True,random_state=42).get_n_splits(train.values)
    rmse = np.sqrt(-cross_val_score(model,X_test,y_test,scoring ="neg_mean_squared_error",cv=kf))
    return (rmse)




lasso = Lasso(alpha=0.0001, max_iter=10e5)
lasso.fit(X_train,y_train)
train_score=lasso.score(X_train,y_train)
test_score=lasso.score(X_test,y_test)
coeff_used = np.sum(lasso.coef_!=0)

comb = pd.read_csv(r'S:\CloudApp\Data\SQLAExport.txt')
dataCols = [
'nb_num'
,'rt_num'
,'beds'
,'amenities_tv_ind'
,'amenities_cable_tv_ind'
,'amenities_internet_ind'
,'amenities_wifi_ind'
,'amenities_kitchen_ind'
,'amenities_paid_park_off_ind'
,'amenities_free_park_on_ind'
,'amenities_heating_ind'
]
comb = comb[dataCols]


comb['predict_val'] = lasso.predict(comb)

#FILE OF PERMUTATIONS AND PREDICTED VALUES.
file_name = r'S:\CloudApp\Data\output.csv'
comb.to_csv(file_name, sep=',', encoding='utf-8')


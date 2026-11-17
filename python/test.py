import pandas as pd
import numpy as np
import optuna
import joblib

from lightgbm import LGBMRegressor,early_stopping
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split, KFold
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error

model = joblib.load('./python/delivery_model.pkl')
sample = pd.DataFrame([{
    'distance_km': 7.5,
    'order_hour': 18,
    'Delivery_person_Age': 28,
    'Delivery_person_Ratings': 4.5,
    'multiple_deliveries': 1,
    'Weatherconditions': 'conditions Sunny',
    'Road_traffic_density': 'Low',
    'Festival': 'No',
    'City': 'Urban',
    'Type_of_vehicle': 'motorcycle'
}])

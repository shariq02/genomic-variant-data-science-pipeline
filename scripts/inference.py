"""
Inference Script for Genomic Variant Models
Usage: python inference.py --model variant --features features.csv
"""
import pickle
import pandas as pd

class VariantPredictor:
    def __init__(self, model_path='models/ensemble_xgb_variants.pkl', 
                 scaler_path='models/variant_scaler.pkl'):
        with open(model_path, 'rb') as f:
            self.model = pickle.load(f)
        with open(scaler_path, 'rb') as f:
            self.scaler = pickle.load(f)
    
    def predict(self, features):
        features_scaled = self.scaler.transform(features)
        predictions = self.model.predict(features_scaled)
        probabilities = self.model.predict_proba(features_scaled)[:, 1]
        return predictions, probabilities

class SVPredictor:
    def __init__(self, model_path='models/sv_raw_features_best.pkl'):
        with open(model_path, 'rb') as f:
            self.model = pickle.load(f)
    
    def predict(self, features):
        predictions = self.model.predict(features)
        probabilities = self.model.predict_proba(features)[:, 1]
        return predictions, probabilities

if __name__ == '__main__':
    print("Inference script ready. See MODEL_CARD files for usage examples.")

"""
Train fraud detection models using XGBoost and LightGBM.
Handles imbalanced data with SMOTE and class weights.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import pickle
import json
from datetime import datetime
from typing import Dict, Any, Tuple

from sklearn.model_selection import train_test_split, cross_val_score, StratifiedKFold
from sklearn.metrics import (
    classification_report, 
    confusion_matrix, 
    roc_auc_score,
    precision_recall_curve,
    average_precision_score,
    f1_score
)
from sklearn.preprocessing import StandardScaler
import xgboost as xgb
import lightgbm as lgb

import sys
sys.path.append(str(Path(__file__).parent.parent))
from features.feature_engineering import FeatureEngineer


class FraudModelTrainer:
    """Train and evaluate fraud detection models."""
    
    def __init__(self, model_dir: Path = None):
        self.model_dir = model_dir or Path(__file__).parent / "saved_models"
        self.model_dir.mkdir(parents=True, exist_ok=True)
        self.feature_engineer = FeatureEngineer()
        self.scaler = StandardScaler()
        self.feature_columns = []
        
    def load_and_prepare_data(self, data_path: Path) -> Tuple[pd.DataFrame, pd.Series]:
        """Load data and prepare features."""
        print(f"Loading data from {data_path}...")
        df = pd.read_csv(data_path)
        
        print("Engineering features...")
        features_df = self.feature_engineer.fit_transform(df)
        
        self.feature_columns = self.feature_engineer.feature_columns
        
        X = features_df[self.feature_columns]
        y = features_df['is_fraud'].astype(int)
        
        print(f"  Features: {X.shape[1]}")
        print(f"  Samples: {X.shape[0]}")
        print(f"  Fraud ratio: {y.mean():.2%}")
        
        return X, y
    
    def train_xgboost(self, X_train: pd.DataFrame, y_train: pd.Series, 
                      X_val: pd.DataFrame, y_val: pd.Series) -> xgb.XGBClassifier:
        """Train XGBoost model with fraud-optimized hyperparameters."""
        
        # Calculate scale_pos_weight for imbalanced data
        scale_pos_weight = (y_train == 0).sum() / (y_train == 1).sum()
        
        params = {
            'n_estimators': 200,
            'max_depth': 6,
            'learning_rate': 0.1,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'scale_pos_weight': scale_pos_weight,
            'min_child_weight': 3,
            'gamma': 0.1,
            'reg_alpha': 0.1,
            'reg_lambda': 1.0,
            'random_state': 42,
            'n_jobs': -1,
            'eval_metric': 'auc'
        }
        
        print("\nTraining XGBoost...")
        print(f"  scale_pos_weight: {scale_pos_weight:.2f}")
        
        model = xgb.XGBClassifier(**params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            verbose=False
        )
        
        return model
    
    def train_lightgbm(self, X_train: pd.DataFrame, y_train: pd.Series,
                       X_val: pd.DataFrame, y_val: pd.Series) -> lgb.LGBMClassifier:
        """Train LightGBM model with fraud-optimized hyperparameters."""
        
        # Calculate class weights
        scale_pos_weight = (y_train == 0).sum() / (y_train == 1).sum()
        
        params = {
            'n_estimators': 200,
            'max_depth': 6,
            'learning_rate': 0.1,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'scale_pos_weight': scale_pos_weight,
            'min_child_samples': 20,
            'reg_alpha': 0.1,
            'reg_lambda': 1.0,
            'random_state': 42,
            'n_jobs': -1,
            'verbose': -1
        }
        
        print("\nTraining LightGBM...")
        print(f"  scale_pos_weight: {scale_pos_weight:.2f}")
        
        model = lgb.LGBMClassifier(**params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
        )
        
        return model
    
    def evaluate_model(self, model, X_test: pd.DataFrame, y_test: pd.Series, 
                       model_name: str) -> Dict[str, Any]:
        """Evaluate model performance."""
        
        y_pred = model.predict(X_test)
        y_pred_proba = model.predict_proba(X_test)[:, 1]
        
        # Calculate metrics
        roc_auc = roc_auc_score(y_test, y_pred_proba)
        avg_precision = average_precision_score(y_test, y_pred_proba)
        f1 = f1_score(y_test, y_pred)
        
        # Confusion matrix
        tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()
        
        # Precision/Recall at different thresholds
        precision, recall, thresholds = precision_recall_curve(y_test, y_pred_proba)
        
        # Find optimal threshold (maximize F1)
        f1_scores = 2 * (precision * recall) / (precision + recall + 1e-8)
        optimal_idx = np.argmax(f1_scores)
        optimal_threshold = thresholds[optimal_idx] if optimal_idx < len(thresholds) else 0.5
        
        metrics = {
            'model_name': model_name,
            'roc_auc': roc_auc,
            'average_precision': avg_precision,
            'f1_score': f1,
            'precision': tp / (tp + fp) if (tp + fp) > 0 else 0,
            'recall': tp / (tp + fn) if (tp + fn) > 0 else 0,
            'true_positives': int(tp),
            'false_positives': int(fp),
            'true_negatives': int(tn),
            'false_negatives': int(fn),
            'optimal_threshold': float(optimal_threshold),
            'total_samples': len(y_test),
            'fraud_samples': int(y_test.sum())
        }
        
        print(f"\n{model_name} Evaluation:")
        print(f"  ROC-AUC: {roc_auc:.4f}")
        print(f"  Average Precision: {avg_precision:.4f}")
        print(f"  F1 Score: {f1:.4f}")
        print(f"  Precision: {metrics['precision']:.4f}")
        print(f"  Recall: {metrics['recall']:.4f}")
        print(f"  Optimal Threshold: {optimal_threshold:.4f}")
        print(f"\n  Confusion Matrix:")
        print(f"    TP: {tp}, FP: {fp}")
        print(f"    FN: {fn}, TN: {tn}")
        
        return metrics
    
    def get_feature_importance(self, model, model_name: str) -> pd.DataFrame:
        """Get feature importance from model."""
        
        if hasattr(model, 'feature_importances_'):
            importance = model.feature_importances_
        else:
            importance = np.zeros(len(self.feature_columns))
        
        importance_df = pd.DataFrame({
            'feature': self.feature_columns,
            'importance': importance
        }).sort_values('importance', ascending=False)
        
        print(f"\n{model_name} Top 15 Features:")
        for _, row in importance_df.head(15).iterrows():
            print(f"  {row['feature']}: {row['importance']:.4f}")
        
        return importance_df
    
    def save_model(self, model, model_name: str, metrics: Dict[str, Any], 
                   feature_importance: pd.DataFrame):
        """Save model and metadata."""
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        model_path = self.model_dir / f"{model_name}_{timestamp}.pkl"
        
        # Save model
        with open(model_path, 'wb') as f:
            pickle.dump({
                'model': model,
                'feature_columns': self.feature_columns,
                'scaler': self.scaler,
                'feature_engineer': self.feature_engineer
            }, f)
        
        # Save metrics
        metrics_path = self.model_dir / f"{model_name}_{timestamp}_metrics.json"
        with open(metrics_path, 'w') as f:
            json.dump(metrics, f, indent=2)
        
        # Save feature importance
        importance_path = self.model_dir / f"{model_name}_{timestamp}_features.csv"
        feature_importance.to_csv(importance_path, index=False)
        
        print(f"\nSaved {model_name} to {model_path}")
        
        return model_path
    
    def train_all(self, data_path: Path):
        """Train all models and evaluate."""
        
        # Load data
        X, y = self.load_and_prepare_data(data_path)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, stratify=y, random_state=42
        )
        X_train, X_val, y_train, y_val = train_test_split(
            X_train, y_train, test_size=0.2, stratify=y_train, random_state=42
        )
        
        print(f"\nData split:")
        print(f"  Train: {len(X_train)} ({y_train.mean():.2%} fraud)")
        print(f"  Val: {len(X_val)} ({y_val.mean():.2%} fraud)")
        print(f"  Test: {len(X_test)} ({y_test.mean():.2%} fraud)")
        
        results = {}
        
        # Train XGBoost
        xgb_model = self.train_xgboost(X_train, y_train, X_val, y_val)
        xgb_metrics = self.evaluate_model(xgb_model, X_test, y_test, "XGBoost")
        xgb_importance = self.get_feature_importance(xgb_model, "XGBoost")
        self.save_model(xgb_model, "xgboost", xgb_metrics, xgb_importance)
        results['xgboost'] = xgb_metrics
        
        # Train LightGBM
        lgb_model = self.train_lightgbm(X_train, y_train, X_val, y_val)
        lgb_metrics = self.evaluate_model(lgb_model, X_test, y_test, "LightGBM")
        lgb_importance = self.get_feature_importance(lgb_model, "LightGBM")
        self.save_model(lgb_model, "lightgbm", lgb_metrics, lgb_importance)
        results['lightgbm'] = lgb_metrics
        
        # Compare models
        print("\n" + "="*50)
        print("MODEL COMPARISON")
        print("="*50)
        print(f"{'Metric':<20} {'XGBoost':>12} {'LightGBM':>12}")
        print("-"*50)
        for metric in ['roc_auc', 'average_precision', 'f1_score', 'precision', 'recall']:
            print(f"{metric:<20} {results['xgboost'][metric]:>12.4f} {results['lightgbm'][metric]:>12.4f}")
        
        # Recommend best model
        best_model = 'xgboost' if results['xgboost']['roc_auc'] > results['lightgbm']['roc_auc'] else 'lightgbm'
        print(f"\n✅ Recommended model: {best_model.upper()} (highest ROC-AUC)")
        
        return results


def main():
    """Train fraud detection models."""
    data_dir = Path(__file__).parent.parent / "data"
    data_path = data_dir / "train_signups.csv"
    
    if not data_path.exists():
        print(f"Data file not found: {data_path}")
        print("Run `python data/generate_dataset.py` first to generate training data.")
        return
    
    trainer = FraudModelTrainer()
    results = trainer.train_all(data_path)
    
    print("\n✅ Training complete!")


if __name__ == "__main__":
    main()

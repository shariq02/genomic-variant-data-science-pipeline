"""
Model Utilities for Genomic Variant ML Pipeline
================================================
Reusable functions for model training, evaluation, and deployment.

Usage:
    from scripts.model_utils import load_model, evaluate_model
    
    model = load_model('models/my_model.pkl')
    results = evaluate_model(model, X_test, y_test)
"""

import pickle
import json
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, Optional, Union, List

# ML imports
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, average_precision_score, confusion_matrix,
    classification_report, brier_score_loss, log_loss
)
from sklearn.calibration import calibration_curve
import matplotlib.pyplot as plt
import seaborn as sns


# ============================================================================
# MODEL LOADING AND SAVING
# ============================================================================

def load_model(model_path: Union[str, Path]) -> object:
    """
    Load a pickled model.
    
    Args:
        model_path: Path to .pkl model file
        
    Returns:
        Loaded model object
        
    Example:
        model = load_model('models/ensemble_xgb_variants.pkl')
    """
    model_path = Path(model_path)
    
    if not model_path.exists():
        raise FileNotFoundError(f"Model file not found: {model_path}")
    
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    
    print(f"Loaded model from {model_path}")
    return model


def save_model(model: object, model_path: Union[str, Path], 
               overwrite: bool = False) -> None:
    """
    Save a model to pickle file.
    
    Args:
        model: Model object to save
        model_path: Path to save .pkl file
        overwrite: If False, raises error if file exists
        
    Example:
        save_model(trained_model, 'models/new_model.pkl')
    """
    model_path = Path(model_path)
    model_path.parent.mkdir(parents=True, exist_ok=True)
    
    if model_path.exists() and not overwrite:
        raise FileExistsError(f"Model file exists: {model_path}. Use overwrite=True")
    
    with open(model_path, 'wb') as f:
        pickle.dump(model, f)
    
    print(f"Saved model to {model_path}")


def load_scaler(scaler_path: Union[str, Path]) -> object:
    """Load a pickled scaler (StandardScaler, etc.)."""
    return load_model(scaler_path)


def save_scaler(scaler: object, scaler_path: Union[str, Path]) -> None:
    """Save a scaler to pickle file."""
    save_model(scaler, scaler_path, overwrite=True)


# ============================================================================
# MODEL EVALUATION
# ============================================================================

def evaluate_classification(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    y_proba: Optional[np.ndarray] = None,
    labels: Optional[List[str]] = None
) -> Dict:
    """
    Comprehensive classification evaluation.
    
    Args:
        y_true: True labels
        y_pred: Predicted labels
        y_proba: Predicted probabilities (optional)
        labels: Class labels (optional)
        
    Returns:
        Dictionary with all metrics
        
    Example:
        results = evaluate_classification(y_test, y_pred, y_proba)
        print(f"F1: {results['f1']:.3f}")
    """
    results = {
        'accuracy': accuracy_score(y_true, y_pred),
        'precision': precision_score(y_true, y_pred, zero_division=0),
        'recall': recall_score(y_true, y_pred, zero_division=0),
        'f1': f1_score(y_true, y_pred, zero_division=0)
    }
    
    # Add probability-based metrics if available
    if y_proba is not None:
        results['roc_auc'] = roc_auc_score(y_true, y_proba)
        results['pr_auc'] = average_precision_score(y_true, y_proba)
        results['brier_score'] = brier_score_loss(y_true, y_proba)
        results['log_loss'] = log_loss(y_true, y_proba)
    
    # Confusion matrix
    cm = confusion_matrix(y_true, y_pred)
    results['confusion_matrix'] = cm.tolist()
    results['tn'], results['fp'], results['fn'], results['tp'] = cm.ravel()
    
    # Classification report
    if labels:
        results['classification_report'] = classification_report(
            y_true, y_pred, target_names=labels, output_dict=True
        )
    
    return results


def evaluate_model(
    model: object,
    X: pd.DataFrame,
    y: np.ndarray,
    model_name: str = "model",
    verbose: bool = True
) -> Dict:
    """
    Evaluate a model and return comprehensive metrics.
    
    Args:
        model: Trained model
        X: Features
        y: True labels
        model_name: Name for display
        verbose: Print results
        
    Returns:
        Dictionary with metrics
        
    Example:
        results = evaluate_model(xgb_model, X_test, y_test, "XGBoost")
    """
    # Make predictions
    y_pred = model.predict(X)
    
    # Get probabilities if available
    try:
        y_proba = model.predict_proba(X)[:, 1]
    except:
        y_proba = None
    
    # Evaluate
    results = evaluate_classification(y, y_pred, y_proba)
    results['model_name'] = model_name
    results['n_samples'] = len(y)
    
    if verbose:
        print(f"\n{model_name} Evaluation:")
        print(f"  Accuracy:  {results['accuracy']:.4f}")
        print(f"  Precision: {results['precision']:.4f}")
        print(f"  Recall:    {results['recall']:.4f}")
        print(f"  F1-Score:  {results['f1']:.4f}")
        if y_proba is not None:
            print(f"  ROC-AUC:   {results['roc_auc']:.4f}")
    
    return results


def compare_models(
    models: Dict[str, object],
    X: pd.DataFrame,
    y: np.ndarray
) -> pd.DataFrame:
    """
    Compare multiple models on same data.
    
    Args:
        models: Dictionary of {name: model}
        X: Features
        y: True labels
        
    Returns:
        DataFrame with comparison
        
    Example:
        models = {'XGBoost': xgb, 'RandomForest': rf, 'LightGBM': lgb}
        comparison = compare_models(models, X_test, y_test)
        print(comparison.sort_values('f1', ascending=False))
    """
    results = []
    
    for name, model in models.items():
        metrics = evaluate_model(model, X, y, name, verbose=False)
        results.append({
            'model': name,
            'accuracy': metrics['accuracy'],
            'precision': metrics['precision'],
            'recall': metrics['recall'],
            'f1': metrics['f1'],
            'roc_auc': metrics.get('roc_auc', np.nan)
        })
    
    df = pd.DataFrame(results)
    return df.sort_values('f1', ascending=False)


# ============================================================================
# METADATA AND REPORTING
# ============================================================================

def save_model_metadata(
    model_name: str,
    metrics: Dict,
    hyperparameters: Dict,
    output_path: Union[str, Path],
    additional_info: Optional[Dict] = None
) -> None:
    """
    Save model metadata to JSON.
    
    Args:
        model_name: Model identifier
        metrics: Performance metrics
        hyperparameters: Model hyperparameters
        output_path: Path to save JSON
        additional_info: Any additional metadata
        
    Example:
        save_model_metadata(
            'druggability_xgboost_v1',
            {'f1': 0.85, 'roc_auc': 0.92},
            {'n_estimators': 200, 'max_depth': 7},
            'models/metadata/druggability_metadata.json'
        )
    """
    metadata = {
        'model_name': model_name,
        'created_date': datetime.now().isoformat(),
        'metrics': metrics,
        'hyperparameters': hyperparameters
    }
    
    if additional_info:
        metadata.update(additional_info)
    
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"Saved metadata to {output_path}")


def create_experiment_log(
    experiment_name: str,
    model_type: str,
    results: Dict,
    save_path: Optional[Union[str, Path]] = None
) -> Dict:
    """
    Create a structured experiment log.
    
    Args:
        experiment_name: Name of experiment
        model_type: Type of model
        results: Results from evaluate_model()
        save_path: Optional path to save JSON
        
    Returns:
        Experiment log dictionary
        
    Example:
        log = create_experiment_log(
            'druggability_experiment_1',
            'regression',
            results,
            'reports/experiments.json'
        )
    """
    log = {
        'experiment_name': experiment_name,
        'model_type': model_type,
        'timestamp': datetime.now().isoformat(),
        'results': results
    }
    
    if save_path:
        save_path = Path(save_path)
        
        # Append to existing file or create new
        if save_path.exists():
            with open(save_path, 'r') as f:
                logs = json.load(f)
            logs.append(log)
        else:
            logs = [log]
        
        with open(save_path, 'w') as f:
            json.dump(logs, f, indent=2)
        
        print(f"Appended experiment log to {save_path}")
    
    return log


# ============================================================================
# VISUALIZATION
# ============================================================================

def plot_confusion_matrix(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    labels: Optional[List[str]] = None,
    title: str = "Confusion Matrix",
    save_path: Optional[Union[str, Path]] = None,
    figsize: Tuple[int, int] = (8, 6)
) -> None:
    """
    Plot confusion matrix.
    
    Args:
        y_true: True labels
        y_pred: Predicted labels
        labels: Class labels
        title: Plot title
        save_path: Optional path to save figure
        figsize: Figure size
        
    Example:
        plot_confusion_matrix(
            y_test, y_pred,
            labels=['Benign', 'Pathogenic'],
            save_path='figures/cm.png'
        )
    """
    cm = confusion_matrix(y_true, y_pred)
    
    fig, ax = plt.subplots(figsize=figsize)
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=ax)
    
    if labels:
        ax.set_xticklabels(labels)
        ax.set_yticklabels(labels)
    
    ax.set_xlabel('Predicted')
    ax.set_ylabel('Actual')
    ax.set_title(title)
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"Saved confusion matrix to {save_path}")
    
    plt.show()


def plot_roc_curve(
    y_true: np.ndarray,
    y_proba: np.ndarray,
    title: str = "ROC Curve",
    save_path: Optional[Union[str, Path]] = None,
    figsize: Tuple[int, int] = (8, 6)
) -> None:
    """
    Plot ROC curve.
    
    Args:
        y_true: True labels
        y_proba: Predicted probabilities
        title: Plot title
        save_path: Optional path to save
        figsize: Figure size
    """
    from sklearn.metrics import roc_curve
    
    fpr, tpr, thresholds = roc_curve(y_true, y_proba)
    roc_auc = roc_auc_score(y_true, y_proba)
    
    fig, ax = plt.subplots(figsize=figsize)
    ax.plot(fpr, tpr, linewidth=2, label=f'ROC (AUC = {roc_auc:.3f})')
    ax.plot([0, 1], [0, 1], 'k--', linewidth=1, label='Random')
    
    ax.set_xlabel('False Positive Rate')
    ax.set_ylabel('True Positive Rate')
    ax.set_title(title)
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"Saved ROC curve to {save_path}")
    
    plt.show()


def plot_feature_importance(
    model: object,
    feature_names: List[str],
    top_n: int = 20,
    title: str = "Feature Importance",
    save_path: Optional[Union[str, Path]] = None,
    figsize: Tuple[int, int] = (10, 8)
) -> pd.DataFrame:
    """
    Plot feature importance from tree-based model.
    
    Args:
        model: Trained model with feature_importances_
        feature_names: List of feature names
        top_n: Number of top features to show
        title: Plot title
        save_path: Optional save path
        figsize: Figure size
        
    Returns:
        DataFrame with feature importances
        
    Example:
        importance_df = plot_feature_importance(
            xgb_model,
            X_train.columns,
            top_n=15,
            save_path='figures/importance.png'
        )
    """
    # Get feature importances
    importances = model.feature_importances_
    
    # Create dataframe
    importance_df = pd.DataFrame({
        'feature': feature_names,
        'importance': importances
    }).sort_values('importance', ascending=False)
    
    # Plot top N
    top_features = importance_df.head(top_n)
    
    fig, ax = plt.subplots(figsize=figsize)
    ax.barh(range(len(top_features)), top_features['importance'])
    ax.set_yticks(range(len(top_features)))
    ax.set_yticklabels(top_features['feature'])
    ax.invert_yaxis()
    ax.set_xlabel('Importance')
    ax.set_title(title)
    ax.grid(True, alpha=0.3, axis='x')
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"Saved feature importance to {save_path}")
    
    plt.show()
    
    return importance_df


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def calculate_metrics_by_threshold(
    y_true: np.ndarray,
    y_proba: np.ndarray,
    thresholds: List[float] = [0.3, 0.4, 0.5, 0.6, 0.7]
) -> pd.DataFrame:
    """
    Calculate metrics at different probability thresholds.
    
    Args:
        y_true: True labels
        y_proba: Predicted probabilities
        thresholds: List of thresholds to test
        
    Returns:
        DataFrame with metrics per threshold
        
    Example:
        threshold_analysis = calculate_metrics_by_threshold(
            y_test, y_proba, [0.3, 0.5, 0.7, 0.9]
        )
    """
    results = []
    
    for thresh in thresholds:
        y_pred = (y_proba >= thresh).astype(int)
        
        results.append({
            'threshold': thresh,
            'accuracy': accuracy_score(y_true, y_pred),
            'precision': precision_score(y_true, y_pred, zero_division=0),
            'recall': recall_score(y_true, y_pred, zero_division=0),
            'f1': f1_score(y_true, y_pred, zero_division=0)
        })
    
    return pd.DataFrame(results)


def print_summary(results: Dict, title: str = "Model Performance") -> None:
    """
    Print formatted summary of results.
    
    Args:
        results: Dictionary from evaluate_model()
        title: Title to display
        
    Example:
        print_summary(results, "XGBoost Performance")
    """
    print("\n" + "="*80)
    print(title.center(80))
    print("="*80)
    
    print(f"\nPerformance Metrics:")
    print(f"  Accuracy:  {results.get('accuracy', 0):.4f}")
    print(f"  Precision: {results.get('precision', 0):.4f}")
    print(f"  Recall:    {results.get('recall', 0):.4f}")
    print(f"  F1-Score:  {results.get('f1', 0):.4f}")
    
    if 'roc_auc' in results:
        print(f"  ROC-AUC:   {results['roc_auc']:.4f}")
    
    if 'confusion_matrix' in results:
        cm = np.array(results['confusion_matrix'])
        print(f"\nConfusion Matrix:")
        print(f"  TN: {cm[0,0]:,}  FP: {cm[0,1]:,}")
        print(f"  FN: {cm[1,0]:,}  TP: {cm[1,1]:,}")
    
    print("="*80)


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == '__main__':
    print("Model Utilities Module")
    print("="*80)
    print("\nAvailable functions:")
    print("  - load_model(), save_model()")
    print("  - evaluate_model(), compare_models()")
    print("  - save_model_metadata(), create_experiment_log()")
    print("  - plot_confusion_matrix(), plot_roc_curve()")
    print("  - plot_feature_importance()")
    print("  - calculate_metrics_by_threshold()")
    print("\nImport with: from scripts.model_utils import *")
    print("="*80)

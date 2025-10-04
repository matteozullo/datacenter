import pandas as pd
import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression, LogisticRegressionCV


# -------------------------------
# Constants Definition
# -------------------------------
INPUT_FILE = "LBNL_Ix_Queue_Data_File_thru2024_v2.xlsx"
SHEET_NAME = "03. Complete Queue Data"
OUTPUT_FILE = "lbnl_data_with_predictions.csv"


# -------------------------------
# Preprocess full dataset
# -------------------------------
def prob_inter_prep_full(data):
    df = data.copy()

    # Debug: Check initial data shape and global overview
    print(f"Initial data shape: {df.shape}")

    # Filter by prop_year
    df = df[(df['prop_year'] <= 2023) | df['prop_year'].isna()]
    print(f"After prop_year filter: {df.shape}")

    # Create outcome variable
    df['outcome'] = np.nan
    df.loc[df['q_status'].isin(['active', 'operational']), 'outcome'] = 1
    df.loc[df['q_status'].isin(['suspended', 'withdrawn']), 'outcome'] = 0

    # Debug: Global outcome distribution (for overview only)
    print(f"Global outcome distribution: {df['outcome'].value_counts(dropna=False).to_dict()}")
    print(f"Entities in full dataset: {df['entity'].nunique() if 'entity' in df.columns else 'N/A'}")

    # Drop rows with missing outcome
    df = df.dropna(subset=['outcome'])
    print(f"After dropping missing outcomes: {df.shape}")

    # Check required columns exist
    required_cols = ['entity', 'outcome', 'mw1', 'type_clean', 'fips_codes']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"Warning: Missing columns: {missing_cols}")
        # Use available columns only
        available_cols = [col for col in required_cols if col in df.columns]
        df = df[available_cols]
    else:
        df = df[required_cols]

    print(f"Final preprocessed data shape: {df.shape}")
    print(f"Entities in data: {df['entity'].nunique() if 'entity' in df.columns else 'N/A'}")

    return df

# -------------------------------
# Pull training set for entity
# -------------------------------
def get_entity_training_set(prep_data, entity):
    entity_data = prep_data[prep_data['entity'] == entity].copy()
    print(f"Training data for entity {entity}: {len(entity_data)} samples")

    # Show entity-specific outcome distribution
    if 'outcome' in entity_data.columns:
        outcome_dist = entity_data['outcome'].value_counts(dropna=False)
        print(f"Entity {entity} outcome distribution: {outcome_dist.to_dict()}")

    return entity_data

# -------------------------------
# Check training data viability
# -------------------------------
def check_training_data_viability(train_data, predictors):
    """Check if training data can support regression with these predictors"""

    # Check for sufficient variation in each predictor
    for pred in predictors:
        if pred not in train_data.columns:
            print(f"Training data missing column: {pred}")
            return False

        # For numerical columns, need >1 unique values
        if train_data[pred].dtype in ['int64', 'float64']:
            if train_data[pred].nunique() <= 1:
                print(f"Insufficient variation in numerical predictor {pred}: {train_data[pred].nunique()} unique values")
                return False
        # For categorical columns, need >=2 unique values
        else:
            if train_data[pred].nunique() < 2:
                print(f"Insufficient variation in categorical predictor {pred}: {train_data[pred].nunique()} unique values")
                return False

    # Check sample size after dropping missing values
    train_clean = train_data[predictors + ['outcome']].dropna()
    if len(train_clean) < 2:
        print(f"Insufficient training samples after dropping NAs: {len(train_clean)}")
        return False

    # Check class balance
    if train_clean['outcome'].nunique() < 2:
        print(f"Only {train_clean['outcome'].nunique()} outcome class(es) in clean training data")
        return False

    print(f"Training data viable: {len(train_clean)} samples, {train_clean['outcome'].nunique()} classes")
    return True

# -------------------------------
# Get viable predictor combinations for training data
# -------------------------------
def get_viable_predictor_combinations(train_data):
    """
    Get all predictor combinations that are viable for the training data.
    Returns list of predictor lists in order of preference (most complex first).
    """
    print(f"Evaluating predictor combinations for training data ({len(train_data)} samples)")

    # Define all possible predictor combinations in order of preference
    all_combinations = [
        ['mw1', 'type_clean', 'fips_codes'],  # Full model
        ['mw1', 'type_clean'],                # Without location
        ['mw1', 'fips_codes'],                # Without type
        ['mw1']                               # Minimal model
    ]

    viable_combinations = []

    for combo in all_combinations:
        print(f"Checking training viability for: {combo}")

        if check_training_data_viability(train_data, combo):
            viable_combinations.append(combo)
            print(f"✓ Training viable: {combo}")
        else:
            print(f"✗ Training not viable: {combo}")

    return viable_combinations

# -------------------------------
# Fit a single model for specific predictors
# -------------------------------
def fit_single_model(train_data, predictors, penalized=False):
    """
    Fit a single model for the given predictors.
    Returns the fitted pipeline or None if fitting fails.
    """
    # Clean training data
    train_clean = train_data[predictors + ['outcome']].dropna()

    if len(train_clean) < 2:
        return None

    X_train = train_clean[predictors]
    y_train = train_clean['outcome']

    if y_train.nunique() < 2:
        return None

    # Set up preprocessing
    cat_cols = X_train.select_dtypes(include=['object', 'category']).columns.tolist()

    if cat_cols:
        preprocessor = ColumnTransformer(
            transformers=[('cat', OneHotEncoder(drop='first', handle_unknown='ignore'), cat_cols)],
            remainder='passthrough'
        )
    else:
        preprocessor = ColumnTransformer(transformers=[], remainder='passthrough')

    # Set up model
    if penalized:
        model = LogisticRegressionCV(
            penalty='l2',
            Cs=10,
            cv=min(5, len(train_clean)),
            solver='lbfgs',
            scoring='neg_log_loss',
            max_iter=1000,
            n_jobs=-1
        )
    else:
        model = LogisticRegression(
            penalty=None,
            solver='lbfgs',
            max_iter=1000,
            n_jobs=-1
        )

    # Create and fit pipeline
    clf = Pipeline([('pre', preprocessor), ('model', model)])

    try:
        clf.fit(X_train, y_train)
        return clf
    except Exception as e:
        print(f"Model fitting failed for {predictors}: {str(e)}")
        return None

# -------------------------------
# Row-wise prediction with multiple models
# -------------------------------
def predict_with_row_wise_selection(train_data, prediction_data, viable_predictors, penalized=False):
    """
    Make predictions using the best available predictors for each row.
    Each row gets predicted using the most complex model for which it has complete data.
    """
    print(f"Making row-wise predictions for {len(prediction_data)} samples")

    predictions = np.full(len(prediction_data), np.nan)
    prediction_counts = {str(combo): 0 for combo in viable_predictors}

    # Pre-fit all viable models
    fitted_models = {}
    for predictors in viable_predictors:
        print(f"Fitting model for predictors: {predictors}")
        model = fit_single_model(train_data, predictors, penalized=penalized)
        if model is not None:
            fitted_models[str(predictors)] = model
            print(f"✓ Model fitted for {predictors}")
        else:
            print(f"✗ Model fitting failed for {predictors}")

    if not fitted_models:
        print("No models could be fitted")
        return predictions

    # For each row, find the best available predictor set and make prediction
    for idx, (_, row) in enumerate(prediction_data.iterrows()):
        best_predictors = None

        # Try predictor combinations in order of preference (most complex first)
        for predictors in viable_predictors:
            # Check if this row has all required predictors non-missing
            if all(pd.notna(row[pred]) for pred in predictors if pred in prediction_data.columns):
                best_predictors = predictors
                break

        if best_predictors is not None and str(best_predictors) in fitted_models:
            # Make prediction using the best available model
            model = fitted_models[str(best_predictors)]
            row_data = pd.DataFrame([row])

            try:
                pred_proba = model.predict_proba(row_data[best_predictors])
                if pred_proba.shape[1] == 2:
                    predictions[idx] = pred_proba[0, 1]
                else:
                    predictions[idx] = pred_proba[0, 0]

                prediction_counts[str(best_predictors)] += 1

            except Exception as e:
                print(f"Prediction failed for row {idx} with predictors {best_predictors}: {str(e)}")

    # Print summary statistics
    print("Prediction summary:")
    for combo, count in prediction_counts.items():
        if count > 0:
            print(f"  {combo}: {count} predictions")

    valid_predictions = predictions[~np.isnan(predictions)]
    print(f"Total valid predictions: {len(valid_predictions)}/{len(predictions)}")

    return predictions

# -------------------------------
# Vectorized prediction
# -------------------------------
def prob_inter_vectorized(prep_data, data, penalized=False):
    predictions = np.full(len(data), np.nan)

    print(f"Making predictions for {len(data)} samples")

    # Check if entity column exists in both datasets
    if 'entity' not in data.columns:
        print("Error: 'entity' column not found in prediction data")
        return predictions

    if 'entity' not in prep_data.columns:
        print("Error: 'entity' column not found in training data")
        return predictions

    # Group by entity
    grouped = data.groupby('entity')
    print(f"Processing {len(grouped)} entities")

    for i, (entity, group) in enumerate(grouped):
        print(f"\nProcessing entity {i+1}/{len(grouped)}: {entity}")

        # Get training data for this entity
        train_data = get_entity_training_set(prep_data, entity)

        if len(train_data) == 0:
            print(f"No training data found for entity {entity}")
            continue

        # Get all viable predictor combinations for this entity's training data
        viable_predictors = get_viable_predictor_combinations(train_data)

        if not viable_predictors:
            print(f"No viable predictor combinations for entity {entity}")
            continue

        print(f"Viable predictor combinations: {viable_predictors}")

        # Make predictions using row-wise predictor selection
        preds = predict_with_row_wise_selection(train_data, group, viable_predictors, penalized=penalized)

        # Assign predictions back to original indices
        predictions[group.index] = preds

        # Print some stats
        valid_preds = preds[~np.isnan(preds)]
        if len(valid_preds) > 0:
            print(f"Generated {len(valid_preds)} valid predictions (mean: {valid_preds.mean():.3f})")
        else:
            print("No valid predictions generated")

    # Summary statistics
    valid_predictions = predictions[~np.isnan(predictions)]
    print(f"\nFinal summary:")
    print(f"Total predictions: {len(predictions)}")
    print(f"Valid predictions: {len(valid_predictions)}")
    print(f"NaN predictions: {np.sum(np.isnan(predictions))}")

    if len(valid_predictions) > 0:
        print(f"Prediction range: [{valid_predictions.min():.3f}, {valid_predictions.max():.3f}]")
        print(f"Mean prediction: {valid_predictions.mean():.3f}")

    return predictions

# -------------------------------
# Main Execution
# -------------------------------
def main():
    """
    Main function to orchestrate data loading, preprocessing, prediction, and saving.
    Handles errors related to file loading and processing.
    """
    try:
        # Step 1: Load data
        print("Loading data...")
        lbnl_data = pd.read_excel(INPUT_FILE, sheet_name=SHEET_NAME, skiprows=SKIP_ROWS)
        print(f"Loaded data shape: {lbnl_data.shape}")
        print(f"Columns: {list(lbnl_data.columns)}")

        # Step 2: Preprocess data
        print("\nPreprocessing data...")
        prep_data = prob_inter_prep_full(lbnl_data)

        if len(prep_data) == 0:
            print("Error: No data remaining after preprocessing")
            return

        # Step 3: Generate predictions
        print("\nGenerating predictions...")
        lbnl_data['pred_logit'] = prob_inter_vectorized(prep_data, lbnl_data, penalized=False)
        lbnl_data['pred_penalized'] = prob_inter_vectorized(prep_data, lbnl_data, penalized=True)

        # Step 4: Save results
        print("\nSaving results...")
        lbnl_data.to_csv(OUTPUT_FILE, index=False)
        print(f"Results saved to {OUTPUT_FILE}")

        # Step 5: Display sample results
        print("\nSample predictions:")
        available_sample_cols = [col for col in SAMPLE_COLUMNS if col in lbnl_data.columns]
        print(lbnl_data[available_sample_cols].head(10))

    except FileNotFoundError:
        print(f"Error: Could not find the Excel file '{INPUT_FILE}'")
        print("Please make sure the file is in the current directory.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
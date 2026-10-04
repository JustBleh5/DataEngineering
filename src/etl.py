import os
import pandas as pd


# -----------------------------
# File paths
# -----------------------------
INPUT_FILE = "data/raw/orders.csv"
PROCESSED_FILE = "data/processed/orders_clean.csv"
REJECTED_FILE = "data/rejected/orders_rejected.csv"


def extract_data(file_path):
    """
    Read the raw CSV file.
    """
    print("Starting extraction...")

    df = pd.read_csv(file_path)

    print(f"Successfully extracted {len(df)} rows.")

    return df


def validate_and_transform(df):
    """
    Validate the raw data and separate valid and rejected records.
    """

    print("Starting validation and transformation...")

    # Working on a copy so we do not modify the original dataframe directly
    data = df.copy()

    # ----------------------------------
    # Convert order_date to datetime
    # ----------------------------------
    # Invalid dates will become NaT
    data["order_date"] = pd.to_datetime(
        data["order_date"],
        errors="coerce"
    )

    # ----------------------------------
    # Create rejection reasons
    # ----------------------------------
    data["rejection_reason"] = ""

    # Missing customer_id
    missing_customer = data["customer_id"].isna()#with .isna it return True for all the na values and the reverse
    data.loc[missing_customer,"rejection_reason"] += "Missing customer_id; "#.loc returns all the True marked values

    # Invalid quantity
    invalid_quantity = data["quantity"] <= 0

    data.loc[
        invalid_quantity,"rejection_reason"] += "Quantity must be greater than 0; "

    # Invalid order date
    invalid_date = data["order_date"].isna()

    data.loc[
        invalid_date,
        "rejection_reason"
    ] += "Invalid order_date; "

    # ----------------------------------
    # Duplicate order IDs
    # ----------------------------------
    duplicate_order = data.duplicated(
        subset=["order_id"],
        keep="first"
    )

    data.loc[
        duplicate_order,
        "rejection_reason"
    ] += "Duplicate order_id; "

    # ----------------------------------
    # Separate rejected and valid data
    # ----------------------------------
    rejected_df = data[data["rejection_reason"] != ""].copy()

    valid_df = data[
        data["rejection_reason"] == ""
    ].copy()

    # ----------------------------------
    # Transformation
    # ----------------------------------
    valid_df["total_amount"] = (
        valid_df["quantity"]
        * valid_df["unit_price"]
    )

    # Drop rejection_reason from valid dataset
    valid_df.drop(
        columns=["rejection_reason"],
        inplace=True
    )

    print("Validation and transformation completed.")

    return valid_df, rejected_df


def load_data(valid_df, rejected_df):
    """
    Save valid and rejected data into separate files.
    """

    print("Starting load process...")

    # Ensure folders exist
    os.makedirs(
        os.path.dirname(PROCESSED_FILE),
        exist_ok=True
    )

    os.makedirs(
        os.path.dirname(REJECTED_FILE),
        exist_ok=True
    )

    # Save valid data
    valid_df.to_csv(
        PROCESSED_FILE,
        index=False
    )

    # Save rejected data
    rejected_df.to_csv(
        REJECTED_FILE,
        index=False
    )#index=False doesnt save the row index numbers in the actual csv

    print("Load completed.")


def run_pipeline():

    print("\n==============================")
    print("ShopStream ETL Pipeline")
    print("==============================\n")

    # Extract
    raw_df = extract_data(INPUT_FILE)

    total_rows = len(raw_df)

    # Transform + Validate
    valid_df, rejected_df = validate_and_transform(raw_df)

    valid_rows = len(valid_df)
    rejected_rows = len(rejected_df)

    # Load
    load_data(
        valid_df,
        rejected_df
    )

    # ----------------------------------
    # Pipeline Summary
    # ----------------------------------

    print("\n==============================")
    print("ETL PIPELINE SUMMARY")
    print("==============================")

    print(f"Total rows read : {total_rows}")
    print(f"Valid rows      : {valid_rows}")
    print(f"Rejected rows   : {rejected_rows}")

    print("==============================")

    # Reconciliation check
    if total_rows == valid_rows + rejected_rows:
        print("Reconciliation check: PASSED")
    else:
        print("Reconciliation check: FAILED")

    print("\nETL Pipeline Completed Successfully.\n")


if __name__ == "__main__":
    run_pipeline()
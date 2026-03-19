import pandas as pd
from dateutil.relativedelta import relativedelta
from config_903 import DateCols903, EthnicSubcategories
import numpy as np


def format_dates(column):
    column.replace(r"^\s*$", pd.NaT, regex=True)
    column = column.fillna(pd.NaT)
    try:
        column = pd.to_datetime(column, format="%d/%m/%Y")
        return column
    except:
        raise ValueError(f"Invalid date format in {column.name}, expected dd/mm/YYYY")


def calculate_age_buckets(age):
    # Used to make age buckets to mirror published data
    if age < 1:
        return "a) Under 1 year"
    elif age < 5:
        return "b) 1 to 4 years"
    elif age < 10:
        return "c) 5 to 9 years"
    elif age < 16:
        return "d) 10 to 16 years"
    elif age >= 16:
        return "e) 16 years and over"
    else:
        return "f) Age error"


def clean_903_table(df: pd.DataFrame, collection_end: pd.Timestamp):
    clean_df = df.copy()

    if "index" in df.columns:
        clean_df.drop("index", axis=1, inplace=True)

    for column in clean_df.columns:
        if column in DateCols903.cols.value:
            clean_df[f"{column}_dt"] = format_dates(clean_df[column])

    if "ETHNIC" in clean_df.columns:
        clean_df["ETHNICITY"] = clean_df["ETHNIC"].apply(
            lambda ethnicity: EthnicSubcategories[ethnicity].value
        )

    if "DOB_dt" in clean_df.columns:
        clean_df["AGE"] = clean_df["DOB_dt"].apply(
            lambda dob: relativedelta(dt1=collection_end, dt2=dob).normalized().years
        )

        clean_df["AGE_BUCKETS"] = clean_df["AGE"].apply(calculate_age_buckets)

    return clean_df


def group_calcuation(df, column, measure_name):
    """
    Function to group a dataframe by a column and calculate count and percentage for each group.

    """
    grouped = df.groupby(column).size()
    grouped = grouped.to_frame(f"{measure_name} - Count").reset_index()
    grouped = grouped.rename(columns={column: "Value"})

    grouped["Header - Ethncities - Percentage"] = (
        grouped[f"{measure_name} - Count"] / grouped[f"{measure_name} - Count"].sum()
    ) * 100
    return grouped


def time_difference(start_col, end_col, business_days=False):
    """
    Function to calculate the time difference between two date columns in days.
    """
    if business_days:
        # np.busday_count can only use datetime64[D] format,
        # so we need to convert the columns to this format
        time_diff = np.busday_count(
            start_col.values.astype("datetime64[D]"),
            end_col.values.astype("datetime64[D]"),
        )
    else:
        time_diff = end_col - start_col
        time_diff = time_diff / pd.Timedelta(days=1)

    return time_diff.astype("int")


def multiples_same_event(df, col_name):
    """
    Function to identify multiple records for the same event
    """
    df = df.copy()
    multiples = df.groupby("CHILD").size().to_frame("Number of events").reset_index()
    multiples = (
        multiples.groupby("Number of events")
        .size()
        .to_frame("Children with number of events")
        .reset_index()
    )
    multiples["Event Type"] = col_name
    multiples = multiples[
        ["Event Type", "Number of events", "Children with number of events"]
    ]
    return multiples


def group_calcuation_year(df, year_col, col_to_group, measure_name):
    df = df.copy()
    grouped = df.groupby([year_col, col_to_group]).size()
    grouped = grouped.to_frame("Count").reset_index()
    grouped = grouped.rename(columns={col_to_group: "Value"})

    grouped["Percentage by year"] = grouped.apply(
        lambda x: x["Count"]
        / grouped.loc[grouped[year_col] == x[year_col], "Count"].sum()
        * 100,
        axis=1,
    )

    grouped["Measure"] = measure_name
    grouped = grouped[[year_col, "Measure", "Value", "Count", "Percentage by year"]]
    return grouped


def percent_of_col_with_value(df, col, measure_name):
    df = df.copy()
    df[col] = df[col].fillna("No")
    grouped = group_calcuation(df, col, measure_name)
    return grouped


def appears_on_both(df1, df2, measure_name):
    """
    Function to identify records that appear in both dataframes
    """
    df1 = df1.drop_duplicates(subset=["CHILD"]).copy()
    df2 = df2.drop_duplicates(subset=["CHILD"]).copy()
    merged_df = df1.merge(df2, on="CHILD", how="inner")
    merged_df["on_both"] = "Yes"
    df = df1.merge(merged_df[["CHILD", "on_both"]], how="left", on="CHILD")
    df.fillna({"on_both": "No"}, inplace=True)
    output = percent_of_col_with_value(df, "on_both", measure_name)
    return output


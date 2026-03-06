import pandas as pd
from dateutil.relativedelta import relativedelta
from config_903 import DateCols903, EthnicSubcatgories

def format_dates(column):
    column = column.replace(r"^\s*$", pd.NaT, regex=True)
    column = column.fillna(pd.NaT)
    try:
        column = pd.to_datetime(column, format="%d/%m/%Y")
        return column
    except:
        raise ValueError(
            f"Error converting column {column.name} to datetime."
        )

def calculate_age_buckets(age):
    # Used to make age buckets matching published data
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
        def map_ethnicity(ethnicity):
            try:
                return EthnicSubcatgories[ethnicity].value
            except KeyError:
                return f"Unknown ({ethnicity})"  # Handle unknown ethnicity codes
        
        clean_df['ETHNICITY'] = clean_df['ETHNIC'].apply(map_ethnicity)

    if "DOB_dt" in clean_df.columns:
        clean_df['AGE'] = clean_df['DOB_dt'].apply(
            lambda dob: relativedelta(dt1=collection_end, dt2=dob).normalized().years
        )
        clean_df['AGE_BUCKETS'] = clean_df['AGE'].apply(calculate_age_buckets)
        
        clean_df['AGE_BUCKETS'] = clean_df['AGE'].apply(calculate_age_buckets)
    
    return clean_df
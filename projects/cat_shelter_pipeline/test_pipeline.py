import pytest
import pandas as pd
from pipeline import transform_cat_data

def test_transform_cat_data_success():
    """Test that valid nested JSON data is successfully flattened and standardized."""
    # Simulate a messy, nested raw JSON response from the API
    mock_raw_data = [
        {
            "type": "animals",
            "id": "12345",
            "attributes": {
                "name": "Fluffy",
                "ageGroup": "Adult",
                "isAdoptionPending": False
            }
        }
    ]
    
    # Run the transformation logic
    df = transform_cat_data(mock_raw_data)
    
    # Assertions: Verify the output matches our engineering requirements
    assert isinstance(df, pd.DataFrame), "Output must be a Pandas DataFrame"
    assert len(df) == 1, "DataFrame should contain exactly 1 record"
    
    # Verify our custom analyst logic: columns should be flattened and snake_case
    assert "attributes_name" in df.columns, "Nested JSON keys should be flattened with underscores"
    assert "attributes_agegroup" in df.columns, "Column names should be strictly lowercased"
    assert "attributes_isadoptionpending" in df.columns, "CamelCase keys should be entirely lowercased"

def test_transform_cat_data_empty():
    """Test that the function gracefully handles empty API responses without crashing."""
    mock_empty_data = []
    
    df = transform_cat_data(mock_empty_data)
    
    assert isinstance(df, pd.DataFrame), "Output must still be a Pandas DataFrame"
    assert df.empty, "DataFrame should be empty when given empty raw data"
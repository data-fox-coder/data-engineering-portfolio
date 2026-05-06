import pytest
import pandas as pd
from pipeline import transform_cat_data

@pytest.fixture
def mock_config():
    """Provides a controlled mock configuration matching your new Silver layer contract."""
    return {
        "layers": {
            "silver": {
                "deduplicate": True,
                "fields_to_keep": [
                    "id",
                    "attributes_name",
                    "attributes_agegroup",
                    "attributes_isadoptionpending"
                ]
            }
        }
    }

def test_transform_cat_data_success(mock_config):
    """Test that valid nested JSON data is successfully flattened, filtered, and standardized."""
    # Simulate a messy, nested raw JSON response from the API
    mock_raw_data = [
        {
            "type": "animals",
            "id": "12345",
            "attributes": {
                "name": "Fluffy",
                "ageGroup": "Adult",
                "isAdoptionPending": False,
                "someExtraField": "Should be filtered out"  # Field not in fields_to_keep
            }
        },
        {
            "type": "animals",
            "id": "12345",  # Duplicate ID to test deduplication
            "attributes": {
                "name": "Fluffy Duplicate",
                "ageGroup": "Adult",
                "isAdoptionPending": False
            }
        }
    ]
    
    # Run the transformation logic passing our mock configuration
    df = transform_cat_data(mock_raw_data, mock_config)
    
    # Assertions: Verify the output matches our engineering requirements
    assert isinstance(df, pd.DataFrame), "Output must be a Pandas DataFrame"
    assert len(df) == 1, "DataFrame should contain exactly 1 record after deduplication"
    
    # Verify column handling and selection contract
    assert "attributes_name" in df.columns, "Nested JSON keys should be flattened with underscores"
    assert "attributes_agegroup" in df.columns, "Column names should be strictly lowercased"
    assert "someextrafield" not in df.columns, "Fields omitted from config should be discarded"

def test_transform_cat_data_empty(mock_config):
    """Test that the function gracefully handles empty API responses without crashing."""
    mock_empty_data = []
    
    df = transform_cat_data(mock_empty_data, mock_config)
    
    assert isinstance(df, pd.DataFrame), "Output must still be a Pandas DataFrame"
    assert df.empty, "DataFrame should be empty when given empty raw data"
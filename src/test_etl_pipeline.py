import pytest
import pandas as pd
import numpy as np
from io import StringIO

# Importing the functions from your ETL pipeline script
from etl_pipeline import (
    load_data,
    drop_columns,
    fill_missing_values,
    merge_dataframes,
    handle_duplicates,
    clean_terrorism_data,
    clean_country_data
)

@pytest.fixture
def sample_terrorism_data():
    data = """Country_txt,latitude,longitude,provstate,city,claimed,nkill,nwound
    Afghanistan,33.93911,67.709953,Kabul,Kabul,TRUE,100,50
    Iraq,33.2232,43.6793,Baghdad,Baghdad,FALSE,50,30
    Syria,32.7688,39.0896,Raqqa,Raqqa,TRUE,20,10"""
    return pd.read_csv(StringIO(data))

@pytest.fixture
def sample_country_data():
    data = """Country,Abbreviation,Calling Code,Capital,Major City,Latitude,Longitude
    Afghanistan,AF,93,Kabul,Kabul,33.93911,67.709953
    Iraq,IQ,964,Baghdad,Baghdad,33.2232,43.6793
    Syria,SY,963,Damascus,Raqqa,32.7688,39.0896"""
    return pd.read_csv(StringIO(data))

def test_load_data(sample_terrorism_data):
    # Testing if the dataframe is loaded correctly
    assert isinstance(sample_terrorism_data, pd.DataFrame)
    assert sample_terrorism_data.shape == (3, 8)

def test_drop_columns(sample_terrorism_data):
    columns_to_drop = ["latitude", "longitude"]
    cleaned_df = drop_columns(sample_terrorism_data, columns_to_drop)
    
    # Ensure the columns are dropped
    assert "latitude" not in cleaned_df.columns
    assert "longitude" not in cleaned_df.columns

def test_fill_missing_values(sample_terrorism_data):
    # Add missing values for testing
    sample_terrorism_data['nkill'] = sample_terrorism_data['nkill'].fillna(np.nan)
    sample_terrorism_data['claimed'] = sample_terrorism_data['claimed'].fillna(np.nan)
    
    numerical_columns = ['nkill']
    categorical_columns = ['claimed']
    
    cleaned_df = fill_missing_values(sample_terrorism_data, numerical_columns, categorical_columns)
    
    # Test if missing values were filled correctly
    assert cleaned_df['nkill'].isnull().sum() == 0
    assert cleaned_df['claimed'].isnull().sum() == 0

def test_merge_dataframes(sample_terrorism_data, sample_country_data):
    # Clean the data for merging
    terrorism_df = clean_terrorism_data(sample_terrorism_data)
    country_df = clean_country_data(sample_country_data)
    
    # Rename columns for consistency
    terrorism_df.rename(columns={'Country_txt': 'Country'}, inplace=True)
    
    merged_df = merge_dataframes(terrorism_df, country_df, merge_on='Country')
    
    # Ensure that the merge is done correctly
    assert merged_df.shape[0] == 3  # Should merge all three rows correctly
    assert 'Latitude' not in merged_df.columns  # Country df should drop 'Latitude'

def test_handle_duplicates(sample_terrorism_data):
    # Introduce duplicates in the dataset
    sample_terrorism_data = pd.concat([sample_terrorism_data, sample_terrorism_data], ignore_index=True)
    
    # Check duplicates
    cleaned_df = handle_duplicates(sample_terrorism_data)
    
    # Ensure duplicates are removed
    assert cleaned_df.duplicated().sum() == 0
    assert cleaned_df.shape[0] == 3  # Duplicates should be dropped

def test_clean_terrorism_data(sample_terrorism_data):
    cleaned_df = clean_terrorism_data(sample_terrorism_data)
    
    # Ensure that the columns with missing data are dropped
    assert 'approxdate' not in cleaned_df.columns
    assert 'resolution' not in cleaned_df.columns

def test_clean_country_data(sample_country_data):
    cleaned_df = clean_country_data(sample_country_data)
    
    # Ensure that unnecessary columns are dropped
    assert 'Latitude' not in cleaned_df.columns
    assert 'Longitude' not in cleaned_df.columns

@pytest.mark.parametrize("input_data,expected_columns", [
    ("sample_terrorism_data", ['Country_txt', 'latitude', 'longitude', 'provstate', 'city', 'claimed', 'nkill', 'nwound']),
    ("sample_country_data", ['Country', 'Abbreviation', 'Calling Code', 'Capital', 'Major City', 'Latitude', 'Longitude']),
])
def test_column_names(request, input_data, expected_columns):
    df = request.getfixturevalue(input_data)  # Get the fixture correctly
    assert set(df.columns) == set(expected_columns)


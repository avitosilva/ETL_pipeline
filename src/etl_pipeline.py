import pandas as pd
import warnings

warnings.filterwarnings("ignore")

# Set Pandas display options
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

def load_data(file_path, encoding="ISO-8859-1"):
    """
    Load CSV data into a pandas DataFrame.
    
    Parameters:
    - file_path (str): Path to the CSV file.
    - encoding (str): Encoding type for the CSV file (default is ISO-8859-1).
    
    Returns:
    - pd.DataFrame: DataFrame containing the loaded data.
    """
    return pd.read_csv(file_path, encoding=encoding, lineterminator="\n")

def drop_columns(df, columns_to_drop):
    """
    Drop specified columns from the DataFrame.
    
    Parameters:
    - df (pd.DataFrame): The DataFrame from which columns are to be dropped.
    - columns_to_drop (list): List of column names to be dropped from the DataFrame.
    
    Returns:
    - pd.DataFrame: DataFrame with the specified columns removed.
    """
    existing_columns = [col for col in columns_to_drop if col in df.columns]
    return df.drop(columns=existing_columns, errors="ignore")

def fill_missing_values(df, numerical_columns, categorical_columns):
    """
    Fill missing values in numerical and categorical columns.
    Numerical columns are filled with the median; categorical columns with the mode.
    
    Parameters:
    - df (pd.DataFrame): The DataFrame with missing values.
    - numerical_columns (list): List of numerical column names.
    - categorical_columns (list): List of categorical column names.
    
    Returns:
    - pd.DataFrame: DataFrame with missing values filled.
    """
    # Fill numerical columns with median
    for col in numerical_columns:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].median())

    # Fill categorical columns with mode
    for col in categorical_columns:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].mode()[0])

    return df

def merge_dataframes(df1, df2, merge_on='Country'):
    """
    Merge two dataframes on a specified column.
    
    Parameters:
    - df1 (pd.DataFrame): First DataFrame to merge.
    - df2 (pd.DataFrame): Second DataFrame to merge.
    - merge_on (str): Column name to merge the DataFrames on (default is 'Country').
    
    Returns:
    - pd.DataFrame: Merged DataFrame.
    """
    return pd.merge(df1, df2, on=merge_on, how='left')

def handle_duplicates(df):
    """
    Handle duplicates in the DataFrame by removing them.
    
    Parameters:
    - df (pd.DataFrame): DataFrame containing potential duplicates.
    
    Returns:
    - pd.DataFrame: DataFrame with duplicates removed.
    """
    duplicates = df.duplicated().sum()
    print(f"Number of duplicate rows: {duplicates}")
    return df.drop_duplicates()

def clean_terrorism_data(terrorism_df):
    """
    Clean and preprocess the terrorism dataset by removing unnecessary columns
    and filling missing values.
    
    Parameters:
    - terrorism_df (pd.DataFrame): The raw terrorism DataFrame.
    
    Returns:
    - pd.DataFrame: Cleaned terrorism DataFrame.
    """

    # Strip whitespace and special characters from column names
    terrorism_df.columns = terrorism_df.columns.str.strip()

    # Drop the unwanted 'related' column (now properly recognized)
    if 'related' in terrorism_df.columns:
        terrorism_df = terrorism_df.drop(columns=['related'])

    # List of columns to drop
    columns_to_drop = [
        "approxdate", "resolution", "location", "summary", "alternative", "alternative_txt",
        "gsubname", "gsubname2", "gsubname3", "weaptype2", "weaptype3", "weaptype4",
        "weapsubtype1", "weapsubtype2", "weapsubtype3", "weapsubtype4", "weapdetail",
        "claimmode", "claimmode2", "claimmode3", "propextent", "propvalue", "propcomment",
        "nhostkid", "nhostkidus", "nhours", "ndays", "ransom", "ransomamt", "ransompaid", "ransompaidus",
        "ransomnote", "hostkidoutcome", "nreleased", "scite1", "scite2", "scite3", "addnotes","INT_LOG",
        "INT_IDEO",	"INT_MISC",	"INT_ANY","country", "region", "motive", "gname2", "gname3", "compclaim",
        "crit1", "crit2", "crit3", "doubtterr", "vicinity", "kidhijcountry", "divert", "eventid", "dbsource",
        "guncertain1", "nperps", "claimed", "claimmode_txt", "nkill", "nkillus", "nkillter", "nwound",
        "nwoundus", "nwoundte", "extended", "specificity", "multiple", "individual", "nperpcap", "propextent_txt",
        "attacktype1", "attacktype2", "attacktype3", "targtype1", "targsubtype1", "natlty1",
        "weaptype1", "weapsubtype1_txt", "propextent_txt", "hostkidoutcome_txt",
        
        # Additional columns with >60% missing data
        "targtype2", "targtype2_txt", "targsubtype2", "targsubtype2_txt", "corp2", "target2", "natlty2", "natlty2_txt",
        "targtype3", "targtype3_txt", "targsubtype3", "targsubtype3_txt", "corp3", "target3", "natlty3", "natlty3_txt",
        "guncertain2", "guncertain3", "claim2", "claimmode2", "claimmode2_txt", "claim3", "claimmode3", "claimmode3_txt",
        "weaptype2", "weaptype2_txt", "weapsubtype2", "weapsubtype2_txt", "weaptype3", "weaptype3_txt", "weapsubtype3", "weapsubtype3_txt",
        "weaptype4", "weaptype4_txt", "weapsubtype4", "weapsubtype4_txt", "ransomamtus", "ransomnote"
    ]
    
    # Drop the columns with too many missing values
    terrorism_df = drop_columns(terrorism_df, columns_to_drop)
    
    # Columns to fill missing values
    numerical_columns = [
        'latitude', 'longitude', 'specificity', 'doubtterr', 'targsubtype1', 'nperpcap', 
        'nkill', 'nkillus', 'nkillter', 'nwound', 'nwoundus', 'nwoundte'
    ]
    
    categorical_columns = [
        'provstate', 'city', 'targsubtype1_txt', 'corp1', 'target1', 'natlty1', 'natlty1_txt', 
        'targsubtype1_txt', 'property', 'ishostkid', 'guncertain1', 'individual'
    ]
    
    # Fill missing values
    terrorism_df = fill_missing_values(terrorism_df, numerical_columns, categorical_columns)
    
    return terrorism_df

def clean_country_data(country_df):
    """
    Clean and preprocess the country dataset by removing redundant columns.
    
    Parameters:
    - country_df (pd.DataFrame): The raw country data DataFrame.
    
    Returns:
    - pd.DataFrame: Cleaned country data DataFrame.
    """

    # Clean the column names by stripping unwanted whitespace or special characters
    country_df.columns = country_df.columns.str.strip()

    # List of redundant columns to drop
    columns_to_drop = [
        'Abbreviation', 'Calling Code', 'Capital/Major City', 'Largest city',
        'Population: Labor force participation (%)', 'Urban_population', 'Currency-Code',
        'Gasoline Price', 'Fertility Rate', 'Latitude', 'Longitude', 'Agricultural Land (%)',
        'Maternal mortality ratio', 'Out of pocket health expenditure', 'Physicians per thousand',
        'CPI Change (%)', 'Birth Rate', 'Infant mortality', 'Official language', 'Agricultural Land (%)',
        'Gross primary education enrollment (%)', 'Gross tertiary education enrollment (%)'
    ]
    
    # Drop redundant columns (only if they exist)
    country_df = drop_columns(country_df, columns_to_drop)
    
    return country_df



def main():

    """
    Main function to run the ETL pipeline. Loads datasets, cleans them,
    merges them, handles duplicates, and fills missing values. The result
    is saved to a CSV file.
    """

    # Load datasets
    terrorism_data_path = "/home/avito/academy/avitosilva/datasets/globalterrorism.csv" # Replace to your own path
    country_data_path = "/home/avito/academy/avitosilva/datasets/world-data-2023.csv" # Replace to your own path
    
    terrorism_df = load_data(terrorism_data_path)
    country_df = load_data(country_data_path)
    
    # Clean terrorism data
    terrorism_df = clean_terrorism_data(terrorism_df)
    
    # Clean country data
    country_df = clean_country_data(country_df)
    
    # Rename columns for consistency
    terrorism_df.rename(columns={
        'country_txt': 'Country',
        'iyear': 'Year',
        'imonth': 'Month',
        'iday': 'Day',
        'region_txt': 'Region',
        'provstate': 'Province/State',
        'city': 'City',
        'latitude': 'Latitude',
        'longitude': 'Longitude',
        'success': 'Success',
        'suicide': 'Suicide',
        'attacktype1_txt': 'Primary Attack Type',
        'attacktype2_txt': 'Secondary Attack Type',
        'attacktype3_txt': 'Tertiary Attack Type',
        'targtype1_txt': 'Primary Target Type',
        'targsubtype1_txt': 'Primary Target Subtype',
        'corp1': 'Target Corporation',
        'target1': 'Target Name',
        'natlty1_txt': 'Target Nationality',
        'gname': 'Terrorist Group',
        'weaptype1_txt': 'Weapon Type',
        'property': 'Property Damage',
        'ishostkid': 'Hostage Situation'
    }, inplace=True)

    
    # Merge datasets
    merged_df = merge_dataframes(terrorism_df, country_df, merge_on='Country')
    
    # Handle duplicates
    merged_df = handle_duplicates(merged_df)
    
    # Fill missing values for merged data
    numeric_columns = merged_df.select_dtypes(include=['float64', 'int64']).columns
    categorical_columns = merged_df.select_dtypes(include=['object']).columns
    merged_df = fill_missing_values(merged_df, numeric_columns, categorical_columns)
    
    # Verify that no missing values are left
    print(merged_df.isnull().sum())
    
    # Return or save the cleaned and merged DataFrame
    merged_df.to_csv("cleaned_merged_data.csv", index=False)
    print("Merged data saved to 'cleaned_merged_data.csv'.")

if __name__ == "__main__":
    main()

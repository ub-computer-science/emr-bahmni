import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
from datetime import datetime, timedelta
import os
import re
import sqlalchemy as sa
import shutil
import hashlib
import warnings
from table_map import target_keys
import secrets
from datetime import datetime
from category import categories
import zipfile


# Suppress warnings
warnings.filterwarnings('ignore')


mart_db_name = os.environ.get('MART_DB_NAME', 'martdb') 
mart_db_username = os.environ.get('MART_DB_USERNAME','bahmni-mart' )
mart_db_password = os.environ.get('MART_DB_PASSWORD', 'password')
mart_db_host = os.environ.get('MART_DB_HOST', 'localhost')

# Database connection URI
db_uri = f"postgresql://{mart_db_username}:{mart_db_password}@{mart_db_host}/{mart_db_name}"
db_engine = create_engine(db_uri)

# Global variables
salt = secrets.token_hex(32)
default_date_format = "%d/%m/%Y"

# Date range
top_start_date_str = "10/01/2024"
top_end_date_str = None  # Example: "14/03/2024

# Search categories for tables 
# Check category.py for all current categories
search_categories = [
    'clinical_observations_and_vital_signs'
]

obs = 'clinical_observations_and_vital_signs'



def filter_by_date_range(df):
  """Filters a DataFrame by date range."""

  if 'date_created' not in df.columns:
    return df  # No date column to filter
  
  # Parse start date
  start_date_str = top_start_date_str
  start_date = datetime.strptime(start_date_str, default_date_format)

  # Parse end date
  end_date_str = top_end_date_str or datetime.now().strftime(default_date_format)
  
  # Add one day to allow pandas correctly calculate the range to be inclusive of enddate
  end_date = datetime.strptime(end_date_str, default_date_format) + timedelta(days=1)

   # Convert 'date_created' column to datetime if it's not already
  if pd.api.types.is_string_dtype(df['date_created']):
      df['date_created'] = pd.to_datetime(df['date_created'], format=default_date_format)

  # Filter the DataFrame
  filtered_df = df[(df['date_created'] >= start_date) & (df['date_created'] <= end_date)]
  return filtered_df



def enhanced_hash(value, salt=''):
    """Enhanced hashing function."""
    hashed_value = hashlib.sha256((str(value) + salt).encode('utf-8')).hexdigest()
    return hashed_value
  
def hash_value(df):
    """Hashes values in specified DataFrame columns."""
    for action, columns in target_keys.items():
        if action == "delete_keys":
            df.drop(columns=columns, inplace=True, errors='ignore')
        elif action == "delete_keys_whilecart":
            delete_columns = [col for col in df.columns if any(key.lower() in col.lower() for key in columns)]
            df.drop(columns=delete_columns, inplace=True)
        elif action == "anonymize":
            for col in columns:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: hashlib.sha256(enhanced_hash(x).encode('utf-8')).hexdigest())
        else:
            print(f"Unknown action: {action}")
    return df
  
def create_dir(dir_path):
    """Creates a directory if it doesn't exist."""
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
    os.makedirs(dir_path)
    
def export_table_to_csv(table_name, output_dir):
    """Exports a table to a CSV file."""
    try:
        with db_engine.connect() as conn:
            # Read data from SQL table into DataFrame
            df = pd.read_sql(sql=f"SELECT * FROM {table_name}", con=conn.connection)

            # Apply hashing and date filtering
            df = hash_value(df)
            df = filter_by_date_range(df)

            # Export DataFrame to CSV file
            df.to_csv(f"{output_dir}/{table_name}.csv", index=False)
            print(f"Exported {table_name} to CSV")
    except Exception as e:
        print(f"Error exporting {table_name}: {e}")

def get_obs_tables(tables, inspector):
  tables_with_column = []
  
  for table_name in tables:
    columns = inspector.get_columns(table_name)
    if any(column['name'] == 'obs_datetime' for column in columns):
      tables_with_column.append(table_name)
      
  return tables_with_column

  
def get_valid_table_from_categories(my_categories, tables, inspector):
  valid_tables = []
   
  if(obs in my_categories ):
    # Get obs table since observations themselves are table 
    obs_tables = get_obs_tables(tables, inspector)
    
    if(any(obs_tables)):
      valid_tables = valid_tables + obs_tables
 
  for category in my_categories:
    category_tables = categories.get(category)
      
    if(any(category_tables)):
      valid_tables = valid_tables + category_tables
  
  if not any(valid_tables):
    return tables
  
  return list(set(valid_tables).intersection(tables))

def zip_folder(folder_path, zip_file_path):
  """Zips a folder and its contents into a specified zip file.

  Args:
    folder_path: Path to the folder to be zipped.
    zip_file_path: Path to the output zip file.
  """

  with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
    for root, dirs, files in os.walk(folder_path):
      for file in files:
        zipf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), folder_path))



# Get all tables from the database and filter based on categories
inspector = inspect(db_engine)
all_tables = inspector.get_table_names()
filtered_tables = [table for table in all_tables if not re.search(r'batch|job|execution|task', table, flags=re.IGNORECASE)]
tables_to_export = get_valid_table_from_categories(search_categories, filtered_tables, inspector)

# Create output directory
now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
output_dir = f'exports/{now}_exported_csv_files'
create_dir(output_dir)

# Export tables to CSV files
if tables_to_export:
    for table in tables_to_export:
        export_table_to_csv(table, output_dir)
else:
    print("No tables to export")

# zip the folder
zip_file_name = f'{now}_export.zip'
zip_folder(output_dir, zip_file_name)


    
    
    
    
    
    
    
# def hash_value(df):
#   #  """Processes a DataFrame based on specified column actions.

#   # Args:
#   #   df: The pandas DataFrame to process.
#   #   target_keys: A dictionary containing lists of columns to delete or anonymize.

#   # Returns:
#   #   The processed DataFrame.
#   # """
#   for action, columns in target_keys.items():
#     if action == "delete_keys":
#       df.drop(columns=columns, inplace=True, errors='ignore')
#     elif action == "delete_keys_whilecart":
#       delete_columns = [col for col in df.columns if any(key.lower() in col.lower() for key in columns)]
#       df.drop(columns=delete_columns, inplace=True)
#     elif action == "anonymize":
#       for col in columns:
#         print(f"{col} Data")
#         print()
#         if col in df.columns:
#           df[col] = df[col].apply(lambda x: hashlib.sha256(enhanced_hash(x).encode('utf-8')).hexdigest())
#     else:
#       print(f"Unknown action: {action}")
#   return df

    

# Get a list of all tables in the database
# tables = db_engine.table_names()
# inspector = inspect(db_engine)
# tables = inspector.get_table_names()
# tables = [table for table in tables if not re.search(r'batch|job|execution|task', table, flags=re.IGNORECASE)]

# tables = get_valid_table_from_categories(search_categories, tables)

# # Specify output directory
# now = datetime.now()
# # time = now.strftime("%Y-%m-%d_%I-%M-%S-%p")
# time = 'time'

# output_dir = f'exports/{time}_exported_csv_files'
# create_dir(output_dir)

# if (len(tables) >= 1):
#    for table in tables:
#       export_table_to_csv(table, output_dir)
# else:
#    print("No data to export")
#    None
import json
import os
import pandas as pd

def read_json_file(json_file):
    with open(json_file, 'r') as file:
        try:
            json_data = json.load(file)
            return json_data
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON from file {json_file}: {e}")
            return None

def read_txt_file(txt_file):
    # Read the file and store each line as a string in a list
    with open(txt_file, 'r') as file:
        try:
            lines = file.readlines()
            return lines
        except json.JSONDecodeError as e:
            print(f"Error extracting TXT lines from file {txt_file}: {e}")
            return None
        

def apply_json_dumps(df):
    for column in df.columns:
        try:
            if df[column].dtype == 'object':
                # Attempt to convert the column to JSON
                df[column] = df[column].apply(lambda x: json.dumps(x))
        except (TypeError, ValueError):
            pass

    return df

def apply_types(df, data_types):
    # Update column types based on the dictionary
    for col, dtype in data_types.items():
        df[col] = df[col].astype(dtype)

def export_dict_to_csv(dataframe, data_types, csv_output_filename):
    # Concatenate the data into a single DataFrame
    combined_data = pd.DataFrame(dataframe)
    apply_types(combined_data, data_types)
    # Apply json.dumps to JSON-like columns
    combined_data = apply_json_dumps(combined_data)
    combined_data.to_csv(csv_output_filename, na_rep='NULL', index=False, encoding='utf-8')


def add_filename_to_json_data(json_data, filename):
    json_data["filename"] = filename
    return json_data

# Path to the directory containing JSON files
json_directory = 'data/'

# List to store the data from JSON files
matches_metadata = []
matches_data = []

# Iterate through the JSON files in the directory
for filename in os.listdir(json_directory):
    if filename.endswith('.json'):
        # Execute JSON to CSV conversion function
        filepath = os.path.join(json_directory, filename)
        json_data = read_json_file(filepath)
        matches_metadata.append(json_data)
    elif filename.endswith('.txt'):
        # Execute TXT to CSV conversion function
        filepath = os.path.join(json_directory, filename)
        txt_data = read_txt_file(filepath)
        data = [
            add_filename_to_json_data(
                json.loads(line.strip())
                , filename)
            for line in txt_data
        ]
        matches_data += data

matches_metadata_data_types = {
    'date_time': pd.StringDtype(),
}
export_dict_to_csv(matches_metadata, matches_metadata_data_types, 'matches_metadata.csv')
matches_data_data_types = {
    'timestamp': pd.StringDtype(),
    'filename': pd.StringDtype()
}
export_dict_to_csv(matches_data, matches_data_data_types, 'matches_data.csv')

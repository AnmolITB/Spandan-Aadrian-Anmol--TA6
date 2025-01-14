import os
import pandas as pd

def analyze_precip_files(directory):
    for filename in os.listdir(directory):
        if filename.startswith('precip'):
            filepath = os.path.join(directory, filename)
            data = pd.read_csv(filepath, delim_whitespace=True, header=None)
            data.columns = ['ID', 'Year', 'Month'] + [f'Day{i}' for i in range(1, 32)]
            invalid_months = data[~data['Month'].between(1, 12)]
            if not invalid_months.empty:
                print(f"Issues found in {filename}:")
                print(invalid_months.to_string(index=False))
            else:
                print(f"Analyzing {filename}:")
                print(data.describe())

analyze_precip_files('/path/to/your/directory')
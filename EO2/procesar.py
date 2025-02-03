import pandas as pd
import os
import logging

# Configure logging
logging.basicConfig(filename='data_analysis.log', level=logging.INFO,
                   format='%(asctime)s:%(levelname)s:%(message)s')

def read_and_validate_files(folder_path):
    files = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith('.dat')]
    valid_files = []
    for file in files:
        try:
            df = pd.read_csv(file, sep=None, engine='python', nrows=5)
            logging.info(f"File {file} read successfully with columns: {df.columns}")
            valid_files.append(file)
        except Exception as e:
            logging.error(f"Error reading file {file}: {e}")
    return valid_files

def process_file(file):
    try:
        df = pd.read_csv(file, sep=None, engine='python')
        return df
    except Exception as e:
        logging.error(f"Error processing file {file}: {e}")
        return None

def calculate_statistics(df):
    try:
        total_data = df.size
        total_missing_values = df.isna().sum().sum()
        total_non_missing_values = total_data - total_missing_values
        total_zeros = (df == 0).sum().sum()
        total_values = total_non_missing_values + total_missing_values
        total_days = df.shape[0]
        days_without_registry = df.isna().all(axis=1).sum()
        total_words = (df.apply(lambda col: col.map(lambda x: isinstance(x, str))).sum().sum())
        total_missing_999 = (df == -999).sum().sum()
        percentage_missing_999 = (total_missing_999 / total_data) * 100

        return {
            'total_data': total_data,
            'total_missing_values': total_missing_values,
            'total_non_missing_values': total_non_missing_values,
            'total_zeros': total_zeros,
            'total_values': total_values,
            'total_days': total_days,
            'days_without_registry': days_without_registry,
            'total_words': total_words,
            'total_missing_999': total_missing_999,
            'percentage_missing_999': percentage_missing_999
        }
    except Exception as e:
        logging.error(f"Error calculating statistics: {e}")
        return None

def process_folder(folder_path):
    valid_files = read_and_validate_files(folder_path)
    if not valid_files:
        print("No valid files found to process.")
        return

    all_stats = []
    for file in valid_files:
        df = process_file(file)
        if df is not None:
            stats = calculate_statistics(df)
            if stats:
                all_stats.append(stats)
            else:
                logging.warning(f"No statistics calculated for file {file}")
        else:
            logging.warning(f"File {file} could not be processed")

    if not all_stats:
        print("No statistics calculated.")
        return

    total_data = sum(stat['total_data'] for stat in all_stats)
    total_missing_values = sum(stat['total_missing_values'] for stat in all_stats)
    total_non_missing_values = sum(stat['total_non_missing_values'] for stat in all_stats)
    total_zeros = sum(stat['total_zeros'] for stat in all_stats)
    total_values = sum(stat['total_values'] for stat in all_stats)
    total_days = sum(stat['total_days'] for stat in all_stats)
    days_without_registry = sum(stat['days_without_registry'] for stat in all_stats)
    total_words = sum(stat['total_words'] for stat in all_stats)
    total_missing_999 = sum(stat['total_missing_999'] for stat in all_stats)
    percentage_missing_999 = (total_missing_999 / total_data) * 100

    percentage_processed_data = (total_non_missing_values / total_values) * 100
    percentage_days_without_registry = (days_without_registry / total_days) * 100

    with open('data_summary.txt', 'w') as f:
        f.write(f"Total data: {total_data}\n")
        f.write(f"Total missing values: {total_missing_values}\n")
        f.write(f"Total non-missing values: {total_non_missing_values}\n")
        f.write(f"Total zeros: {total_zeros}\n")
        f.write(f"Total values: {total_values}\n")
        f.write(f"Total words: {total_words}\n")
        f.write(f"Total missing -999 values: {total_missing_999}\n")
        f.write(f"Percentage of missing -999 values: {percentage_missing_999:.2f}%\n")
        f.write(f"Percentage of processed data: {percentage_processed_data:.2f}%\n")
        f.write(f"Percentage of days without registry: {percentage_days_without_registry:.2f}%\n")

    print(f"\nTotal data: {total_data}")
    print(f"Total missing values: {total_missing_values}")
    print(f"Total non-missing values: {total_non_missing_values}")
    print(f"Total zeros: {total_zeros}")
    print(f"Total values: {total_values}")
    print(f"Total words: {total_words}")
    print(f"Total missing -999 values: {total_missing_999}")
    print(f"Percentage of missing -999 values: {percentage_missing_999:.2f}%")
    print(f"Percentage of processed data: {percentage_processed_data:.2f}%")
    print(f"Percentage of days without registry: {percentage_days_without_registry:.2f}%")

# 12 mesos de 312 dias
folder_path = '../EO1/precip.MIROC5.RCP60.2006-2100.SDSM_REJ/'
process_folder(folder_path)
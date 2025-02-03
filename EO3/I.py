import pandas as pd
import os
import logging
import matplotlib.pyplot as plt

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

    summary = {
        'Total data': total_data,
        'Total missing values': total_missing_values,
        'Total non-missing values': total_non_missing_values,
        'Total zeros': total_zeros,
        'Total values': total_values,
        'Total words': total_words,
        'Total missing -999 values': total_missing_999,
        'Percentage of missing -999 values': percentage_missing_999,
        'Percentage of processed data': percentage_processed_data,
        'Percentage of days without registry': percentage_days_without_registry
    }
    # Display summary on screen
    for key, value in summary.items():
        print(f"{key}: {value}")

    # Export summary to CSV
    summary_df = pd.DataFrame([summary])
    summary_df.to_csv('data_summary.csv', index=False)

    # Plot and save the summary graph
    plt.figure(figsize=(10, 6))
    plt.bar(summary.keys(), summary.values())
    plt.xticks(rotation=45, ha='right')
    plt.title('Statistical Summary')
    plt.tight_layout()
    plt.savefig('summary_graph.png')  # Save the figure as a PNG file
    plt.show()

# Example usage
folder_path = '../EO1/precip.MIROC5.RCP60.2006-2100.SDSM_REJ/'
process_folder(folder_path)
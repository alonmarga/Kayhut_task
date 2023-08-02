import pandas as pd
import logging

logging.basicConfig(filename='feature_engineering_log.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def categorize_gap(gap):
    if gap > 8:
        return 'high'
    elif 4 <= gap <= 8:
        return 'medium'
    else:
        return 'low'


def perform_feature_engineering(dataframe):
    try:
        logging.info("Performing feature engineering")

        dataframe['gap'] = dataframe['high'] - dataframe['low']

        dataframe['gap_categories'] = dataframe['gap'].apply(categorize_gap)

        # Calculate the moving average and standard deviation - 3 daysss
        dataframe['moving_average_gap'] = dataframe['gap'].rolling(window=3, min_periods=1).mean()
        dataframe['std_deviation_gap'] = dataframe['gap'].rolling(window=3, min_periods=1).std()

        dataframe['date'] = pd.to_datetime(dataframe['date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')

        logging.info("Feature engineering function completed.")

        return dataframe
    except Exception as ex:
        logging.error(f"An error occurred during feature engineering: {ex}")
        return None


try:
    cleaned_data_file = 'cleaned_data.csv'
    cleaned_df = pd.read_csv(cleaned_data_file)

    feature_engineered_df = perform_feature_engineering(cleaned_df)

    if feature_engineered_df is not None:
        feature_engineered_file = 'feature_engineered_data.csv'
        feature_engineered_df.to_csv(feature_engineered_file, index=False)
        print("Feature engineering completed successfully.")
        logging.info("Feature engineering completed successfully.")
    else:
        print("Feature engineering failed.")
        logging.warning("Feature engineering failed.")
except Exception as ex:
    print(f"An error occurred: {ex}")
    logging.error(f"An error occurred: {ex}")

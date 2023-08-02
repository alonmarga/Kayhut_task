import requests
import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(filename='data_processing_log.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

logging.info("Data processing started at: %s", datetime.now())

stocks = ["AAPL", "GOOG", "MSFT", "GOOG", "AMZN", "NVDA", "BTC", "MNDY", "INTC", "UNH", "META", "JNJ", "MA"]

url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v3/get-historical-data"

headers = {
    "X-RapidAPI-Key": "cd0b5c6c49msha361c35820e253fp13a50djsnf12a87445224",
    "X-RapidAPI-Host": "apidojo-yahoo-finance-v1.p.rapidapi.com"
}

cleaned_data = []

for stock_symbol in stocks:
    params = {
        "symbol": stock_symbol
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Raise an exception for non-200 responses

        data = response.json()
        prices = data.get("prices", [])

        for price_data in prices:
            # checking all keys in the list exists in dict
            if all(key in price_data for key in ['date', 'open', 'high', 'low', 'close', 'adjclose']):
                timestamp = price_data['date']
                date = datetime.utcfromtimestamp(timestamp).strftime('%d/%m/%Y')
                row = {
                    'date': date,
                    'open': round(price_data['open'], 2),
                    'high': round(price_data['high'], 2),
                    'low': round(price_data['low'], 2),
                    'close': round(price_data['close'], 2),
                    'stock': stock_symbol,
                    'adjclose': round(price_data['adjclose'], 2)
                }
                cleaned_data.append(row)
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data for stock {stock_symbol}: {e}")
        logging.error(f"Failed to fetch data for stock {stock_symbol}: {e}")
    except Exception as ex:
        print(f"An error occurred for stock {stock_symbol}: {ex}")
        logging.error(f"An error occurred for stock {stock_symbol}: {ex}")

df = pd.DataFrame(cleaned_data)

df.dropna(inplace=True)

cleaned_data_file = 'cleaned_data.csv'
df.to_csv(cleaned_data_file, index=False)

print("Data cleaning completed successfully.")
logging.info("Data cleaning completed successfully at: : %s", datetime.now())
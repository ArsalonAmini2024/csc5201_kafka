#!/opt/homebrew/bin/python3

import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer

import requests
import numpy as np
import pandas as pd
from datetime import datetime
from datetime import date
from datetime import timedelta
import time
import yfinance as yf

def get_latest_stock_data(stock):
    data = stock.history(period="1d", interval="1m")
    latest_data = data.iloc[-1]  # Get the latest row
    return latest_data

def get_today():
    return date.today()

def get_next_day_str(today):
    return get_date_from_string(today) + timedelta(days=1)

def get_date_from_string(expiration_date):
    return datetime.strptime(expiration_date, "%Y-%m-%d").date()

def get_string_from_date(expiration_date):
    return expiration_date.strftime('%Y-%m-%d')

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--tickers', nargs='+', default=['NVDA', 'AAPL', 'META', 'NFLX', 'AMZN'])
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    tickers = args.tickers
    print('tickers', tickers)

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    # Produce data for each ticker every 5 seconds
    while True:
        for ticker in tickers:
            # Fetch latest stock data
            stock = yf.Ticker(ticker)
            latest_data = get_latest_stock_data(stock)

            # Extract open, high, low, close, and volume
            open_price = latest_data['Open']
            high_price = latest_data['High']
            low_price = latest_data['Low']
            close_price = latest_data['Close']
            volume = latest_data['Volume']

            # Send data to Kafka topic
            topic = ticker
            producer.produce(topic, key=str(get_today()), value=f"Open: {open_price}, High: {high_price}, Low: {low_price}, Close: {close_price}, Volume: {volume}", callback=delivery_callback)
            producer.flush()
            time.sleep(5)

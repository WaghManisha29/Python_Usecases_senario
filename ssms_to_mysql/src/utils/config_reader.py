# src/utils/config_reader.py
import configparser

def get_config():
    config = configparser.ConfigParser()
    config.read("config/config.ini")
    return config

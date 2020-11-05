from py2neo import Graph
import configparser

def connection():

    config = configparser.ConfigParser()

    config.read('config.ini')

    return Graph(scheme="bolt", host="localhost", port=7687, auth=(config["SETTINGS"]["USER"], config["SETTINGS"]["PASSWORD"]))



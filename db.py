import configparser
import mysql.connector

config = configparser.ConfigParser()
config.read('config.ini')

def connect():
    print('connect')
    return mysql.connector.connect(
            host= config.get('sqlBD', 'host'),
            user= config.get('sqlBD', 'user'),
            passwd= config.get('sqlBD', 'pass'),
            database= config.get('sqlBD', 'database')
            )

def createTable(tableName):
   mydb = connect()
   mycursor = mydb.cursor()
   sql = "CREATE TABLE IF NOT EXISTS " + tableName + " (city VARCHAR(255), date DATETIME)"
   mycursor.execute(sql)
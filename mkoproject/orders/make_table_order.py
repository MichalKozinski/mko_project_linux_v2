import mysql.connector

db_config = {
    'host' : 'localhost',
    'user' : 'root',
    'password' : 'knf2291',
    'database' : 'mkoproject'
}

connection = mysql.connector.connect(**db_config)
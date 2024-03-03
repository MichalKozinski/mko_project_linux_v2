import mysql.connector

db_config = {
    'host' : 'localhost',
    'user' : 'root',
    'password' : 'knf2291',
    'database' : 'mkoproject'
}

connection = mysql.connector.connect(**db_config)

cursor = connection.cursor()
query = '''
CREATE TABLE activities(
ActivityID INT AUTO_INCREMENT PRIMARY KEY,
WorkplaceNumber INT,
OrderName VARCHAR(255),
PositionName VARCHAR(255),
ElementNumber INT,
Timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
'''

cursor.execute(query)
cursor.close()
connection.close()
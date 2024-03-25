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
CREATE TABLE orders(
OrderID INT AUTO_INCREMENT PRIMARY KEY,
OrderName VARCHAR(255),
Offer VARCHAR(255),
Client VARCHAR(255),
Quantity INT,
ManHoursOffer FLOAT,
ProductionLine INT,
StartDate DATE,
EndDate DATE,
ManHoursAfterProduction FLOAT,
ProjectManager INT,
CONSTRAINT fk_ProjectManager
    FOREIGN KEY (ProjectManager) REFERENCES employees(EmpID),
Status VARCHAR(255)
)
'''

cursor.execute(query)
cursor.close()
connection.close()
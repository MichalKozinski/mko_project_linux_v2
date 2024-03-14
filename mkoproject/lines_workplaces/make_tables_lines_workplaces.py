# import mysql.connector
# import sys
# sys.path.append('/home/hadoop/mko_project_linux_v1/mkoproject')
# from API.api import get_db, connection_close
# from flask import Flask
import mysql.connector


def get_conn():
    db_config = {
        'host' : 'iu51mf0q32fkhfpl.cbetxkdyhwsb.us-east-1.rds.amazonaws.com',
        'user' : 'gagjxd2qh66bywiq',
        'password' : 't0u2mazwyo55e827',
        'database' : 'fgvomip29s41qom2'
    }

    return mysql.connector.connect(**db_config)

def make_table_lines(conn):
    cursor=conn.cursor()
    query = '''
    CREATE TABLE lines_table(
    LineID INT AUTO_INCREMENT PRIMARY KEY,
    Week INT,
    AvailableManHours FLOAT,
    RealManHours FLOAT
    )
    '''
    cursor.execute(query)
    cursor.close()

def make_table_workplaces(conn):
    cursor=conn.cursor()
    query = '''
    CREATE TABLE workplaces(
    WorkplaceID INT AUTO_INCREMENT PRIMARY KEY,
    LineID INT,
    CONSTRAINT fk_LineID
        FOREIGN KEY (LineID) REFERENCES lines_table(LineID),
    CurrentScanerUser1 INT,
    CurrentScanerUser2 INT,
    CurrentScanerUser3 INT,
    CurrentScanerUser4 INT,
    CurrentScanerUser5 INT
    )
    '''
    cursor.execute(query)
    cursor.close()

def main():
    conn = get_conn()
    make_table_lines(conn)
    make_table_workplaces(conn)
    conn.close()
        


if __name__ == '__main__':
    main()

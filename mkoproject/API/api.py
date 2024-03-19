import mysql.connector
import os
from flask import Flask, request, g, jsonify



app = Flask(__name__)


# db_config = {
#     'host' : 'localhost',
#     'user' : 'root',
#     'password' : 'knf2291',
#     'database' : 'mkoproject'
# }


def get_db():
    if 'db' not in g:
        url = os.getenv('JAWSDB_MARIA_URL')
        db_config = {
            'user': url.split(':')[1].lstrip('//'),
            'password': url.split(':')[2].split('@')[0],
            'host': url.split('@')[1].split('/')[0].split(':')[0],
            'database': url.split('/')[-1],
            'raise_on_warnings': True
        }
        g.db = mysql.connector.connect(**db_config)
        g.cursor = g.db.cursor(dictionary=True)
    return g.cursor


def login_logout(EmpID, WorkplaceNumber, ScanerNumber):
    cursor = get_db()
    query = 'SELECT ' + 'CurrentScanerUser' + str(ScanerNumber) + ' FROM workplaces WHERE WorkplaceID=%s'
    print(query)
    cursor.execute(query, (WorkplaceNumber,))
    #g.db.commit()
    user = cursor.fetchone()
    print(user)
    print(EmpID)
    if user['CurrentScanerUser' + str(ScanerNumber)]==0:
        query = 'SELECT Title FROM employees WHERE EmpID=%s'
        cursor.execute(query, (EmpID,))
        #g.db.commit()
        title = cursor.fetchone()
        print(title)
        if title and title['Title'].startswith('Production Technician'):
            query = 'UPDATE workplaces SET ' + 'CurrentScanerUser' + str(ScanerNumber) + '=%s WHERE WorkplaceID=%s'
            cursor.execute(query, (EmpID, WorkplaceNumber,))
            g.db.commit()
            return 'Pracownik zalogowany na stanowisku ' + str(WorkplaceNumber) + 'skaner numer ' + str(ScanerNumber)
        else:
            return 'Brak zezwolenia na logowanie - pracownik nieprodukcjny'
    elif user['CurrentScanerUser' + str(ScanerNumber)]==int(EmpID):
        query = 'UPDATE workplaces SET ' + 'CurrentScanerUser' + str(ScanerNumber) + '=0 WHERE WorkplaceID=%s'
        cursor.execute(query, (WorkplaceNumber,))
        g.db.commit()
        return 'Pracownik wylogowany ze stanowiska ' + str(WorkplaceNumber) + 'skaner numer' + str(ScanerNumber)
    else:
        return 'Na tym stanowisku jest już zalogowany pracownik o numerze ' + str(user['CurrentScanerUser' + str(ScanerNumber)])


def add_activity(OrderName, PositionName, ElementNumber, WorkplaceNumber, ScanerNumber):
    
    return 'tu bedzie funkcja add activity'
    

@app.teardown_appcontext
def connection_close(exception):
    db = g.pop('db', None)
    if db is not None:
        db.close()

@app.route('/scan', methods=['GET'])
def handle_scan():
    # trzeba dodać sprawdzanie zalogowanie pracownika na stanowsko ID=workplacenum:skanernum. Do logowania warunki pracownika, np. jeżeli jest pracownik to skladnia kodu jest E:EmpID, w przeciwnym razie normalne skanownie elementu zlecanie. To sie odnosi do Z={code}. Jak jest E sprawdza czy dla danego workplacenum:scanernum jest jakiś pracownik--> błąd lub czy nie jest zalogowany ten sam --> wylogowanie (procedura jest taka, że ponowne zeskanowanie na danym stanowisku pracownika to wylogowanie czyli wyzerowanie CurrentScanerUser(1-5) w tabeli workplaces) else zapisanie w tym polu numeru pracownika
    code = request.args.get('Z')
    WorkplaceScanerNumber = request.args.get('ID')
    if code and WorkplaceScanerNumber:
        try:
            OrderName_E, PositionName_EmpID, ElementNumber = code.split(':')
            WorkplaceNumber, ScanerNumber = WorkplaceScanerNumber.split(':')
            message = ''
            if OrderName_E == 'E':
                message = login_logout(PositionName_EmpID, WorkplaceNumber, ScanerNumber)
            else:
                message = add_activity(OrderName_E, PositionName_EmpID, ElementNumber,WorkplaceNumber, ScanerNumber)
            # cursor = get_db()
            # cursor.execute(''' INSERT INTO activities (WorkplaceNumber ,OrderName, PositionName, ElementNumber ) VALUES (%s, %s, %s, %s)''', (WorkplaceNumber ,OrderName_E, PositionName_EmpID, ElementNumber))
            # g.db.commit()
            return message , 200
        except ValueError:
            return 'Nieprawidłowy format danych', 400
    else:
        return 'Brak wymaganyh parametrów', 400

@app.route('/test_db')
def test_db():
    cursor = get_db()
    cursor.execute("SELECT * FROM activities LIMIT 5;")  
    rows = cursor.fetchall()
    return jsonify(rows)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
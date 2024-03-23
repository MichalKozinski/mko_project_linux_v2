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
    cursor.execute(query, (WorkplaceNumber,))
    #g.db.commit()
    user = cursor.fetchone()
    if user['CurrentScanerUser' + str(ScanerNumber)]==0:
        query = 'SELECT Title FROM employees WHERE EmpID=%s'
        cursor.execute(query, (EmpID,))
        #g.db.commit()
        title = cursor.fetchone()
        if title and title['Title'].startswith('Production Technician'):
            query = 'UPDATE workplaces SET ' + 'CurrentScanerUser' + str(ScanerNumber) + '=%s WHERE WorkplaceID=%s'
            cursor.execute(query, (EmpID, WorkplaceNumber,))
            g.db.commit()
            return 'Pracownik zalogowany na stanowisku ' + str(WorkplaceNumber) + ' skaner numer ' + str(ScanerNumber)
        else:
            return 'Brak zezwolenia na logowanie - pracownik nieprodukcjny'
    elif user['CurrentScanerUser' + str(ScanerNumber)]==int(EmpID):
        query = 'UPDATE workplaces SET ' + 'CurrentScanerUser' + str(ScanerNumber) + '=0 WHERE WorkplaceID=%s'
        cursor.execute(query, (WorkplaceNumber,))
        g.db.commit()
        return 'Pracownik wylogowany ze stanowiska ' + str(WorkplaceNumber) + ' skaner numer ' + str(ScanerNumber)
    else:
        return 'Na tym stanowisku jest już zalogowany pracownik o numerze ' + str(user['CurrentScanerUser' + str(ScanerNumber)])


def can_add_activity(existing_activities, new_activity):
    match = 0
    print(existing_activities)
    print(new_activity)
    for activity in existing_activities:
        if (activity['OrderName']==new_activity['OrderName'] and activity['PositionName']==new_activity['PositionName'] and activity['ElementNumber']==new_activity['ElementNumber'] and activity['WorkplaceNumber']==new_activity['WorkplaceNumber']):
            match += 1
    print(match)
    return match<2


def add_activity(OrderName, PositionName, ElementNumber, WorkplaceNumber, ScanerNumber):
    #funkcja sprawdza najpierw czy ktoś jest zalogowany na stanowisku WorkplaceNumber:ScanerNumber i jeżeli tak to pobiera numer tego pracownika. Jeżeli nie podaje komunikat, że najpierw musisz się zalogować. Następnie dodaje dane aktywności jeżeli pracownik jest zalogowany. Dane analizowane są z tabeli activities w taki sposób, że pierwsze aktywność to start a drugie to zakończenie danej czynności. W przypadku wykonywania programu CNC zakładamy 3 wpisy, wczytanie programu, rozpoczęcie obróbki, zakończenie obróbki 
    cursor = get_db()
    query = 'SELECT ' + 'CurrentScanerUser' + str(ScanerNumber) + ' FROM workplaces WHERE WorkplaceID=%s'
    cursor.execute(query, (WorkplaceNumber,))
    user = cursor.fetchone()
    if user['CurrentScanerUser' + str(ScanerNumber)]==0:
        return 'Aby dodać aktywność musisz zalogować się do skanera. Zesknuj swój kod pracownika abyt to zrobić' 
    else:
        new_activity = {
            'WorkplaceNumber' : WorkplaceNumber,
            'OrderName' : OrderName,
            'PositionName' : PositionName,
            'ElementNumber' :  ElementNumber
        }
        cursor.execute("SELECT * FROM activities")  
        existing_activities = cursor.fetchall()
        print(can_add_activity(existing_activities, new_activity))
            # query = 'INSERT INTO activities (WorkplaceNumber ,OrderName, PositionName, ElementNumber, EmpID ) VALUES (%s, %s, %s, %s, %s)'
            # cursor.execute(query, (WorkplaceNumber ,OrderName, PositionName, ElementNumber, user['CurrentScanerUser' + str(ScanerNumber)], ))
            # g.db.commit()
            # return 'Dodałeś aktywność do bazy'


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
                #connection_close()
            else:
                message = add_activity(OrderName_E, PositionName_EmpID, ElementNumber,WorkplaceNumber, ScanerNumber)
                #connection_close()
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
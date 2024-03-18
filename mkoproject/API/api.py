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
    if code and WorkplaceNumber:
        try:
            OrderName, PositionName, ElementNumber = code.split(':')
            WorkplaceNumber, ScanerNumber = WorkplaceScanerNumber.split(':')
            cursor = get_db()
            cursor.execute(''' INSERT INTO activities (WorkplaceNumber ,OrderName, PositionName, ElementNumber ) VALUES (%s, %s, %s, %s)''', (WorkplaceNumber ,OrderName, PositionName, ElementNumber))
            g.db.commit()
            return 'Dane poprawnie dodane do bazy', 200
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
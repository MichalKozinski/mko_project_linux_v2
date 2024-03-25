import csv
import mysql.connector
from make_tables_lines_workplaces import get_conn

CSV_WP_FILENAME = 'workplaces.csv'
CSV_L_FILENAME = 'lines.csv'


def load_csv_file(csv_filename):
    with open(csv_filename, 'r', newline='') as content:
        reader = csv.reader(content)
        data = list(reader)
    return data


def fill_lines_tab(conn,data):
    cursor=conn.cursor()
    for row in data:
        cursor.execute(''' INSERT INTO lines_table (LineID, AvailableManHoursWeek1, AvailableManHoursWeek2, AvailableManHoursWeek3, AvailableManHoursWeek4, AvailableManHoursWeek5, AvailableManHoursWeek6, AvailableManHoursWeek7, AvailableManHoursWeek8, AvailableManHoursWeek9, AvailableManHoursWeek10, AvailableManHoursWeek11, AvailableManHoursWeek12, AvailableManHoursWeek13, AvailableManHoursWeek14, AvailableManHoursWeek15, AvailableManHoursWeek16, AvailableManHoursWeek17, AvailableManHoursWeek18, AvailableManHoursWeek19, AvailableManHoursWeek20, AvailableManHoursWeek21, AvailableManHoursWeek22, AvailableManHoursWeek23, AvailableManHoursWeek24, AvailableManHoursWeek25, AvailableManHoursWeek26, AvailableManHoursWeek27, AvailableManHoursWeek28, AvailableManHoursWeek29, AvailableManHoursWeek30, AvailableManHoursWeek31, AvailableManHoursWeek32, AvailableManHoursWeek33, AvailableManHoursWeek34, AvailableManHoursWeek35, AvailableManHoursWeek36, AvailableManHoursWeek37, AvailableManHoursWeek38, AvailableManHoursWeek39, AvailableManHoursWeek40, AvailableManHoursWeek41, AvailableManHoursWeek42, AvailableManHoursWeek43, AvailableManHoursWeek44, AvailableManHoursWeek45, AvailableManHoursWeek46, AvailableManHoursWeek47, AvailableManHoursWeek48, AvailableManHoursWeek49, AvailableManHoursWeek50, AvailableManHoursWeek51, AvailableManHoursWeek52)  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[20], row[21], row[22], row[23], row[24], row[25], row[26], row[27], row[28], row[29], row[30], row[31], row[32], row[33], row[34], row[35], row[36], row[37], row[38], row[39], row[40], row[41], row[42], row[43], row[44], row[45], row[46], row[47], row[48], row[49], row[50], row[51], row[52]))
        conn.commit()
    # cursor.execute(''' INSERT INTO lines_table  (LineID) VALUES (1)''')
    # cursor.execute(''' INSERT INTO lines_table  (LineID) VALUES (2)''')
    # cursor.execute(''' INSERT INTO lines_table  (LineID) VALUES (3)''')
    # cursor.execute(''' INSERT INTO lines_table  (LineID) VALUES (4)''')
    # cursor.execute(''' INSERT INTO lines_table  (LineID) VALUES (5)''')
    cursor.close()


def fill_workplaces_tab(conn,data):
    cursor=conn.cursor()
    try:
        for row in data:
            print(row[0])
            cursor.execute(''' INSERT INTO workplaces (WorkplaceID ,LineID, CurrentScanerUser1, CurrentScanerUser2, CurrentScanerUser3, CurrentScanerUser4, CurrentScanerUser5 ) VALUES (%s, %s, %s, %s, %s, %s, %s)''', (int(row[0]), int(row[1]), int(row[2]), int(row[3]), int(row[4]), int(row[5]), int(row[6])))
            conn.commit()
    except Exception as e:
            print(e)
    finally:
        cursor.close()


def main():
    # data_lines=load_csv_file(CSV_L_FILENAME)
    data_workplaces=load_csv_file(CSV_WP_FILENAME)
    conn=get_conn()
    if conn.is_connected:
        print('ok')
    else:
        print('blad')
    # fill_lines_tab(conn,data_lines)
    fill_workplaces_tab(conn,data_workplaces)
    conn.close()


if __name__ == '__main__':
    main()
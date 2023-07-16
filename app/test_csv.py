import csv
from pyhive import hive
from pyhive.exc import OperationalError

HOSTNAME: str = 'localhost'
PORT: int = 10000

connection: hive.Connection = hive.connect(HOSTNAME)
print('Connected:', connection)

cursor: hive.Cursor = connection.cursor()
print('Cursor:', cursor)

sql: str = '''
    CREATE TABLE fiscales (
        id INT,
        name STRING
    )
'''
print('SQL:', sql)
try:
    cursor.execute(sql)
except OperationalError as error:
    print('Warning:', error)
    if 'already exists' not in str(error):
        raise
else:
    print('Created!')

sql: str = '''
    INSERT INTO fiscales
    VALUES (1, 'John'), (2, 'Jane'), (3, 'Bob')
'''
print('SQL:', sql)
try:
    cursor.execute(sql)
except OperationalError as error:
    print('Warning:', error)
    raise
else:
    print('Inserted!')

connection.commit()
print('Committed!')

sql: str = 'SELECT * FROM fiscales'
print('SQL:', sql)
cursor.execute(sql)
rows: list = list(cursor.fetchall())

csv_file = "result.csv"
with open(csv_file, 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(rows)

connection.close()
print('Connection closed!')

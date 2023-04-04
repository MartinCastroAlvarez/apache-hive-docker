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
results: list = cursor.fetchall()
for row in results:
    print('Row:', row)

connection.close()
print('Connection closed!')

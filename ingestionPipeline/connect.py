import psycopg2
#from pytest import param
from config import config
import pyodbc

def connect():
    conn = None
    # try:
    params = config()
    if params['default'] == "postgresql":
        database = params['default']
        del params['default']
        conn = psycopg2.connect(**params)
        # print(conn)

    elif params['default'] == "mssql":
        database = params['default']
        del params['default']
        conn = pyodbc.connect(**params)
        # print(conn)
    
    # conn = pyodbc.connect(driver='{SQL Server}', host='IBS-LAP-397', database='covid19', trusted_connection='tcon', user='testmssql', password='welcome@123')

    
    #cur = conn.cursor()
    # execute a statement
    #print('PostgreSQL database version:')
    #cur.execute('SELECT * from abcd')

    # display the PostgreSQL database server version
    #db_version = cur.fetchone()
    #print(db_version)
    # close the communication with the PostgreSQL
    #cur.close()
    return conn, database

    # except (Exception, psycopg2.DatabaseError) as error:
        # print(error)
    '''
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
    '''

'''
if __name__ == '__main__':
    connect()
'''

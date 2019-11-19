
import sqlite3 as sq

def load_db(dict_in,dbname):
    table_list = [table for table in dict_in.keys()]
#export to sqlite
    sql_data = f'data/{dbname}.db'
    
    conn = sq.connect(sql_data)
    cur = conn.cursor()
    for table in table_list:
        dropstring = '''drop table if exists "{0}"'''.format(table)
        print(dropstring)
        cur.execute(dropstring)
        print(f'create and load table - {table}')
        dict_in[table].to_sql(table,conn, if_exists='replace', index=True)
    conn.commit()
    conn.close() 
    print('SQLite Load Complete')




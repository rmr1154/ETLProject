#pip install cleanco
#pip install fastparquet
from cleanco import cleanco
import pandas as pd
import sqlite3 as sq

def clean_co_names(df, col):
    df['cleaned'] = df[col]
    #print(f'Sample: {df.cleaned.head(1)}')
    df['cleaned'] = df['cleaned'].str.upper() # uppercase
    print(f'Set Upper')#: {df.cleaned.head(1)}')
    df['cleaned'] = df['cleaned'].str.replace(',', '') # Remove commas
    print(f'Remove commas')#: {df.cleaned.head(1)}')
    df['cleaned'] = df['cleaned'].str.replace(' - ', ' ') # Remove hyphens
    print(f'Remove hyphens')#: {df.cleaned.head(1)}')
    df['cleaned'] = df['cleaned'].str.replace(r"\(.*\)","") # Remove text between parenthesis 
    print(f'Remove text between parens')#: {df.cleaned.head(1)}')
    df['cleaned'] = df['cleaned'].str.replace(' AND ', ' & ') #replace AND with &
    print(f'replace AND with &')#: {df.cleaned.head(1)}')
    df['cleaned'] = df['cleaned'].str.strip() # Remove spaces in the begining/end
    print(f'Remove leading/trailing spaces')#: {df.cleaned.head(1)}')
    df['cleaned'] = df['cleaned'] .apply(lambda x: cleanco(x).clean_name() if type(x)==str else x) # Remove business entities extensions (1)
    print(f'Cleanco Pass1')#: {df.cleaned.head(1)}')
    df['cleaned'] = df['cleaned'].str.replace('.','') # Remove dots
    print(f'Remove dots')#: {df.cleaned.head(1)}')
    df['cleaned'] = df['cleaned'] .str.encode('utf-8') # Encode
    print(f'Encode utf-8')#: {df.cleaned.head(1)}')
    df['cleaned'] = df['cleaned'] .apply(lambda x: cleanco(x).clean_name() if type(x)==str else x) # Remove business entities extensions (2) - after removing the dots
    print(f'Cleanco Pass2')#: {df.cleaned.head(1)}')
    #print(f'Final Transform: {df[col].head(1)} - {df.cleaned.head(1)}')
    return df



def convert_epoch_time(df,col_list):
    for col in col_list:
        #df[col].fillna(value="1900-01-01",inplace=True)
        df[col] = pd.to_datetime(df[col],unit='s')
    return df

def split_address(df,col):
    #do stuffs
    return df    

 
def web_screenshot(df,col):
    #grab screenshot w/ splinter
    return df

#Let's export everything to parquet
def export_parquet(table):
    for table in table_list:
        table.reset_index(drop=True).to_parquet('data/'+table+'.parquet', engine='fastparquet', compression='gzip') 
        

def export_sqlite(table,dbname):
    sql_data = f'data/{dbname}.db'
    #etl.sq.register_adapter(etl.np.int64, lambda val: float(val))
    conn = sq.connect(sql_data)
    cur = conn.cursor()
    for table in table_list:
        try:
            dropstring = '''drop table if exists "{0}"'''.format(table)
            print(dropstring)
            cur.execute(dropstring)
            table.to_sql(table,conn, if_exists='replace', index=False)
        except:
            pass
    conn.commit()
    conn.close() 
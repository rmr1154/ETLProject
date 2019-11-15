#pip install cleanco
from cleanco import cleanco
import pandas as pd

def clean_co_names(df_in, col):
    df = df_in.copy()
    df['cleaned'] = df[col].str.upper() # uppercase
    df['cleaned'] = df[col].str.replace(',', '') # Remove commas
    df['cleaned'] = df[col].str.replace(' - ', ' ') # Remove hyphens
    df['cleaned'] = df[col].str.replace(r"\(.*\)","") # Remove text between parenthesis 
    df['cleaned'] = df[col].str.replace(' AND ', ' & ') #replace AND with &
    df['cleaned'] = df[col].str.strip() # Remove spaces in the begining/end
    df['cleaned'] = df[col].str.replace('.','') # Remove dots
    df['cleaned'] = df[col].str.encode('utf-8') # Encode
    df['cleaned'] = df[col].apply(lambda x: cleanco(x).clean_name() if type(x)==str else x) # Remove business entities extensions (1)
    df['cleaned'] = df[col].apply(lambda x: cleanco(x).clean_name() if type(x)==str else x) # Remove business entities extensions (2) - after removing the dots
    return df



def convert_epoch_time(df,col_list):
    for col in col_list:
        #df[col].fillna(value="1900-01-01",inplace=True)
        df[col] = pd.to_datetime(df[col],unit='s')
    return df

 
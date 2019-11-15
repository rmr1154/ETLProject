#pip install cleanco
from cleanco import cleanco
import pandas as pd

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

 
import pandas as pd
import zipfile

def extract_zip(archive,file):
    #unzip and import contents into dataframe
    with zipfile.ZipFile(archive) as z:
        with z.open(file) as f:
            df = pd.read_csv(f)
    return df



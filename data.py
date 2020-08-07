# %%
import pandas as pd
from sqlalchemy import create_engine
import requests
from zipfile import ZipFile
import os


# %%
def Download(y):
    if y < 2016 or y > 2020:
        return 0
    print("開始下載" + str(y) + "年報告...")
    url = "https://www.cftc.gov/files/dea/history/dea_cit_xls_" + str(y) + ".zip"
    resp = requests.get(url)
    file_path = os.path.join(os.path.basename(url))
    with open(file_path, 'wb') as file:
        file.write(resp.content)

    with ZipFile(file_path) as zip_ref:
        zip_ref.extractall()
    return file_path


# %%
if __name__ == '__main__':
    for i in range(2016, 2021):
        path = Download(i)
        if (not(path)):
            print('Error')
            continue
        df = pd.read_excel('deacit.xls')
        df = df.drop(columns=['As_of_Date_In_Form_YYMMDD'])
        header = df.columns.tolist()
        header = header[1:2] + header[0:1] + header[2:]
        dict = {}
        for h in header:
            if h == 'Report_Date_as_MM_DD_YYYY':
                dict[h] = 'date'
            elif h[-1] == ' ':
                dict[h] = h[:-1].lower()
            elif h == 'Change_NonComm_Spead_All_NoCIT':
                dict[h] = 'Change_NonComm_Spread_All_NoCIT'.lower()
            else:
                dict[h] = h.lower()
        df = df[header]
        df = df.rename(columns=dict)
        df['date'] = df['date'].apply(lambda x: x.strftime("%Y-%m-%d"))

        engine = create_engine(
            'postgres://postgres:postgres@192.168.99.100:5432/postgres')
        df.to_sql('cftc_cit_supplement', engine, index=False, if_exists='append')
        print(str(i)+'年 Done')
        os.remove(path)

import pandas as pd
import numpy as np

def get_convert_float(text):
    try:
        return float(text)
    except Exception as e:
        print(e)

def get_remove_first_zero(text):
    try:
        return str(text).lstrip('0')
    except Exception as e:
        print(e)

alcs = pd.read_excel("H:/Clients/TeamLease/ALCS Letters/25012022/ALCS/HDFC_NEFT_ALCS_25012022.xlsx", dtype=str)
neft = pd.read_excel("H:/Clients/TeamLease/ALCS Letters/25012022/UTR_DT_24_01_2022.xlsx", dtype=str)

alcs['Issued Amt'] = alcs['Issued Amt'].apply(get_convert_float)
neft['Amt'] = neft['Amt'].apply(get_convert_float)

alcs['Acc #'] = alcs['Acc #'].apply(get_remove_first_zero)
neft['Bene Acct No'] = neft['Bene Acct No'].apply(get_remove_first_zero)

print(alcs[['Acc #', 'Issued Amt', 'Date']])

print(neft[['Bene Acct No', 'Amt', 'Value Dt']])

new_df = pd.merge(alcs, neft, how='left', left_on=['Acc #', 'Issued Amt'], right_on=['Bene Acct No', 'Amt'])

print(new_df['UTR Number'])

new_df.to_excel('H:/Clients/TeamLease/ALCS Letters/25012022/Updated_UTR_HDFC_etl.xlsx', index = False)
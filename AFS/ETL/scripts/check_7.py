import pandas as pd
import numpy as np

neft_columns = ['Transaction Type', 'Name Of Beneficiary', 'CMS Bank Reference Number',
       'RTGS UTR  Number', 'Beneficiary  Bank IFSC Code', 'Net Amount',
       'Payment Date', 'Debit Account No.', 'Beneficiary Bank A/c No',
       'Customer Reference No', 'Account Type', 'Client ID', 'Invoice Number',
       'Status Of Transaction', 'Liquidation Date']

data_column_converter = {}
for name in neft_columns:
    data_column_converter[name] = str

data_neft = pd.read_excel("H:/Clients/TeamLease/ALCS Letters/28022022/ICICI Reversal/ICICI_UTR_DT_16_02_2022.xlsx", skiprows=5, converters=data_column_converter)


nuture_columns = ['Debit A/c no', 'Beneficiary A/c No', 'Beneficiary Name', 'Amount',
       'Payment Mode', 'Date', 'IFSC Code', 'Payable Location Name',
       'Print Location name', 'Bene Mobile No', 'Bene Email Id', 'Bene Add 1',
       'Bene Add 2', 'Bene Add 3', 'Bene Add 4', 'Add details 1',
       'Add details 2', 'Add details 3', 'Add details 4', 'Add details 5',
       'Remarks', 'Payment Ref No', 'Status', 'Liquidation Date',
       'Customer Ref No', 'Instrument Ref No', 'Instrument_No', 'UTR NO']

data_column_converter = {}
for name in nuture_columns:
    data_column_converter[name] = str

data_nuture = pd.read_excel("H:/Clients/TeamLease/ALCS Letters/28022022/ICICI Reversal/ICICI_UTR_DT_16_02_2022-Nurture.xlsx", skiprows=5, converters=data_column_converter)


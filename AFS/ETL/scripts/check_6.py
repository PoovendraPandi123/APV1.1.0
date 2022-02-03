# import pandas as pd
import re

# def get_ifsc_type(*args):
#     try:
#         return args[1][:4] + "-" + args[0]
#     except Exception as e:
#         print(e)
#
# data = pd.read_excel('H:/Clients/TeamLease/ALCS Letters/Outputs/27012022/alcs_hdfc_neft_output_etl.xlsx')
#
# print(data[['payment_type', 'IFSC CODES']])
#
# data['ifsc_type'] = data[['payment_type', 'IFSC CODES']].apply(lambda x : get_ifsc_type(*x), axis = 1)
#
# print(data['ifsc_type'])

hdfc_ifsc_code_grouped_list = [{'ifsc_type': 'HDFC-REIMB', 're_letter_generated_number_one': '9', 'bank_utr_column': '', 'issued_amount': 11320, 'letter_number_ifsc': ''}, {'ifsc_type': 'OTHER-OPP', 're_letter_generated_number_one': '10', 'bank_utr_column': '0000201251662157', 'issued_amount': 381639, 'letter_number_ifsc': ''}, {'ifsc_type': 'OTHER-OPP', 're_letter_generated_number_one': '5', 'bank_utr_column': '0000201250727629', 'issued_amount': 191292, 'letter_number_ifsc': ''}, {'ifsc_type': 'OTHER-OPP', 're_letter_generated_number_one': '6', 'bank_utr_column': '0000201251221789', 'issued_amount': 303903, 'letter_number_ifsc': ''}, {'ifsc_type': 'OTHER-OPP', 're_letter_generated_number_one': '7', 'bank_utr_column': '0000201251664144', 'issued_amount': 158565, 'letter_number_ifsc': ''}, {'ifsc_type': 'OTHER-REIMB', 're_letter_generated_number_one': '3', 'bank_utr_column': '0000201250624031', 'issued_amount': 1307258, 'letter_number_ifsc': ''}, {'ifsc_type': 'OTHER-REIMB', 're_letter_generated_number_one': '4', 'bank_utr_column': '0000201251633986', 'issued_amount': 83787, 'letter_number_ifsc': ''}, {'ifsc_type': 'OTHER-REIMB', 're_letter_generated_number_one': '9', 'bank_utr_column': '', 'issued_amount': 1923524, 'letter_number_ifsc': ''}, {'ifsc_type': 'OTHER-SALARY', 're_letter_generated_number_one': '1', 'bank_utr_column': '0000201250441569', 'issued_amount': 667903, 'letter_number_ifsc': ''}, {'ifsc_type': 'OTHER-SALARY', 're_letter_generated_number_one': '2', 'bank_utr_column': '0000201250945903', 'issued_amount': 2546820, 'letter_number_ifsc': ''}, {'ifsc_type': 'OTHER-SALARY', 're_letter_generated_number_one': '8', 'bank_utr_column': '0000201251633349', 'issued_amount': 689589, 'letter_number_ifsc': ''}]

letter_numbers = []

for details in hdfc_ifsc_code_grouped_list:
    if not re.search(r'hdfc', details['ifsc_type'].lower()):
        letter_numbers.append(int(details['re_letter_generated_number_one']))

letter_numbers.sort()
# print(letter_numbers)
# Ordering the Grouped List
hdfc_ifsc_code_grouped_list_ordered = list()

for i in range(0, len(letter_numbers)):
    for details in hdfc_ifsc_code_grouped_list:
        if not re.search(r'hdfc', details['ifsc_type'].lower()):
            if details["re_letter_generated_number_one"] == str(letter_numbers[i]):
                hdfc_ifsc_code_grouped_list_ordered.append(details)

# for order in hdfc_ifsc_code_grouped_list_ordered:
#     print(order)

letter_number = 1
for order in hdfc_ifsc_code_grouped_list_ordered:
    for detail in hdfc_ifsc_code_grouped_list:
        if order["ifsc_type"] == detail["ifsc_type"] and order["re_letter_generated_number_one"] == detail["re_letter_generated_number_one"] \
                and order["bank_utr_column"] == detail["bank_utr_column"] and order["issued_amount"] == detail["issued_amount"]:
            detail["letter_number_ifsc"] = str(letter_number)
            letter_number = letter_number + 1

# Salary
for group in hdfc_ifsc_code_grouped_list:
    if re.search(r'hdfc', group["ifsc_type"].lower()):
        if re.search(r'sal', group["ifsc_type"].lower()):
            group['letter_number_ifsc'] = str(letter_number)
            letter_number = letter_number + 1

# Reimbursement
for group in hdfc_ifsc_code_grouped_list:
    if re.search(r'hdfc', group["ifsc_type"].lower()):
        if re.search(r'rei', group["ifsc_type"].lower()):
            group['letter_number_ifsc'] = str(letter_number)
            letter_number = letter_number + 1

# OPP
for group in hdfc_ifsc_code_grouped_list:
    if re.search(r'hdfc', group["ifsc_type"].lower()):
        if re.search(r'opp', group["ifsc_type"].lower()):
            group['letter_number_ifsc'] = str(letter_number)
            letter_number = letter_number + 1

# Creating Reference Column

for group in hdfc_ifsc_code_grouped_list:
    group["reference"] = group["ifsc_type"] + "_" + group["re_letter_generated_number_one"]

# for group in hdfc_ifsc_code_grouped_list:
#     print(group)

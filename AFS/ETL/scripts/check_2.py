date_list = [{'payment_date': '2021-11-24 10:30', 'payment_type': 'SALARY', 'issued_amount': 32546.0, 'utr_number': '', 'bank_debit_date': '', 're_letter_upload_number': ''}, {'payment_date': '2021-11-24 12:14', 'payment_type': 'REIMB', 'issued_amount': 20509.0, 'utr_number': '', 'bank_debit_date': '', 're_letter_upload_number': ''}, {'payment_date': '2021-11-24 13:00', 'payment_type': 'OPP', 'issued_amount': 50071.0, 'utr_number': '', 'bank_debit_date': '', 're_letter_upload_number': ''}, {'payment_date': '2021-11-24 15:00', 'payment_type': 'SALARY', 'issued_amount': 17940.0, 'utr_number': '', 'bank_debit_date': '', 're_letter_upload_number': ''}, {'payment_date': '2021-11-24 16:15', 'payment_type': 'REIMB', 'issued_amount': 169075.0, 'utr_number': '', 'bank_debit_date': '', 're_letter_upload_number': ''}, {'payment_date': '2021-11-24 16:30', 'payment_type': 'OPP', 'issued_amount': 17020.0, 'utr_number': '', 'bank_debit_date': '', 're_letter_upload_number': ''}, {'payment_date': '2021-11-24 19:30', 'payment_type': 'SALARY', 'issued_amount': 979588.0, 'utr_number': '', 'bank_debit_date': '', 're_letter_upload_number': ''}]

salary_list = []
opp_list = []
reimbursement_list = []

for date in date_list:
    if date["payment_type"] == "SALARY":
        salary_list.append(date['payment_date'])
    elif date["payment_type"] == "OPP":
        opp_list.append(date['payment_date'])
    elif date["payment_type"] == "REIMB":
        reimbursement_list.append(date['payment_date'])

letter_number = 1

# SALARY
for salary_date in salary_list:
    if int(salary_date.split(" ")[-1].split(":")[0]) in [10,11,12] and salary_date.split(" ")[-1] != '12:30':
        for date in date_list:
            if date["payment_date"] == salary_date and len(date["re_letter_upload_number"]) == 0:
                date["re_letter_upload_number"] = str(letter_number)
                letter_number += 1
    elif int(salary_date.split(" ")[-1].split(":")[0]) in [15,16,17]:
        for date in date_list:
            if date["payment_date"] == salary_date and len(date["re_letter_upload_number"]) == 0:
                date["re_letter_upload_number"] = str(letter_number)
                letter_number += 1
    elif int(salary_date.split(" ")[-1].split(":")[0]) in [19,20,21,22,23,24]:
        for date in date_list:
            if date["payment_date"] == salary_date and len(date["re_letter_upload_number"]) == 0:
                date["re_letter_upload_number"] = str(letter_number)
                letter_number += 1

# REIMBURSEMENT
for reimb_date in reimbursement_list:
    if int(reimb_date.split(" ")[-1].split(":")[0]) in [12,13,14,15]:
        for date in date_list:
            if date["payment_date"] == reimb_date and len(date["re_letter_upload_number"]) == 0:
                date["re_letter_upload_number"] = str(letter_number)
                letter_number += 1
    elif int(reimb_date.split(" ")[-1].split(":")[0]) in [16,17,18,19,20,21,22,23,24]:
       for date in date_list:
           if date["payment_date"] == reimb_date and len(date["re_letter_upload_number"]) == 0:
               date["re_letter_upload_number"] = str(letter_number)
               letter_number += 1

# OPP
for opp_date in opp_list:
    if int(opp_date.split(" ")[-1].split(":")[0]) in [12,13,14,15]:
        for date in date_list:
            if date["payment_date"] == opp_date and len(date["re_letter_upload_number"]) == 0:
                date["re_letter_upload_number"] = str(letter_number)
                letter_number += 1
    elif int(opp_date.split(" ")[-1].split(":")[0]) in [16,17,18,19,20,21,22,23,24]:
        for date in date_list:
            if date["payment_date"] == opp_date and len(date["re_letter_upload_number"]) == 0:
                date["re_letter_upload_number"] = str(letter_number)
                letter_number += 1

for date in date_list:
    print(date)

date_list = [{'payment_date': '2022-01-17 10:33SALARY', 'payment_type': 'SALARY', 'issued_amount': 5510.0, 'utr_number': '0000201171088102', 'bank_debit_date': '2022-01-17 00:00:00', 're_letter_upload_number': '', 'bank_reference_text': 'TE621701.101 17012022'}, {'payment_date': '2022-01-17 12:14REIMB', 'payment_type': 'REIMB', 'issued_amount': 252045.0, 'utr_number': '0000201171215883', 'bank_debit_date': '2022-01-17 00:00:00', 're_letter_upload_number': '', 'bank_reference_text': 'TE621701.116 17012022'}, {'payment_date': '2022-01-17 12:19OPP', 'payment_type': 'OPP', 'issued_amount': 391735.0, 'utr_number': '0000201171220473', 'bank_debit_date': '2022-01-17 00:00:00', 're_letter_upload_number': '', 'bank_reference_text': 'TE621701.118 17012022'}, {'payment_date': '2022-01-17 13:01OPP', 'payment_type': 'OPP', 'issued_amount': 393213.0, 'utr_number': '0000201171402251', 'bank_debit_date': '2022-01-17 00:00:00', 're_letter_upload_number': '', 'bank_reference_text': 'TE621701.130 17012022'}, {'payment_date': '2022-01-17 13:50SALARY', 'payment_type': 'SALARY', 'issued_amount': 34863.0, 'utr_number': '0000201171440124', 'bank_debit_date': '2022-01-17 00:00:00', 're_letter_upload_number': '', 'bank_reference_text': 'TE621701.136 17012022'}, {'payment_date': '2022-01-17 15:06SALARY', 'payment_type': 'SALARY', 'issued_amount': 19998.0, 'utr_number': '0000201171630218', 'bank_debit_date': '2022-01-17 00:00:00', 're_letter_upload_number': '', 'bank_reference_text': 'TE621701.145 17012022'}, {'payment_date': '2022-01-17 16:15REIMB', 'payment_type': 'REIMB', 'issued_amount': 69247.0, 'utr_number': '0000201171769696', 'bank_debit_date': '2022-01-17 00:00:00', 're_letter_upload_number': '', 'bank_reference_text': 'TE621701.151 17012022'}, {'payment_date': '2022-01-17 16:19OPP', 'payment_type': 'OPP', 'issued_amount': 1113764.0, 'utr_number': '0000201171780383', 'bank_debit_date': '2022-01-17 00:00:00', 're_letter_upload_number': '', 'bank_reference_text': 'TE621701.153 17012022'}, {'payment_date': '2022-01-17 16:30OPP', 'payment_type': 'OPP', 'issued_amount': 47226.0, 'utr_number': '0000201171807712', 'bank_debit_date': '2022-01-17 00:00:00', 're_letter_upload_number': '', 'bank_reference_text': 'TE621701.156 17012022'}, {'payment_date': '2022-01-17 21:00SALARY', 'payment_type': 'SALARY', 'issued_amount': 61580.0, 'utr_number': '0000201172277095', 'bank_debit_date': '2022-01-18 00:00:00', 're_letter_upload_number': '', 'bank_reference_text': 'TE621701.192 17012022'}]
for date in date_list:
    print(date)

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

# SALARY - 12.30
for salary_date in salary_list:
    if salary_date.split(" ")[-1] == '12:30':
        for date in date_list:
            if date["payment_date"] == salary_date and len(date["re_letter_upload_number"]) == 0:
                date["re_letter_upload_number"] = str(letter_number)
                letter_number += 1

for date in date_list:
    print(date)
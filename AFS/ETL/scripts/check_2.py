date_list = [{'payment_date': '2021-11-24 10:30', 'payment_type': 'SALARY', 'issued_amount': 32546.0, 'utr_number': '', 'bank_debit_date': '', 're_letter_upload_number': ''}, {'payment_date': '2021-11-24 12:14', 'payment_type': 'REIMB', 'issued_amount': 20509.0, 'utr_number': '', 'bank_debit_date': '', 're_letter_upload_number': ''}, {'payment_date': '2021-11-24 13:00', 'payment_type': 'OPP', 'issued_amount': 50071.0, 'utr_number': '', 'bank_debit_date': '', 're_letter_upload_number': ''}, {'payment_date': '2021-11-24 15:00', 'payment_type': 'SALARY', 'issued_amount': 17940.0, 'utr_number': '', 'bank_debit_date': '', 're_letter_upload_number': ''}, {'payment_date': '2021-11-24 16:15', 'payment_type': 'REIMB', 'issued_amount': 169075.0, 'utr_number': '', 'bank_debit_date': '', 're_letter_upload_number': ''}, {'payment_date': '2021-11-24 16:30', 'payment_type': 'OPP', 'issued_amount': 17020.0, 'utr_number': '', 'bank_debit_date': '', 're_letter_upload_number': ''}, {'payment_date': '2021-11-24 19:30', 'payment_type': 'SALARY', 'issued_amount': 979588.0, 'utr_number': '', 'bank_debit_date': '', 're_letter_upload_number': ''}]

salary_list = []
opp_list = []

for date in date_list:
    if date["payment_type"] == "SALARY":
        salary_list.append(date)
    elif date["payment_type"]:
        pass
date_list = ['2022-01-17 13:01OPP', '2022-01-17 12:14REIMB', '2022-01-17 12:19OPP',
 '2022-01-17 10:30SALARY']

# payment_dict = {
#     "payment_type" : "",
#     "payment_date" : "",
#     "re_letter_upload_number": ""
# }

agg_list = list()

# for i in range(0, len(date_list)):
#     agg_list.append(i)

for date in date_list:
    if date.split(":")[-1][2:] == "OPP":
        agg_list.append({
            "payment_type" : "OPP",
            "payment_date" : date,
            "re_letter_upload_number" : ""
        })
    elif date.split(":")[-1][2:] == "REIMB":
        agg_list.append({
            "payment_type" : "REIMB",
            "payment_date" : date,
            "re_letter_upload_number" : ""
        })
    elif date.split(":")[-1][2:] == "SALARY":
        agg_list.append({
            "payment_type": "SALARY",
            "payment_date": date,
            "re_letter_upload_number": ""
        })

salary_list = list()
opp_list = list()
reimbursement_list = list()

for agg in agg_list:
    if agg["payment_type"] == "SALARY":
        salary_list.append(agg['payment_date'])
    elif agg["payment_type"] == "OPP":
        opp_list.append(agg['payment_date'])
    elif agg["payment_type"] == "REIMB":
        reimbursement_list.append(agg['payment_date'])

letter_number = 1

# SALARY
for salary_date in salary_list:
    if int(salary_date.split(" ")[-1].split(":")[0]) in [10,11,12] and salary_date.split(" ")[-1] != '12:30':
        for agg in agg_list:
            if agg["payment_date"] == salary_date and len(agg["re_letter_upload_number"]) == 0:
                agg["re_letter_upload_number"] = str(letter_number)
                letter_number += 1
    elif int(salary_date.split(" ")[-1].split(":")[0]) in [15,16,17]:
        for agg in agg_list:
            if agg["payment_date"] == salary_date and len(agg["re_letter_upload_number"]) == 0:
                agg["re_letter_upload_number"] = str(letter_number)
                letter_number += 1
    elif int(salary_date.split(" ")[-1].split(":")[0]) in [19,20,21,22,23,24]:
        for agg in agg_list:
            if agg["payment_date"] == salary_date and len(agg["re_letter_upload_number"]) == 0:
                agg["re_letter_upload_number"] = str(letter_number)
                letter_number += 1

# REIMBURSEMENT
for reimb_date in reimbursement_list:
    if int(reimb_date.split(" ")[-1].split(":")[0]) in [12,13,14,15]:
        for agg in agg_list:
            if agg["payment_date"] == reimb_date and len(agg["re_letter_upload_number"]) == 0:
                agg["re_letter_upload_number"] = str(letter_number)
                letter_number += 1
    elif int(reimb_date.split(" ")[-1].split(":")[0]) in [16,17,18,19,20,21,22,23,24]:
       for agg in agg_list:
           if agg["payment_date"] == reimb_date and len(agg["re_letter_upload_number"]) == 0:
               agg["re_letter_upload_number"] = str(letter_number)
               letter_number += 1

# OPP
for opp_date in opp_list:
    if int(opp_date.split(" ")[-1].split(":")[0]) in [12,13,14,15]:
        for agg in agg_list:
            if agg["payment_date"] == opp_date and len(agg["re_letter_upload_number"]) == 0:
                agg["re_letter_upload_number"] = str(letter_number)
                letter_number += 1
    elif int(opp_date.split(" ")[-1].split(":")[0]) in [16,17,18,19,20,21,22,23,24]:
        for agg in agg_list:
            if agg["payment_date"] == opp_date and len(agg["re_letter_upload_number"]) == 0:
                agg["re_letter_upload_number"] = str(letter_number)
                letter_number += 1

# SALARY - 12.30
for salary_date in salary_list:
    if salary_date.split(" ")[-1][:5] == '12:30':
        for agg in agg_list:
            if agg["payment_date"] == salary_date and len(agg["re_letter_upload_number"]) == 0:
                agg["re_letter_upload_number"] = str(letter_number)
                letter_number += 1

for agg in agg_list:
    print(agg)
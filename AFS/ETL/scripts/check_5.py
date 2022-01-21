# client_details_properties = {'url': 'http://localhost:50010/api/v1/alcs/generic/client_details/?client_name={client_name}', 'header': {'Content-Type': 'application/json'}, 'data': ''}
#
# samsung_client_url = client_details_properties["url"].split("=")
# samsung_client_url[-1] = 'Samsung'
# samsung_client_properties = "=".join(samsung_client_url)
#
# print(samsung_client_properties)
#
# bajaj_client_url = client_details_properties["url"].split("=")
# bajaj_client_url[-1] = 'Bajaj'
# bajaj_client_properties = "=".join(bajaj_client_url)
#
# print(bajaj_client_properties)

grouped_client_list = [{'client_name': 'BAJAJ', 're_letter_generated_number': '3', 're_letter_generated_number_one': '6'}, {'client_name': 'BAJAJ', 're_letter_generated_number': '5', 're_letter_generated_number_one': '6'}, {'client_name': 'OTHERS', 're_letter_generated_number': '1', 're_letter_generated_number_one': '1'}, {'client_name': 'OTHERS', 're_letter_generated_number': '2', 're_letter_generated_number_one': '2'}, {'client_name': 'OTHERS', 're_letter_generated_number': '4', 're_letter_generated_number_one': '3'}, {'client_name': 'OTHERS', 're_letter_generated_number': '6', 're_letter_generated_number_one': '4'}, {'client_name': 'OTHERS', 're_letter_generated_number': '7', 're_letter_generated_number_one': '5'}]

letter_number = 0
others_letters_numbers_list = []
for client in grouped_client_list:
    if client["client_name"] == "OTHERS":
        others_letters_numbers_list.append(int(client["re_letter_generated_number"]))

others_letters_numbers_list.sort()

for i in range(0, len(others_letters_numbers_list)):
    for client in grouped_client_list:
        if int(client["re_letter_generated_number"]) == others_letters_numbers_list[i]:
            client["re_letter_generated_number_one"] = str(i+1)
            letter_number = i + 1

samsung_letters_numbers_list = []
for client in grouped_client_list:
    if client["client_name"] == "SAMSUNG":
        samsung_letters_numbers_list.append(int(client['re_letter_generated_number']))

# print("samsung_letters_numbers_list")
# print(samsung_letters_numbers_list)

samsung_letters_numbers_list.sort()

for i in range(0, len(samsung_letters_numbers_list)):
    for client in grouped_client_list:
        if int(client["re_letter_generated_number"]) == samsung_letters_numbers_list[i]:
            client["re_letter_generated_number_one"] = str(letter_number + 1)
            letter_number = letter_number + 1

bajaj_letters_numbers_list = []
for client in grouped_client_list:
    if client["client_name"] == "BAJAJ":
        bajaj_letters_numbers_list.append(int(client['re_letter_generated_number']))

# print("bajaj_letters_numbers_list")
# print(bajaj_letters_numbers_list)

bajaj_letters_numbers_list.sort()

for i in range(0, len(bajaj_letters_numbers_list)):
    print("bajaj_letters_numbers_list[i]", bajaj_letters_numbers_list[i])
    for client in grouped_client_list:
        print(client)
        if int(client['re_letter_generated_number']) == bajaj_letters_numbers_list[i]:
            client["re_letter_generated_number_one"] = str(letter_number + 1)


# for group in grouped_client_list:
#     print(group)
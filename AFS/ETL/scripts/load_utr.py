import pandas as pd
import numpy as np

data_frame = pd.read_excel("H:/Clients/TeamLease/ALCS Letters/25012022_Proper/alcs_hdfc_neft_output_etl.xlsx", dtype=str)

data_frame['tenants_id'] = 1
data_frame['groups_id'] = 1
data_frame['entities_id'] = 1
data_frame['file_id'] = 1
data_frame['job_execution_id'] = 1
data_frame['m_processing_layer_id'] = 1
data_frame['m_processing_sub_layer_id'] = 4
data_frame['processing_layer_id'] = 404
data_frame['processing_layer_name'] = 'HDFC-NEFT LETTERS RECON'
data_frame['is_active'] = 1
data_frame['created_by'] = 1
data_frame['created_date'] = '2021-10-06 19:45:11.997120'
data_frame['modified_by'] = 1
data_frame['modified_date'] = '2021-10-06 19:45:11.997120'

insert_query = "INSERT INTO alcs_recon.stg_internal_records ( int_reference_text_1, int_reference_text_2, int_reference_text_3, int_reference_date_time_1, int_reference_text_4, int_amount_1, int_reference_text_5, int_reference_text_6, int_reference_text_7, int_reference_text_8, int_reference_text_9, int_reference_text_10, int_reference_text_11, int_reference_text_12, int_reference_text_13, int_reference_text_14, int_reference_text_15, tenants_id, groups_id, entities_id, file_id, job_execution_id, m_processing_layer_id, m_processing_sub_layer_id, processing_layer_id, processing_layer_name, is_active, created_by, created_date, modified_by, modified_date) VALUES {data_values};"

if len(data_frame) > 0:
    data_rows_list = []
    for index, rows in data_frame.iterrows():
        # create a list for the current row
        data_list = [rows[column] for column in data_frame.columns]
        data_rows_list.append(data_list)

    # Create a insert string from the list
    records = []
    for record_lists in data_rows_list:
        record_string = ''
        for record_list in record_lists:
            record_string = record_string + "'" + str(record_list) + "', "
        record_proper = "(" + record_string[:-2] + "),"
        records.append(record_proper)

    insert_value_string = ''
    for record in records:
        insert_value_string = insert_value_string + record

    final_query = insert_query.replace("{data_values}", insert_value_string[:-1])

    file = open("H:/Clients/TeamLease/ALCS Letters/25012022_Proper/hdfc_neft_25012022.sql", "w+")
    file.write(final_query)
    file.close()
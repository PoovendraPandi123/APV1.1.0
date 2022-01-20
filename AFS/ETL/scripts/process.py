import logging
import re
import pandas as pd
import etl_functions as ef
import data_request as dr

def get_process_sources(
        spark, sqlContext, alcs_spark_df, bank_spark_df, action_code_list, source_1_columns, source_2_columns,
        validate_attribute_1_row_list, validate_attribute_2_row_list, date_transform_attribute_1_row_list,
        date_transform_attribute_2_row_list, source_1_name, source_2_name, date_config_folder, date_config_file,
        aggregator_details_1_properties, aggregator_details_2_properties, field_extraction_properties,
        transformation_operators_list, source_definition_properties, client_details_properties):

    """
    :type _chunk_pattern: str
    ... other fields
    """
    try:
        validated_pandas_df_alcs = ''
        validated_pandas_df_bank = ''
        date_transformed_pandas_df_alcs = ''
        date_transformed_pandas_df_bank = ''
        field_extracted_pandas_df_bank = ''
        utr_updated_pandas_df_alcs = ''
        utr_updated_payment_date_agg_list = list()

        # Transformation Operators
        delimiter_operators = get_field_extraction_delimiter_operators(transformation_operators_list)
        # print("delimiter_operators", delimiter_operators)
        # print("delimiter_operators Type", type(delimiter_operators))
        print("action_code_list", action_code_list)

        if len(delimiter_operators) > 0:
            for action_code in action_code_list:
                if action_code == "A01_CLN_ALCS":
                    if len(alcs_spark_df.toPandas()) > 0:
                        validate_data_alcs = ef.ValidateData(
                            action_code = action_code,
                            read_spark_df = alcs_spark_df,
                            source_columns = source_1_columns,
                            validate_attribute_row_list = validate_attribute_1_row_list
                        )
                        validated_pandas_df_alcs = validate_data_alcs.get_pandas_validated_df()
                        # print("validated_pandas_df_alcs")
                        # print(validated_pandas_df_alcs)
                    else:
                        print("Length of ALCS Dataframe is equal to Zero!!!")
                        break
                elif action_code == "A02_CLN_BANK":
                    if len(bank_spark_df.toPandas()) > 0:
                        validate_data_bank = ef.ValidateData(
                            action_code = action_code,
                            read_spark_df = bank_spark_df,
                            source_columns = source_2_columns,
                            validate_attribute_row_list = validate_attribute_2_row_list
                        )
                        validated_pandas_df_bank = validate_data_bank.get_pandas_validated_df()
                        # print("validated_pandas_df_bank")
                        # print(validated_pandas_df_bank)
                    else:
                        print("Length of BANK Dataframe is equal to Zero!!!")
                        break
                elif action_code == "A01_DTF_ALCS":
                    if len(validated_pandas_df_alcs) > 0:
                        date_transform_alcs = ef.DateTransformData(
                            action_code = action_code,
                            validated_pandas_df = validated_pandas_df_alcs,
                            date_transform_attribute_row_list = date_transform_attribute_1_row_list,
                            date_config_folder = date_config_folder,
                            date_config_file = date_config_file,
                            source_name = source_1_name
                        )
                        date_transformed_pandas_df_alcs = date_transform_alcs.get_date_transformed_data()
                        # print(date_transformed_pandas_df_alcs.head(10))
                    else:
                        print("Length of Validated ALCS Dataframe is equal to Zero!!!")
                        break

                elif action_code == "A02_DTF_BANK":
                    if len(validated_pandas_df_bank) > 0:
                        date_transform_bank = ef.DateTransformData(
                            action_code = action_code,
                            validated_pandas_df = validated_pandas_df_bank,
                            date_transform_attribute_row_list = date_transform_attribute_2_row_list,
                            date_config_folder = date_config_folder,
                            date_config_file = date_config_file,
                            source_name = source_2_name
                        )
                        date_transformed_pandas_df_bank = date_transform_bank.get_date_transformed_data()
                        # print(date_transformed_pandas_df_bank.head(10))
                        # print(date_transformed_pandas_df_bank[['Date', 'Value Dt']])
                    else:
                        print("Length of Validated BANK Dataframe is equal to Zero!!!")
                        break

                elif action_code == "A02_FEX_BANK":
                    if len(date_transformed_pandas_df_bank) > 0:
                        # date_transformed_spark_df = spark.createDataFrame(date_transformed_pandas_df_bank)
                        # date_transformed_spark_df.createOrReplaceTempView("date_transformed_bank_spark_df")

                        aggregator_details_properties_bank = dr.GetResponse(aggregator_details_2_properties)
                        aggregator_details_list_bank = aggregator_details_properties_bank.get_response_data()
                        m_aggregator_id = aggregator_details_list_bank[0]["m_aggregator"]
                        m_sources_id = aggregator_details_list_bank[0]["m_sources_id"]

                        field_extraction_properties["url"] = field_extraction_properties["url"].replace("{m_aggregator_id}", str(m_aggregator_id)).replace("{m_sources_id}", str(m_sources_id))
                        field_extraction_properties_bank = dr.GetResponse(field_extraction_properties)
                        field_extraction_list_bank = field_extraction_properties_bank.get_response_data()

                        source_definition_properties["url"] = source_definition_properties["url"].replace("{source_id}", str(m_sources_id))
                        source_definition_properties_bank = dr.GetResponse(source_definition_properties)
                        source_definition_list_bank = source_definition_properties_bank.get_response_data()

                        derived_column = "utr"
                        for field_extraction in field_extraction_list_bank:
                            if field_extraction["pattern_type"] == "separator":
                                pattern_input = field_extraction["pattern_input"]
                                character = ''
                                for delimiter_operator in delimiter_operators:
                                    for k,v in delimiter_operator.items():
                                        if k == pattern_input.split("-")[0]:
                                            character = v

                                extract_position = pattern_input.split("-")[-1]
                                transaction_reference = field_extraction["transaction_reference"]
                                transaction_placed = field_extraction["transaction_placed"]
                                m_source_definitions_id = field_extraction["m_source_definitions_id"]
                                reference_field_name = get_source_definition_attribute_name(source_definition_list_bank, m_source_definitions_id)
                                # print("reference_field_name", reference_field_name)
                                # date_transformed_pandas_df_bank["utr"] = date_transformed_pandas_df_bank[reference_field_name]
                                field_extracted_pandas_df_bank = get_field_extraction(
                                    data_frame = date_transformed_pandas_df_bank,
                                    extract_position = extract_position,
                                    transaction_reference = transaction_reference,
                                    transaction_placed = transaction_placed,
                                    reference_field_name = reference_field_name,
                                    character = character,
                                    derived_column = derived_column,
                                    source_2_name = source_2_name
                                )
                                # print("Field Extracted DF")
                                # print(field_extracted_pandas_df_bank)
                                # print(field_extracted_pandas_df_bank['utr'])
                                # date_transformed_pandas_df_bank[new_column] = date_transformed_pandas_df_bank[reference_field_name]
                                # transaction_reference = transaction_reference, extract_position = extract_position,
                                # transaction_placed = transaction_placed, character = character
                                # sql_query = "select *, substring_index(\"{reference_field}\", \"{character}\", {extract_position}) from date_transformed_bank_spark_df where \"{reference_field}\" like '{conditions}';"
                                # sql_query = """select *, substring_index(substring_index(`{reference_field}`, "'{character}'", {extract_position}), "'{character}'", -1) from date_transformed_bank_spark_df;"""
                                # sql_query_proper = sql_query.replace('{reference_field}', reference_field_name).replace('{conditions}', condition).replace("{character}", character).replace("{extract_position}", str(extract_position))
                                # print("SQL Query Proper", sql_query_proper)
                                #
                                # sql_check = sqlContext.sql(sql_query_proper)
                                # print(sql_check.show())

                    else:
                        print("Length of Date Transformed Bank Dataframe is equal to Zero!!!")
                        break

                elif action_code == "A01_MTF_ALCS":
                    if len(field_extracted_pandas_df_bank) > 0:
                        if len(date_transformed_pandas_df_alcs) > 0:
                            field_name_numeric_transform = get_field_name(source_name = source_2_name)
                            # print("field_name_numeric_transform", field_name_numeric_transform)
                            numeric_converted_pandas_df_alcs = get_convert_pandas_df_numeric(
                                pandas_df = date_transformed_pandas_df_alcs,
                                field_name = "Issued Amt",
                                source_name = source_1_name
                            )
                            numeric_converted_pandas_df_bank = get_convert_pandas_df_numeric(
                                pandas_df = field_extracted_pandas_df_bank,
                                field_name = field_name_numeric_transform,
                                source_name = source_2_name
                            )
                            if len(numeric_converted_pandas_df_alcs) > 0 and len(numeric_converted_pandas_df_bank) > 0:
                                date_extracted_pandas_df_alcs = get_extraction_alcs(data_frame=numeric_converted_pandas_df_alcs)
                                date_extracted_pandas_df_alcs = get_add_unique_extraction_alcs(dataframe = date_extracted_pandas_df_alcs)

                                # date_extracted_pandas_df_alcs.to_excel("H:/Clients/TeamLease/ALCS Letters/CheckResult/transformed_alcs_etl.xlsx",sheet_name='HDFC_ALCS', index=False)

                                if len(date_extracted_pandas_df_alcs) > 0:
                                    # Adding Bank Reference Column with Default Value
                                    date_extracted_pandas_df_alcs['bank_reference_column'] = ''
                                    date_extracted_pandas_df_alcs['re_letter_generated_number'] = ''
                                    math_transformed_df_alcs = date_extracted_pandas_df_alcs.groupby(["pm_payment_date_unique_proper"])['Issued Amt'].sum().reset_index()
                                    # print("math_transformed_df_alcs")
                                    # print(math_transformed_df_alcs)
                                    payment_date_aggregated_list = list()

                                    for i in range(0, len(math_transformed_df_alcs)):
                                        payment_date_aggregated_list.append({
                                            "payment_date": str(math_transformed_df_alcs["pm_payment_date_unique_proper"][i]),
                                            "payment_type": get_extract_payment_type(str(math_transformed_df_alcs["pm_payment_date_unique_proper"][i])),
                                            "issued_amount": float(math_transformed_df_alcs["Issued Amt"][i]),
                                            "utr_number": "",
                                            "bank_debit_date": "",
                                            "re_letter_upload_number": "",
                                            "bank_reference_text": ""
                                        })

                                    # print("payment_date_aggregated_list", payment_date_aggregated_list)
                                    utr_updated_payment_date_agg_list = get_update_utr_agg_list(bank_df = numeric_converted_pandas_df_bank, aggregate_list=payment_date_aggregated_list, source_2_name=source_2_name)
                                    # print("utr_updated_payment_date_agg_list", utr_updated_payment_date_agg_list)
                                    for payment_date_agg in utr_updated_payment_date_agg_list:
                                        date_extracted_pandas_df_alcs.loc[date_extracted_pandas_df_alcs['pm_payment_date_unique_proper'] == payment_date_agg["payment_date"], ['UTR Number']] = payment_date_agg["utr_number"]
                                        date_extracted_pandas_df_alcs.loc[date_extracted_pandas_df_alcs['pm_payment_date_unique_proper'] == payment_date_agg["payment_date"], ['Debit Date']] = payment_date_agg["bank_debit_date"]
                                        date_extracted_pandas_df_alcs.loc[date_extracted_pandas_df_alcs['pm_payment_date_unique_proper'] == payment_date_agg["payment_date"], ['bank_reference_column']] = payment_date_agg["bank_reference_text"]

                                    utr_updated_pandas_df_alcs = date_extracted_pandas_df_alcs
                                    # print("utr_updated_payment_date_agg_list", utr_updated_payment_date_agg_list)
                                    # print(date_extracted_pandas_df_alcs)
                                    # print(date_extracted_pandas_df_alcs[['pm_payment_date_proper', 'UTR Number', 'Debit Date']])
                                    # date_extracted_pandas_df_alcs.to_excel("H:/Clients/TeamLease/ALCS Letters/CheckResult/hdfc_alcs_etl.xlsx", sheet_name='HDFC_ALCS', index=False)

                                else:
                                    print("Length of Date Extracted ALCS Dataframe is equal to Zero!!!")
                            else:
                                print("Length of Numeric Converted ALCS Dataframe or Bank Dataframe is equal to Zero!!!")
                        else:
                            print("Length of Date Transformed ALCS Dataframe is equal to Zero!!!")
                    else:
                        print("Length of Field Extracted Bank Dataframe is equal to Zero!!!")

                elif action_code == "A01_REP_ALCS":
                    if len(utr_updated_pandas_df_alcs) > 0:
                        if len(utr_updated_payment_date_agg_list) > 0:
                            upload_number_update_output = get_upload_number_update(agg_list = utr_updated_payment_date_agg_list, letter_number=1)
                            upload_number_updated_agg_list = upload_number_update_output[0]
                            letter_number = upload_number_update_output[1]
                            # print("upload_number_updated_agg_list", upload_number_updated_agg_list)

                            # Letter Number Column for BANK
                            numeric_converted_pandas_df_bank['letter_number'] = ''

                            for agg in upload_number_updated_agg_list:
                                date_extracted_pandas_df_alcs.loc[date_extracted_pandas_df_alcs['pm_payment_date_unique_proper'] == agg["payment_date"], ['re_letter_generated_number']] = agg['re_letter_upload_number']
                                numeric_converted_pandas_df_bank.loc[numeric_converted_pandas_df_bank['Transaction Particulars'] == agg["bank_reference_text"], ['letter_number']] = agg['re_letter_upload_number']

                            letter_no_not_generated_alcs_df = date_extracted_pandas_df_alcs[date_extracted_pandas_df_alcs['re_letter_generated_number'] == ""]
                            # print("letter_no_not_generated_alcs_df")
                            # print(letter_no_not_generated_alcs_df)
                            if len(letter_no_not_generated_alcs_df) > 0:
                                math_transformed_df_alcs_second = letter_no_not_generated_alcs_df.groupby(["pm_payment_date_unique_proper_second"])['Issued Amt'].sum().reset_index()
                                # print("math_transformed_df_alcs_second")
                                # print(math_transformed_df_alcs_second)
                                payment_date_aggregated_list_second = list()

                                for i in range(0, len(math_transformed_df_alcs_second)):
                                    payment_date_aggregated_list_second.append({
                                        "payment_date": str(math_transformed_df_alcs_second["pm_payment_date_unique_proper_second"][i]),
                                        "payment_type": get_extract_payment_type_second(str(math_transformed_df_alcs_second["pm_payment_date_unique_proper_second"][i])),
                                        "issued_amount": float(math_transformed_df_alcs_second["Issued Amt"][i]),
                                        "utr_number": "",
                                        "bank_debit_date": "",
                                        "re_letter_upload_number": "",
                                        "bank_reference_text": ""
                                    })

                                # print('payment_date_aggregated_list_second', payment_date_aggregated_list_second)
                                # for agg in payment_date_aggregated_list_second:
                                #     print(agg)
                                utr_updated_payment_date_agg_list_second = get_update_utr_agg_list(bank_df=numeric_converted_pandas_df_bank, aggregate_list=payment_date_aggregated_list_second, source_2_name=source_2_name)
                                #
                                # print("utr_updated_payment_date_agg_list_second")
                                # for agg in utr_updated_payment_date_agg_list_second:
                                #     print(agg)

                                for payment_date_agg_second in utr_updated_payment_date_agg_list_second:
                                    date_extracted_pandas_df_alcs.loc[date_extracted_pandas_df_alcs['pm_payment_date_unique_proper_second'] == payment_date_agg_second["payment_date"], ['UTR Number']] = payment_date_agg_second["utr_number"]
                                    date_extracted_pandas_df_alcs.loc[date_extracted_pandas_df_alcs['pm_payment_date_unique_proper_second'] == payment_date_agg_second["payment_date"], ['Debit Date']] = payment_date_agg_second["bank_debit_date"]
                                    date_extracted_pandas_df_alcs.loc[date_extracted_pandas_df_alcs['pm_payment_date_unique_proper_second'] == payment_date_agg_second["payment_date"], ['bank_reference_column']] = payment_date_agg_second["bank_reference_text"]

                                utr_updated_pandas_df_alcs_second = date_extracted_pandas_df_alcs
                                upload_number_update_second_output = get_upload_number_update_second(agg_list=utr_updated_payment_date_agg_list_second, letter_number=letter_number)
                                #
                                upload_number_updated_agg_list_second = upload_number_update_second_output[0]
                                #
                                # print("upload_number_updated_agg_list_second", upload_number_updated_agg_list_second)
                                #
                                for agg in upload_number_updated_agg_list_second:
                                    utr_updated_pandas_df_alcs_second.loc[utr_updated_pandas_df_alcs_second['pm_payment_date_unique_proper_second'] == agg["payment_date"], ['re_letter_generated_number']] = agg['re_letter_upload_number']
                                    numeric_converted_pandas_df_bank.loc[numeric_converted_pandas_df_bank['Transaction Particulars'] == agg["bank_reference_text"], ['letter_number']] = agg['re_letter_upload_number']
                                # #

                                letter_no_not_generated_alcs_second_df = utr_updated_pandas_df_alcs_second[utr_updated_pandas_df_alcs_second['re_letter_generated_number'] == ""]

                                if len(letter_no_not_generated_alcs_second_df) > 0:
                                    math_transformed_letter_no_not_generated_df_alcs_second = letter_no_not_generated_alcs_second_df['pm_payment_date_unique_proper'].unique()
                                    math_transformed_letter_no_not_generated_df_alcs_second_list = math_transformed_letter_no_not_generated_df_alcs_second.tolist()
                                    upload_number_updated_agg_list_no_utr = get_upload_update_no_utr(date_list=math_transformed_letter_no_not_generated_df_alcs_second_list,
                                                                                                     letter_number=letter_number_second)

                                    for agg in upload_number_updated_agg_list_no_utr:
                                        utr_updated_pandas_df_alcs_second.loc[utr_updated_pandas_df_alcs_second['pm_payment_date_unique_proper'] == agg["payment_date"], ['re_letter_generated_number']] = agg['re_letter_upload_number']

                                    utr_updated_pandas_df_alcs_second.to_excel("H:/Clients/TeamLease/ALCS Letters/Outputs/20012022/alcs_axis_output_etl.xlsx", sheet_name='AXIS_ALCS', index=False)
                                    numeric_converted_pandas_df_bank.to_excel("H:/Clients/TeamLease/ALCS Letters/Outputs/20012022/alcs_axis_output_bank_etl.xlsx", sheet_name='AXIS_BANK', index=False)
                                else:
                                    utr_updated_pandas_df_alcs_second.to_excel("H:/Clients/TeamLease/ALCS Letters/Outputs/20012022/alcs_axis_output_etl.xlsx", sheet_name='AXIS_ALCS', index=False)
                                    numeric_converted_pandas_df_bank.to_excel("H:/Clients/TeamLease/ALCS Letters/Outputs/20012022/alcs_axis_output_bank_etl.xlsx",sheet_name='AXIS_BANK', index=False)
                            else:
                                date_extracted_pandas_df_alcs.to_excel("H:/Clients/TeamLease/ALCS Letters/Outputs/20012022/alcs_axis_output_etl.xlsx", sheet_name='AXIS_ALCS', index=False)
                                numeric_converted_pandas_df_bank.to_excel("H:/Clients/TeamLease/ALCS Letters/Outputs/20012022/alcs_axis_output_bank_etl.xlsx", sheet_name='AXIS_BANK', index=False)
                            # letter_no_not_generated_alcs_df.to_excel("H:/Clients/TeamLease/ALCS Letters/Outputs/alcs_axis_output_letter_no_etl.xlsx", sheet_name='AXIS_ALCS', index=False)
                        else:
                            print("Length of UTR Updated Payment Date Agg List is equal to Zero!!!")
                    else:
                        print("Length of UTR Updated Pandas Dataframe is equal to Zero!!!")

                elif action_code == "A05_REP_ALCS":
                    if len(utr_updated_pandas_df_alcs) > 0:
                        if len(utr_updated_payment_date_agg_list) > 0:
                            upload_number_update_output = get_upload_number_update(agg_list = utr_updated_payment_date_agg_list, letter_number=1)
                            upload_number_updated_agg_list = upload_number_update_output[0]
                            letter_number = upload_number_update_output[1]
                            # print("upload_number_updated_agg_list", upload_number_updated_agg_list)

                            # Letter Number Column for BANK
                            numeric_converted_pandas_df_bank['letter_number'] = ''

                            for agg in upload_number_updated_agg_list:
                                date_extracted_pandas_df_alcs.loc[date_extracted_pandas_df_alcs['pm_payment_date_unique_proper'] == agg["payment_date"], ['re_letter_generated_number']] = agg['re_letter_upload_number']
                                numeric_converted_pandas_df_bank.loc[numeric_converted_pandas_df_bank['Description'] == agg["bank_reference_text"], ['letter_number']] = agg['re_letter_upload_number']

                            letter_no_not_generated_alcs_df = date_extracted_pandas_df_alcs[date_extracted_pandas_df_alcs['re_letter_generated_number'] == ""]
                            # print("letter_no_not_generated_alcs_df")
                            # print(letter_no_not_generated_alcs_df)
                            if len(letter_no_not_generated_alcs_df) > 0:
                                math_transformed_df_alcs_second = letter_no_not_generated_alcs_df.groupby(["pm_payment_date_unique_proper_second"])['Issued Amt'].sum().reset_index()
                                # print("math_transformed_df_alcs_second")
                                # print(math_transformed_df_alcs_second)
                                payment_date_aggregated_list_second = list()

                                for i in range(0, len(math_transformed_df_alcs_second)):
                                    payment_date_aggregated_list_second.append({
                                        "payment_date": str(math_transformed_df_alcs_second["pm_payment_date_unique_proper_second"][i]),
                                        "payment_type": get_extract_payment_type_second(str(math_transformed_df_alcs_second["pm_payment_date_unique_proper_second"][i])),
                                        "issued_amount": float(math_transformed_df_alcs_second["Issued Amt"][i]),
                                        "utr_number": "",
                                        "bank_debit_date": "",
                                        "re_letter_upload_number": "",
                                        "bank_reference_text": ""
                                    })

                                # print('payment_date_aggregated_list_second', payment_date_aggregated_list_second)
                                # for agg in payment_date_aggregated_list_second:
                                #     print(agg)
                                utr_updated_payment_date_agg_list_second = get_update_utr_agg_list(bank_df=numeric_converted_pandas_df_bank, aggregate_list=payment_date_aggregated_list_second, source_2_name = source_2_name)
                                #
                                # print("utr_updated_payment_date_agg_list_second")
                                # for agg in utr_updated_payment_date_agg_list_second:
                                #     print(agg)

                                for payment_date_agg_second in utr_updated_payment_date_agg_list_second:
                                    date_extracted_pandas_df_alcs.loc[date_extracted_pandas_df_alcs['pm_payment_date_unique_proper_second'] == payment_date_agg_second["payment_date"], ['UTR Number']] = payment_date_agg_second["utr_number"]
                                    date_extracted_pandas_df_alcs.loc[date_extracted_pandas_df_alcs['pm_payment_date_unique_proper_second'] == payment_date_agg_second["payment_date"], ['Debit Date']] = payment_date_agg_second["bank_debit_date"]
                                    date_extracted_pandas_df_alcs.loc[date_extracted_pandas_df_alcs['pm_payment_date_unique_proper_second'] == payment_date_agg_second["payment_date"], ['bank_reference_column']] = payment_date_agg_second["bank_reference_text"]

                                utr_updated_pandas_df_alcs_second = date_extracted_pandas_df_alcs
                                upload_number_update_second_output = get_upload_number_update_second(agg_list=utr_updated_payment_date_agg_list_second, letter_number=letter_number)
                                #
                                upload_number_updated_agg_list_second = upload_number_update_second_output[0]
                                #
                                # print("upload_number_updated_agg_list_second", upload_number_updated_agg_list_second)
                                #
                                for agg in upload_number_updated_agg_list_second:
                                    utr_updated_pandas_df_alcs_second.loc[utr_updated_pandas_df_alcs_second['pm_payment_date_unique_proper_second'] == agg["payment_date"], ['re_letter_generated_number']] = agg['re_letter_upload_number']
                                    numeric_converted_pandas_df_bank.loc[numeric_converted_pandas_df_bank['Description'] == agg["bank_reference_text"], ['letter_number']] = agg['re_letter_upload_number']
                                # #

                                letter_no_not_generated_alcs_second_df = utr_updated_pandas_df_alcs_second[utr_updated_pandas_df_alcs_second['re_letter_generated_number'] == ""]

                                if len(letter_no_not_generated_alcs_second_df) > 0:
                                    math_transformed_letter_no_not_generated_df_alcs_second = letter_no_not_generated_alcs_second_df['pm_payment_date_unique_proper'].unique()
                                    math_transformed_letter_no_not_generated_df_alcs_second_list = math_transformed_letter_no_not_generated_df_alcs_second.tolist()
                                    upload_number_updated_agg_list_no_utr = get_upload_update_no_utr(date_list=math_transformed_letter_no_not_generated_df_alcs_second_list,
                                                                                                     letter_number=letter_number_second)

                                    for agg in upload_number_updated_agg_list_no_utr:
                                        utr_updated_pandas_df_alcs_second.loc[utr_updated_pandas_df_alcs_second['pm_payment_date_unique_proper'] == agg["payment_date"], ['re_letter_generated_number']] = agg['re_letter_upload_number']

                                    utr_updated_pandas_df_alcs_second.to_excel("H:/Clients/TeamLease/ALCS Letters/Outputs/20012022/alcs_icici_output_etl.xlsx", sheet_name='ICICI_ALCS', index=False)
                                    numeric_converted_pandas_df_bank.to_excel("H:/Clients/TeamLease/ALCS Letters/Outputs/20012022/alcs_icici_output_bank_etl.xlsx", sheet_name='ICICI_BANK', index=False)
                                else:
                                    utr_updated_pandas_df_alcs_second.to_excel("H:/Clients/TeamLease/ALCS Letters/Outputs/20012022/alcs_icici_output_etl.xlsx", sheet_name='ICICI_ALCS', index=False)
                                    numeric_converted_pandas_df_bank.to_excel("H:/Clients/TeamLease/ALCS Letters/Outputs/20012022/alcs_icici_output_bank_etl.xlsx",sheet_name='ICICI_BANK', index=False)
                            else:
                                date_extracted_pandas_df_alcs.to_excel("H:/Clients/TeamLease/ALCS Letters/Outputs/20012022/alcs_icici_output_etl.xlsx", sheet_name='ICICI_ALCS', index=False)
                                numeric_converted_pandas_df_bank.to_excel("H:/Clients/TeamLease/ALCS Letters/Outputs/20012022/alcs_icici_output_bank_etl.xlsx", sheet_name='ICICI_BANK', index=False)
                            # letter_no_not_generated_alcs_df.to_excel("H:/Clients/TeamLease/ALCS Letters/Outputs/alcs_axis_output_letter_no_etl.xlsx", sheet_name='AXIS_ALCS', index=False)
                        else:
                            print("Length of UTR Updated Payment Date Agg List is equal to Zero!!!")
                    else:
                        print("Length of UTR Updated Pandas Dataframe is equal to Zero!!!")

                elif action_code == "A07_REP_ALCS":
                    if len(utr_updated_pandas_df_alcs) > 0:
                        if len(utr_updated_payment_date_agg_list) > 0:
                            upload_number_update_output = get_upload_number_update(agg_list = utr_updated_payment_date_agg_list, letter_number=1)
                            upload_number_updated_agg_list = upload_number_update_output[0]
                            letter_number = upload_number_update_output[1]
                            # print("upload_number_updated_agg_list", upload_number_updated_agg_list)

                            # Letter Number Column for BANK
                            numeric_converted_pandas_df_bank['letter_number'] = ''

                            for agg in upload_number_updated_agg_list:
                                date_extracted_pandas_df_alcs.loc[date_extracted_pandas_df_alcs['pm_payment_date_unique_proper'] == agg["payment_date"], ['re_letter_generated_number']] = agg['re_letter_upload_number']
                                numeric_converted_pandas_df_bank.loc[numeric_converted_pandas_df_bank['Ref No./Cheque No.'] == agg["bank_reference_text"], ['letter_number']] = agg['re_letter_upload_number']

                            letter_no_not_generated_alcs_df = date_extracted_pandas_df_alcs[date_extracted_pandas_df_alcs['re_letter_generated_number'] == ""]
                            # print("letter_no_not_generated_alcs_df")
                            # print(letter_no_not_generated_alcs_df)
                            if len(letter_no_not_generated_alcs_df) > 0:
                                math_transformed_df_alcs_second = letter_no_not_generated_alcs_df.groupby(["pm_payment_date_unique_proper_second"])['Issued Amt'].sum().reset_index()
                                # print("math_transformed_df_alcs_second")
                                # print(math_transformed_df_alcs_second)
                                payment_date_aggregated_list_second = list()

                                for i in range(0, len(math_transformed_df_alcs_second)):
                                    payment_date_aggregated_list_second.append({
                                        "payment_date": str(math_transformed_df_alcs_second["pm_payment_date_unique_proper_second"][i]),
                                        "payment_type": get_extract_payment_type_second(str(math_transformed_df_alcs_second["pm_payment_date_unique_proper_second"][i])),
                                        "issued_amount": float(math_transformed_df_alcs_second["Issued Amt"][i]),
                                        "utr_number": "",
                                        "bank_debit_date": "",
                                        "re_letter_upload_number": "",
                                        "bank_reference_text": ""
                                    })

                                # print('payment_date_aggregated_list_second', payment_date_aggregated_list_second)
                                # for agg in payment_date_aggregated_list_second:
                                #     print(agg)
                                utr_updated_payment_date_agg_list_second = get_update_utr_agg_list(bank_df=numeric_converted_pandas_df_bank, aggregate_list=payment_date_aggregated_list_second, source_2_name = source_2_name)
                                #
                                # print("utr_updated_payment_date_agg_list_second")
                                # for agg in utr_updated_payment_date_agg_list_second:
                                #     print(agg)

                                for payment_date_agg_second in utr_updated_payment_date_agg_list_second:
                                    date_extracted_pandas_df_alcs.loc[date_extracted_pandas_df_alcs['pm_payment_date_unique_proper_second'] == payment_date_agg_second["payment_date"], ['UTR Number']] = payment_date_agg_second["utr_number"]
                                    date_extracted_pandas_df_alcs.loc[date_extracted_pandas_df_alcs['pm_payment_date_unique_proper_second'] == payment_date_agg_second["payment_date"], ['Debit Date']] = payment_date_agg_second["bank_debit_date"]
                                    date_extracted_pandas_df_alcs.loc[date_extracted_pandas_df_alcs['pm_payment_date_unique_proper_second'] == payment_date_agg_second["payment_date"], ['bank_reference_column']] = payment_date_agg_second["bank_reference_text"]

                                utr_updated_pandas_df_alcs_second = date_extracted_pandas_df_alcs
                                upload_number_update_second_output = get_upload_number_update_second(agg_list=utr_updated_payment_date_agg_list_second, letter_number=letter_number)
                                #
                                upload_number_updated_agg_list_second = upload_number_update_second_output[0]
                                letter_number_second = upload_number_update_second_output[1]
                                #
                                # print("upload_number_updated_agg_list_second", upload_number_updated_agg_list_second)
                                #
                                for agg in upload_number_updated_agg_list_second:
                                    utr_updated_pandas_df_alcs_second.loc[utr_updated_pandas_df_alcs_second['pm_payment_date_unique_proper_second'] == agg["payment_date"], ['re_letter_generated_number']] = agg['re_letter_upload_number']
                                    numeric_converted_pandas_df_bank.loc[numeric_converted_pandas_df_bank['Ref No./Cheque No.'] == agg["bank_reference_text"], ['letter_number']] = agg['re_letter_upload_number']
                                # #
                                letter_no_not_generated_alcs_second_df = utr_updated_pandas_df_alcs_second[utr_updated_pandas_df_alcs_second['re_letter_generated_number'] == ""]

                                if len(letter_no_not_generated_alcs_second_df) > 0:
                                    math_transformed_letter_no_not_generated_df_alcs_second = letter_no_not_generated_alcs_second_df['pm_payment_date_unique_proper'].unique()
                                    math_transformed_letter_no_not_generated_df_alcs_second_list = math_transformed_letter_no_not_generated_df_alcs_second.tolist()
                                    upload_number_updated_agg_list_no_utr = get_upload_update_no_utr(date_list=math_transformed_letter_no_not_generated_df_alcs_second_list,
                                                                                                     letter_number=letter_number_second)
                                    # print("upload_number_updated_agg_list_no_utr")
                                    # print(upload_number_updated_agg_list_no_utr)
                                    for agg in upload_number_updated_agg_list_no_utr:
                                        utr_updated_pandas_df_alcs_second.loc[utr_updated_pandas_df_alcs_second['pm_payment_date_unique_proper'] == agg["payment_date"], ['re_letter_generated_number']] = agg['re_letter_upload_number']

                                    utr_updated_pandas_df_alcs_second.to_excel("H:/Clients/TeamLease/ALCS Letters/Outputs/20012022/alcs_sbi_output_etl.xlsx", sheet_name='SBI_ALCS', index=False)
                                    numeric_converted_pandas_df_bank.to_excel("H:/Clients/TeamLease/ALCS Letters/Outputs/20012022/alcs_sbi_output_bank_etl.xlsx", sheet_name='SBI_BANK', index=False)
                                else:
                                    utr_updated_pandas_df_alcs_second.to_excel("H:/Clients/TeamLease/ALCS Letters/Outputs/20012022/alcs_sbi_output_etl.xlsx", sheet_name='SBI_ALCS', index=False)
                                    numeric_converted_pandas_df_bank.to_excel("H:/Clients/TeamLease/ALCS Letters/Outputs/20012022/alcs_sbi_output_bank_etl.xlsx",sheet_name='SBI_BANK', index=False)
                            else:
                                date_extracted_pandas_df_alcs.to_excel("H:/Clients/TeamLease/ALCS Letters/Outputs/20012022/alcs_sbi_output_etl.xlsx", sheet_name='SBI_ALCS', index=False)
                                numeric_converted_pandas_df_bank.to_excel("H:/Clients/TeamLease/ALCS Letters/Outputs/20012022/alcs_sbi_output_bank_etl.xlsx", sheet_name='SBI_BANK', index=False)
                            # letter_no_not_generated_alcs_df.to_excel("H:/Clients/TeamLease/ALCS Letters/Outputs/alcs_axis_output_letter_no_etl.xlsx", sheet_name='AXIS_ALCS', index=False)
                        else:
                            print("Length of UTR Updated Payment Date Agg List is equal to Zero!!!")
                    else:
                        print("Length of UTR Updated Pandas Dataframe is equal to Zero!!!")

                elif action_code == "A09_REP_ALCS":
                    if len(utr_updated_pandas_df_alcs) > 0:
                        if len(utr_updated_payment_date_agg_list) > 0:
                            upload_number_update_output = get_upload_number_update(agg_list = utr_updated_payment_date_agg_list, letter_number=1)
                            upload_number_updated_agg_list = upload_number_update_output[0]
                            letter_number = upload_number_update_output[1]

                            # Letter Number Column for BANK
                            numeric_converted_pandas_df_bank['letter_number'] = ''

                            for agg in upload_number_updated_agg_list:
                                date_extracted_pandas_df_alcs.loc[date_extracted_pandas_df_alcs['pm_payment_date_unique_proper'] == agg["payment_date"], ['re_letter_generated_number']] = agg['re_letter_upload_number']
                                numeric_converted_pandas_df_bank.loc[numeric_converted_pandas_df_bank['Narration'] == agg["bank_reference_text"], ['letter_number']] = agg['re_letter_upload_number']

                            letter_no_not_generated_alcs_df = date_extracted_pandas_df_alcs[date_extracted_pandas_df_alcs['re_letter_generated_number'] == ""]

                            alcs_df = date_extracted_pandas_df_alcs
                            bank_df = numeric_converted_pandas_df_bank

                            if len(letter_no_not_generated_alcs_df) > 0:
                                math_transformed_df_alcs_second = letter_no_not_generated_alcs_df.groupby(["pm_payment_date_unique_proper_second"])['Issued Amt'].sum().reset_index()

                                payment_date_aggregated_list_second = list()

                                for i in range(0, len(math_transformed_df_alcs_second)):
                                    payment_date_aggregated_list_second.append({
                                        "payment_date": str(math_transformed_df_alcs_second["pm_payment_date_unique_proper_second"][i]),
                                        "payment_type": get_extract_payment_type_second(str(math_transformed_df_alcs_second["pm_payment_date_unique_proper_second"][i])),
                                        "issued_amount": float(math_transformed_df_alcs_second["Issued Amt"][i]),
                                        "utr_number": "",
                                        "bank_debit_date": "",
                                        "re_letter_upload_number": "",
                                        "bank_reference_text": ""
                                    })

                                utr_updated_payment_date_agg_list_second = get_update_utr_agg_list(bank_df=numeric_converted_pandas_df_bank, aggregate_list=payment_date_aggregated_list_second, source_2_name = source_2_name)

                                for payment_date_agg_second in utr_updated_payment_date_agg_list_second:
                                    date_extracted_pandas_df_alcs.loc[date_extracted_pandas_df_alcs['pm_payment_date_unique_proper_second'] == payment_date_agg_second["payment_date"], ['UTR Number']] = payment_date_agg_second["utr_number"]
                                    date_extracted_pandas_df_alcs.loc[date_extracted_pandas_df_alcs['pm_payment_date_unique_proper_second'] == payment_date_agg_second["payment_date"], ['Debit Date']] = payment_date_agg_second["bank_debit_date"]
                                    date_extracted_pandas_df_alcs.loc[date_extracted_pandas_df_alcs['pm_payment_date_unique_proper_second'] == payment_date_agg_second["payment_date"], ['bank_reference_column']] = payment_date_agg_second["bank_reference_text"]

                                utr_updated_pandas_df_alcs_second = date_extracted_pandas_df_alcs
                                upload_number_update_second_output = get_upload_number_update_second(agg_list=utr_updated_payment_date_agg_list_second, letter_number=letter_number)

                                upload_number_updated_agg_list_second = upload_number_update_second_output[0]

                                for agg in upload_number_updated_agg_list_second:
                                    utr_updated_pandas_df_alcs_second.loc[utr_updated_pandas_df_alcs_second['pm_payment_date_unique_proper_second'] == agg["payment_date"], ['re_letter_generated_number']] = agg['re_letter_upload_number']
                                    numeric_converted_pandas_df_bank.loc[numeric_converted_pandas_df_bank['Narration'] == agg["bank_reference_text"], ['letter_number']] = agg['re_letter_upload_number']

                                letter_no_not_generated_alcs_second_df = utr_updated_pandas_df_alcs_second[utr_updated_pandas_df_alcs_second['re_letter_generated_number'] == ""]

                                alcs_df = letter_no_not_generated_alcs_second_df
                                bank_df = numeric_converted_pandas_df_bank

                                if len(letter_no_not_generated_alcs_second_df) > 0:
                                    math_transformed_letter_no_not_generated_df_alcs_second = letter_no_not_generated_alcs_second_df['pm_payment_date_unique_proper'].unique()
                                    math_transformed_letter_no_not_generated_df_alcs_second_list = math_transformed_letter_no_not_generated_df_alcs_second.tolist()
                                    upload_number_updated_agg_list_no_utr = get_upload_update_no_utr(date_list=math_transformed_letter_no_not_generated_df_alcs_second_list,
                                                                                                     letter_number=letter_number_second)

                                    for agg in upload_number_updated_agg_list_no_utr:
                                        utr_updated_pandas_df_alcs_second.loc[utr_updated_pandas_df_alcs_second['pm_payment_date_unique_proper'] == agg["payment_date"], ['re_letter_generated_number']] = agg['re_letter_upload_number']

                                    alcs_df = utr_updated_pandas_df_alcs_second
                                    bank_df = numeric_converted_pandas_df_bank

                            # if len(alcs_df) > 0 and len(bank_df) > 0:
                            #     alcs_df.to_excel("H:/Clients/TeamLease/ALCS Letters/Outputs/20012022/alcs_hdfc_output_etl.xlsx", sheet_name='HDFC_ALCS', index=False)
                            #     bank_df.to_excel("H:/Clients/TeamLease/ALCS Letters/Outputs/20012022/alcs_hdfc_output_bank_etl.xlsx", sheet_name='HDFC_BANK', index=False)
                        else:
                            print("Length of UTR Updated Payment Date Agg List is equal to Zero!!!")
                    else:
                        print("Length of UTR Updated Pandas Dataframe is equal to Zero!!!")

                elif action_code == "A09_REP_CL_ALCS":
                    if len(alcs_df) > 0 and len(bank_df) > 0:
                        alcs_df['client_name'] = 'OTHERS'
                        alcs_df['re_letter_generated_number_one'] = ''
                        bank_df['re_letter_generated_number_one'] = ''

                        samsung_client_data = get_client_details(client_properties = client_details_properties, client_name = 'Samsung')
                        bajaj_client_data = get_client_details(client_properties = client_details_properties, client_name = 'Bajaj')

                        samsung_client_list = []
                        for samsung_data in samsung_client_data:
                            samsung_client_list.append(samsung_data['client_id'])

                        bajaj_client_list = []
                        for bajaj_data in bajaj_client_data:
                            bajaj_client_list.append(bajaj_data['client_id'])

                        for client in samsung_client_list:
                            alcs_df.loc[alcs_df['Client Id'] == client, ['client_name']] = 'SAMSUNG'
                        for client in bajaj_client_list:
                            alcs_df.loc[alcs_df['Client Id'] == client, ['client_name']] = 'BAJAJ'

                        alcs_df_grouped_client = alcs_df.groupby(["client_name", "re_letter_generated_number"]).size().reset_index()
                        print("alcs_df_grouped_client")
                        print(alcs_df_grouped_client)

                        grouped_client_list = list()
                        for i in range(0, len(alcs_df_grouped_client)):
                            grouped_client_list.append({
                                "client_name": alcs_df_grouped_client['client_name'][i],
                                "re_letter_generated_number": alcs_df_grouped_client['re_letter_generated_number'][i],
                                "re_letter_generated_number_one": ""
                            })

                        grouped_client_list_updated = get_client_letter_number(group_list = grouped_client_list)
                        print("grouped_client_list_updated")
                        print(grouped_client_list_updated)

                        # alcs_df.loc[alcs_df['client_name'] == "", ['client_name']] = 'OTHERS'
                        # alcs_df.to_excel("H:/Clients/TeamLease/ALCS Letters/CheckResult/alcs_hdfc_client_output_etl.xlsx", sheet_name='HDFC_ALCS', index=False)

                    else:
                        print("Length of ALCS Dataframe and Bank Dataframe is equal to Zero!!!")
        else:
            print("Length of Delimiter Operators is equal to Zero!!!")

    except Exception:
        logging.error("Error in Get Process Sources Function!!!", exc_info=True)

def get_process_alcs(alcs_spark_df, action_code_list):
    try:
        pass
    except Exception:
        logging.error("Error in Get Process ALCS Function!!!", exc_info=True)

def get_process_bank(bank_spark_df, action_code_list):
    try:
        pass
    except Exception:
        logging.error("Error in Get Process BANK Function!!!", exc_info=True)

def get_field_extraction_delimiter_operators(transformation_operators_list):
    try:
        if len(transformation_operators_list) > 0:
            for transformation_operators in transformation_operators_list:
                # print(transformation_operators)
                if transformation_operators["operator_key"] == "delimiter_operators":
                    return transformation_operators["operator_value"]
        else:
            print("Length of Transformation Operators List is equals to Zero!!!")
            return ''
    except Exception:
        logging.error("Error in Getting Field Extraction Operators Function!!!", exc_info=True)
        return ""

def get_source_definition_attribute_name(source_definitions_list, source_definition_id):
    try:
        if len(source_definitions_list) > 0:
            if len(str(source_definition_id)) > 0 and str(source_definition_id) != '0':
                for source_definitions in source_definitions_list:
                    if str(source_definitions["id"]) == str(source_definition_id):
                        return source_definitions["attribute_name"]
            else:
                print("Source Definition Id Not Found!!!")
        else:
            print("Length of Source Definitions List is equals to Zero!!!")
            return ""
    except Exception:
        logging.error("Error in Getting Source Definition Attribute Name Function!!!", exc_info=True)
        return ""

def get_field_extraction(data_frame, extract_position,  transaction_reference, transaction_placed, reference_field_name, character, derived_column, source_2_name):
    try:
        data_frame[derived_column]  = data_frame[reference_field_name].apply(get_extract_text, extract_position = extract_position,
                                                                             transaction_reference = transaction_reference,
                                                                             transaction_placed = transaction_placed,
                                                                             character = character,
                                                                             source_2_name = source_2_name)
        return data_frame

    except Exception as e:
        print(e)
        return ""

def get_extraction_alcs(data_frame):
    try:
        data_frame["pm_payment_date_proper"] = data_frame["PM_Payment_Date"].apply(get_extract_alcs_date)
        data_frame["pm_payment_date_proper_second"] = data_frame["PM_Payment_Date"].apply(get_extract_alcs_date_second)
        data_frame["payment_type"] = data_frame["Remarks"].apply(get_extract_alcs_remarks)
        return data_frame
    except Exception as e:
        print(e)
        return ""

def get_extract_text(text, **kwargs):
    try:
        extract_position = kwargs["extract_position"]
        transaction_reference = kwargs["transaction_reference"]
        transaction_placed = kwargs["transaction_placed"]
        character = kwargs["character"]
        source_2_name = kwargs["source_2_name"]
        # print("Extract Text", text)
        # print("character", character)
        if re.search(r'axis', source_2_name.lower()) or re.search(r'icici', source_2_name.lower()) or re.search(r'sbi', source_2_name.lower()):
            if transaction_placed == "inbetween":
                if re.search(transaction_reference, text):
                    extracted_text = text.split(character)[int(extract_position)-1]
                    # print("Extracted Text", extracted_text)
                    return extracted_text
                else:
                    return ""
            else:
                return ""

        elif re.search(r'hdfc', source_2_name.lower()):
            # print("HDFC Extracted Text", text.replace(" ", ""))
            return text.replace(" ", "")
        else:
            return ""
    except Exception as e:
        print(e)
        return None

def get_extract_alcs_date(text):
    try:
        text_split_colon = str(text).split(":")
        text_split_proper = text_split_colon[0] + ":" + text_split_colon[1]
        return text_split_proper
    except Exception as e:
        print(e)
        return  None

def get_extract_alcs_date_second(text):
    try:
        if re.search(r'12:30', text):
            text_split_colon = str(text).split(":")
            text_split_proper = text_split_colon[0] + ":" + text_split_colon[1]
            return text_split_proper
        else:
            text_split_colon = str(text).split(":")
            text_split_proper = text_split_colon[0]
            return text_split_proper
    except Exception as e:
        print(e)
        return None

def get_extract_alcs_remarks(text):
    try:
        if re.search(r'opp', str(text).lower()):
            return 'OPP'
        elif re.search(r'reim', str(text).lower()):
            return 'REIMB'
        elif re.search(r'sal', str(text).lower()):
            return 'SALARY'
    except Exception as e:
        print(e)
        return  ''

def get_convert_pandas_df_numeric(pandas_df, field_name, source_name):
    try:
        if re.search(r'bank', source_name.lower()):
            # print("Pandas to Numeric Source Name", source_name)
            if re.search(r'axis', source_name.lower()) or re.search(r'icici', source_name.lower()):
                # print("Inside First if")
                if len(pandas_df) > 0:
                    pandas_df[[field_name]] = pandas_df[[field_name]].apply(pd.to_numeric)
                    return pandas_df
                else:
                    print("Length of Pandas Dataframe for Numeric Conversion is equal to Zero!!!")
                    return ""
            elif re.search(r'sbi', source_name.lower()) or re.search(r'hdfc', source_name.lower()):
                # print("Inside Second elif")
                if len(pandas_df) > 0:
                    pandas_df[field_name] = pandas_df[field_name].apply(get_check_numeric)
                    # print("After Check Empty")
                    # print(pandas_df['        Debit'])
                    pandas_df[[field_name]] = pandas_df[[field_name]].apply(pd.to_numeric)
                    return pandas_df
                else:
                    print("Length of Pandas Dataframe for Numeric Conversion is equal to Zero!!!")
                    return ""
            else:
                return ""
        elif re.search(r'alcs', source_name.lower()) and not re.search(r'bank', source_name.lower()):
            # print("Inside First elif")
            if len(pandas_df) > 0:
                pandas_df[[field_name]] = pandas_df[[field_name]].apply(pd.to_numeric)
                return pandas_df
            else:
                print("Length of Pandas Dataframe for Numeric Conversion is equal to Zero!!!")
                return ""
        return ""
    except Exception as e:
        print(e)
        logging.error("Error in Converting Pandas Data frame Series to Numeric!!!", exc_info=True)

def get_update_utr_agg_list(bank_df, aggregate_list, source_2_name):
    try:
        if re.search(r'axis', source_2_name.lower()):
            for payment_date_agg in aggregate_list:
                for i in range(0, len(bank_df)):
                    if (payment_date_agg["issued_amount"] == float(bank_df["Amount (Rs.)"][i])) and bank_df["CR/DR"][i].lower() == "dr":
                        payment_date_agg["utr_number"] = bank_df["utr"][i]
                        payment_date_agg["bank_debit_date"] = bank_df["Tran Date"][i]
                        payment_date_agg["bank_reference_text"] = bank_df["Transaction Particulars"][i]

            return aggregate_list

        elif re.search(r'icici', source_2_name.lower()):
            for payment_date_agg in aggregate_list:
                for i in range(0, len(bank_df)):
                    if (payment_date_agg["issued_amount"] == float(bank_df["Transaction Amount(INR)"][i])) and bank_df["Cr/Dr"][i].lower() == "dr":
                        payment_date_agg["utr_number"] = bank_df["utr"][i]
                        payment_date_agg["bank_debit_date"] = bank_df["Value Date"][i]
                        payment_date_agg["bank_reference_text"] = bank_df["Description"][i]

            return aggregate_list

        elif re.search(r'sbi', source_2_name.lower()):
            for payment_date_agg in aggregate_list:
                for i in range(0, len(bank_df)):
                    if (payment_date_agg["issued_amount"] == float(bank_df["        Debit"][i])):
                        payment_date_agg["utr_number"] = bank_df["utr"][i]
                        payment_date_agg["bank_debit_date"] = bank_df["Value Date"][i]
                        payment_date_agg["bank_reference_text"] = bank_df["Ref No./Cheque No."][i]

            return  aggregate_list

        elif re.search(r'hdfc', source_2_name.lower()):
            for payment_date_agg in aggregate_list:
                for i in range(0, len(bank_df)):
                    if (payment_date_agg["issued_amount"] == float(bank_df["Withdrawal Amt."][i])):
                        payment_date_agg["utr_number"] = bank_df["utr"][i]
                        payment_date_agg["bank_debit_date"] = bank_df["Date"][i]
                        payment_date_agg["bank_reference_text"] = bank_df["Narration"][i]

            return aggregate_list

        return aggregate_list
    except Exception as e:
        print(e)
        logging.error("Error in Updating UTR number in Update UTR Agg List Function!!!", exc_info=True)

def get_upload_number_update(agg_list, letter_number):
    try:

        salary_list = []
        opp_list = []
        reimbursement_list = []

        for agg in agg_list:
            if agg["payment_type"] == "SALARY":
                salary_list.append(agg['payment_date'])
            elif agg["payment_type"] == "OPP":
                opp_list.append(agg['payment_date'])
            elif agg["payment_type"] == "REIMB":
                reimbursement_list.append(agg['payment_date'])

        # SALARY
        for salary_date in salary_list:
            if int(salary_date.split(" ")[-1].split(":")[0]) in [10, 11, 12, 13, 14] and salary_date.split(" ")[-1] != '12:30':
                for agg in agg_list:
                    if agg["payment_date"] == salary_date and len(agg["re_letter_upload_number"]) == 0 and len(agg["utr_number"]) > 0:
                        agg["re_letter_upload_number"] = str(letter_number)
                        letter_number += 1
            elif int(salary_date.split(" ")[-1].split(":")[0]) in [15, 16, 17]:
                for agg in agg_list:
                    if agg["payment_date"] == salary_date and len(agg["re_letter_upload_number"]) == 0 and len(agg["utr_number"]) > 0:
                        agg["re_letter_upload_number"] = str(letter_number)
                        letter_number += 1
            elif int(salary_date.split(" ")[-1].split(":")[0]) in [19, 20, 21, 22, 23, 24]:
                for agg in agg_list:
                    if agg["payment_date"] == salary_date and len(agg["re_letter_upload_number"]) == 0 and len(agg["utr_number"]) > 0:
                        agg["re_letter_upload_number"] = str(letter_number)
                        letter_number += 1

        # REIMBURSEMENT
        for reimb_date in reimbursement_list:
            if int(reimb_date.split(" ")[-1].split(":")[0]) in [12, 13, 14, 15]:
                for agg in agg_list:
                    if agg["payment_date"] == reimb_date and len(agg["re_letter_upload_number"]) == 0 and len(agg["utr_number"]) > 0:
                        agg["re_letter_upload_number"] = str(letter_number)
                        letter_number += 1
            elif int(reimb_date.split(" ")[-1].split(":")[0]) in [16, 17, 18, 19, 20, 21, 22, 23, 24]:
                for agg in agg_list:
                    if agg["payment_date"] == reimb_date and len(agg["re_letter_upload_number"]) == 0 and len(agg["utr_number"]) > 0:
                        agg["re_letter_upload_number"] = str(letter_number)
                        letter_number += 1

        # OPP
        for opp_date in opp_list:
            if int(opp_date.split(" ")[-1].split(":")[0]) in [12, 13, 14, 15]:
                for agg in agg_list:
                    if agg["payment_date"] == opp_date and len(agg["re_letter_upload_number"]) == 0 and len(agg["utr_number"]) > 0:
                        agg["re_letter_upload_number"] = str(letter_number)
                        letter_number += 1
            elif int(opp_date.split(" ")[-1].split(":")[0]) in [16, 17, 18, 19, 20, 21, 22, 23, 24]:
                for agg in agg_list:
                    if agg["payment_date"] == opp_date and len(agg["re_letter_upload_number"]) == 0 and len(agg["utr_number"]) > 0:
                        agg["re_letter_upload_number"] = str(letter_number)
                        letter_number += 1

        # SALARY - 12:30
        for salary_date in salary_list:
            if salary_date.split(" ")[-1][:5] == '12:30':
                for agg in agg_list:
                    if agg["payment_date"] == salary_date and len(agg["re_letter_upload_number"]) == 0 and len(agg["utr_number"]) > 0:
                        agg["re_letter_upload_number"] = str(letter_number)
                        letter_number += 1

        return [agg_list, letter_number]
    except Exception as e:
        print(e)
        logging.error("Error in Getting Upload Number Update!!!", exc_info=True)

def get_upload_number_update_second(agg_list, letter_number):
    try:

        salary_list = []
        opp_list = []
        reimbursement_list = []

        for agg in agg_list:
            if agg["payment_type"] == "SALARY":
                salary_list.append(agg['payment_date'])
            elif agg["payment_type"] == "OPP":
                opp_list.append(agg['payment_date'])
            elif agg["payment_type"] == "REIMB":
                reimbursement_list.append(agg['payment_date'])

        # SALARY
        for salary_date in salary_list:
            if int(salary_date.split(" ")[-1][:2]) in [10, 11, 12, 13, 14]:
                for agg in agg_list:
                    if agg["payment_date"] == salary_date and len(agg["re_letter_upload_number"]) == 0 and len(agg["utr_number"]) > 0:
                        agg["re_letter_upload_number"] = str(letter_number)
                        letter_number += 1
            elif int(salary_date.split(" ")[-1][:2]) in [15, 16, 17]:
                for agg in agg_list:
                    if agg["payment_date"] == salary_date and len(agg["re_letter_upload_number"]) == 0 and len(agg["utr_number"]) > 0:
                        agg["re_letter_upload_number"] = str(letter_number)
                        letter_number += 1
            elif int(salary_date.split(" ")[-1][:2]) in [19, 20, 21, 22, 23, 24]:
                for agg in agg_list:
                    if agg["payment_date"] == salary_date and len(agg["re_letter_upload_number"]) == 0 and len(agg["utr_number"]) > 0:
                        agg["re_letter_upload_number"] = str(letter_number)
                        letter_number += 1

        # REIMBURSEMENT
        for reimb_date in reimbursement_list:
            if int(reimb_date.split(" ")[-1][:2]) in [12, 13, 14, 15]:
                for agg in agg_list:
                    if agg["payment_date"] == reimb_date and len(agg["re_letter_upload_number"]) == 0 and len(agg["utr_number"]) > 0:
                        agg["re_letter_upload_number"] = str(letter_number)
                        letter_number += 1
            elif int(reimb_date.split(" ")[-1][:2]) in [16, 17, 18, 19, 20, 21, 22, 23, 24]:
                for agg in agg_list:
                    if agg["payment_date"] == reimb_date and len(agg["re_letter_upload_number"]) == 0 and len(agg["utr_number"]) > 0:
                        agg["re_letter_upload_number"] = str(letter_number)
                        letter_number += 1

        # OPP
        for opp_date in opp_list:
            if int(opp_date.split(" ")[-1][:2]) in [12, 13, 14, 15]:
                for agg in agg_list:
                    if agg["payment_date"] == opp_date and len(agg["re_letter_upload_number"]) == 0 and len(agg["utr_number"]) > 0:
                        agg["re_letter_upload_number"] = str(letter_number)
                        letter_number += 1
            elif int(opp_date.split(" ")[-1][:2]) in [16, 17, 18, 19, 20, 21, 22, 23, 24]:
                for agg in agg_list:
                    if agg["payment_date"] == opp_date and len(agg["re_letter_upload_number"]) == 0 and len(agg["utr_number"]) > 0:
                        agg["re_letter_upload_number"] = str(letter_number)
                        letter_number += 1

        return [agg_list, letter_number]
    except Exception as e:
        print(e)
        logging.error("Error in Getting Upload Number Second Update!!!", exc_info=True)

def get_upload_update_no_utr(date_list, letter_number):
    try:
        agg_list = list()

        for date in date_list:
            if date.split(":")[-1][2:] == "OPP":
                agg_list.append({
                    "payment_type": "OPP",
                    "payment_date": date,
                    "re_letter_upload_number": ""
                })
            elif date.split(":")[-1][2:] == "REIMB":
                agg_list.append({
                    "payment_type": "REIMB",
                    "payment_date": date,
                    "re_letter_upload_number": ""
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

        # SALARY
        for salary_date in salary_list:
            if int(salary_date.split(" ")[-1].split(":")[0]) in [10, 11, 12, 13, 14] and salary_date.split(" ")[-1] != '12:30':
                for agg in agg_list:
                    if agg["payment_date"] == salary_date and len(agg["re_letter_upload_number"]) == 0:
                        agg["re_letter_upload_number"] = str(letter_number)
                        letter_number += 1
            elif int(salary_date.split(" ")[-1].split(":")[0]) in [15, 16, 17]:
                for agg in agg_list:
                    if agg["payment_date"] == salary_date and len(agg["re_letter_upload_number"]) == 0:
                        agg["re_letter_upload_number"] = str(letter_number)
                        letter_number += 1
            elif int(salary_date.split(" ")[-1].split(":")[0]) in [19, 20, 21, 22, 23, 24]:
                for agg in agg_list:
                    if agg["payment_date"] == salary_date and len(agg["re_letter_upload_number"]) == 0:
                        agg["re_letter_upload_number"] = str(letter_number)
                        letter_number += 1

        # REIMBURSEMENT
        for reimb_date in reimbursement_list:
            if int(reimb_date.split(" ")[-1].split(":")[0]) in [12, 13, 14, 15]:
                for agg in agg_list:
                    if agg["payment_date"] == reimb_date and len(agg["re_letter_upload_number"]) == 0:
                        agg["re_letter_upload_number"] = str(letter_number)
                        letter_number += 1
            elif int(reimb_date.split(" ")[-1].split(":")[0]) in [16, 17, 18, 19, 20, 21, 22, 23, 24]:
                for agg in agg_list:
                    if agg["payment_date"] == reimb_date and len(agg["re_letter_upload_number"]) == 0:
                        agg["re_letter_upload_number"] = str(letter_number)
                        letter_number += 1

        # OPP
        for opp_date in opp_list:
            if int(opp_date.split(" ")[-1].split(":")[0]) in [12, 13, 14, 15]:
                for agg in agg_list:
                    if agg["payment_date"] == opp_date and len(agg["re_letter_upload_number"]) == 0:
                        agg["re_letter_upload_number"] = str(letter_number)
                        letter_number += 1
            elif int(opp_date.split(" ")[-1].split(":")[0]) in [16, 17, 18, 19, 20, 21, 22, 23, 24]:
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

        return agg_list
    except Exception as e:
        print(e)
        logging.error("Error in Getting Upload Number Update No UTR Function!!!", exc_info=True)

def get_field_name(source_name):
    try:
        if len(source_name) > 0:
            if re.search(r'axis', source_name.lower()):
                return "Amount (Rs.)"
            elif re.search(r'icici', source_name.lower()):
                return "Transaction Amount(INR)"
            elif re.search(r'sbi', source_name.lower()):
                return "        Debit"
            elif re.search(r'hdfc', source_name.lower()):
                return "Withdrawal Amt."
        else:
            print("Length of Source Name is equal to Zero!!!")
    except Exception as e:
        print(e)
        logging.error("Error in Getting Field Name!!!", exc_info=True)

def get_check_numeric(text):
    try:
        if len(str(text).replace(" ", "").replace("*", "").replace("RequestingBranchCode:", "")) > 0:
            return str(text).replace(" ", "").replace("*", "").replace("RequestingBranchCode:", "")
        elif len(str(text).replace(" ", "").replace("*", "").replace("RequestingBranchCode:", "")) == 0:
            return str(0)
    except Exception as e:
        return text

def get_add_unique_extraction_alcs(dataframe):
    try:
        if len(dataframe) > 0:
            dataframe["pm_payment_date_unique_proper"] = dataframe['pm_payment_date_proper'].astype(str) + dataframe['payment_type']
            dataframe['pm_payment_date_unique_proper_second'] = dataframe['pm_payment_date_proper_second'].astype(str) + dataframe['payment_type']
            return dataframe
        else:
            print("Length of Dataframe is equal to Zero!!!")
    except Exception as e:
        print(e)
        logging.error("Error in Add unique extraction ALCS Function!!!", exc_info=True)

def get_extract_payment_type(text):
    try:
        if len(text) > 0:
            return text.split(":")[1][2:]
        else:
            return ""
    except Exception as e:
        print(e)
        logging.error("Error in Get Extract Payment Type Function!!!", exc_info=True)

def get_extract_payment_type_second(text):
    try:
        if len(text) > 0:
            return text.split(" ")[1][2:]
        else:
            return ""
    except Exception as e:
        print(e)
        logging.error("Error in Get Extract Payment Type Second Function!!!", exc_info=True)

def get_client_details(client_properties, client_name):
    try:
        if client_properties:
            client_url = client_properties["url"].split("=")
            client_url[-1] = client_name
            client_properties_updated = "=".join(client_url)
            client_properties["url"] = client_properties_updated
            client_details_response = dr.GetResponse(client_properties)
            response_data = client_details_response.get_response_data()
            return response_data
        else:
            return ""
    except Exception as e:
        print(e)
        logging.error("Error in Getting Client Details Properties!!!", exc_info=True)

def get_client_letter_number(group_list):
    try:
        if len(group_list) > 0:

            letter_number = 0
            others_letters_numbers_list = list()
            for group in group_list:
                if group["client_name"] == "OTHERS":
                    others_letters_numbers_list.append(int(group["re_letter_generated_number"]))

            others_letters_numbers_list.sort()
            for i in range(0, len(others_letters_numbers_list)):
                for group in group_list:
                    if int(group["re_letter_generated_number"]) == others_letters_numbers_list[i]:
                        group["re_letter_generated_number_one"] = str(i + 1)
                        letter_number = i + 1

            samsung_letters_numbers_list = list()
            for group in group_list:
                if group["client_name"] == "SAMSUNG":
                    samsung_letters_numbers_list.append(int(group['re_letter_generated_number']))

            samsung_letters_numbers_list.sort()
            for i in range(0, len(samsung_letters_numbers_list)):
                for group in group_list:
                    if int(group["re_letter_generated_number"]) == samsung_letters_numbers_list[i]:
                        group["re_letter_generated_number_one"] = str(letter_number + 1)
                        letter_number = letter_number + 1

            bajaj_letters_numbers_list = list()
            for group in group_list:
                if group["client_name"] == "BAJAJ":
                    bajaj_letters_numbers_list.append(int(group['re_letter_generated_number']))

            bajaj_letters_numbers_list.sort()
            for i in range(0, len(bajaj_letters_numbers_list)):
                for group in group_list:
                    if int(group['re_letter_generated_number']) == bajaj_letters_numbers_list[i]:
                        group["re_letter_generated_number_one"] = str(letter_number + 1)

            return group_list

        else:
            print("Length of Group List is equal to Zero!!!")
            return []
    except Exception as e:
        print(e)
        logging.error("Error in Getting Client Letter Number!!!", exc_info=True)
import logging
import etl_functions as ef
import data_request as dr

def get_process_sources(
        spark, sqlContext, alcs_spark_df, bank_spark_df, action_code_list, source_1_columns, source_2_columns,
        validate_attribute_1_row_list, validate_attribute_2_row_list, date_transform_attribute_1_row_list,
        date_transform_attribute_2_row_list, source_1_name, source_2_name, date_config_folder, date_config_file,
        aggregator_details_1_properties, aggregator_details_2_properties, field_extraction_properties,
        transformation_operators_list, source_definition_properties):
    try:
        validated_pandas_df_alcs = ''
        validated_pandas_df_bank = ''
        date_transformed_pandas_df_alcs = ''
        date_transformed_pandas_df_bank = ''

        # Transformation Operators
        delimiter_operators = get_field_extraction_delimiter_operators(transformation_operators_list)
        print("delimiter_operators", delimiter_operators)
        print("delimiter_operators Type", type(delimiter_operators))
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
                        print(date_transformed_pandas_df_alcs.head(10))
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
                        print(date_transformed_pandas_df_bank.head(10))
                    else:
                        print("Length of Validated BANK Dataframe is equal to Zero!!!")
                        break

                elif action_code == "A02_FEX_BANK":
                    if len(date_transformed_pandas_df_bank) > 0:
                        date_transformed_spark_df = spark.createDataFrame(date_transformed_pandas_df_bank)
                        date_transformed_spark_df.createOrReplaceTempView("date_transformed_bank_spark_df")

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

                        for field_extraction in field_extraction_list_bank:
                            if field_extraction["pattern_type"] == "separator":
                                pattern_input = field_extraction["pattern_input"]
                                character = ''
                                for delimiter_operator in delimiter_operators:
                                    for k,v in delimiter_operator.items():
                                        print("k", k)
                                        if k == pattern_input.split("-")[0]:
                                            character = v

                                extract_position = pattern_input.split("-")[-1]
                                transaction_reference = field_extraction["transaction_reference"]
                                transaction_placed = field_extraction["transaction_placed"]
                                if transaction_placed == "inbetween":
                                    condition = "%" + transaction_reference + "%"
                                m_source_definitions_id = field_extraction["m_source_definitions_id"]
                                reference_field_name = get_source_definition_attribute_name(source_definition_list_bank, m_source_definitions_id)

                                sql_query = "select *, substring_index(\"{reference_field}\", \"{character}\", {extract_position}) from date_transformed_bank_df where \"{reference_field}\" like '{conditions}';"
                                sql_query_proper = sql_query.replace('{reference_field}', reference_field_name).replace('{conditions}', condition).replace("{character}", character).replace("{extract_position}", str(extract_position))
                                print("SQL Query Proper", sql_query_proper)

                                sql_check = sqlContext.sql("select * from date_transformed_bank_spark_df")
                                print(sql_check.show())

                    else:
                        print("Length of Date Transformed Bank Dataframe is equal to Zero!!!")
                        break
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
                print(transformation_operators)
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
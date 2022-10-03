CREATE DEFINER=`root`@`localhost` PROCEDURE `p_transfer_records`(vTenantsId INT, vGroupsId INT, vEntitiesId INT, vMProcessingLayerId INT,  VMProcessingSubLayerId INT , vProcessingLayerId INT, vUserId INT, vFileId INT, vTargetId INT, vMsourcesId INT, vMsourceName TEXT, vGSTRemittanceMonth TEXT, vUniqueField TEXT, vPrimaryDateField TEXT, vStatusField TEXT, OUT vReturn INT)
BEGIN

DECLARE vProcessingStatus VARCHAR(16)  ;
DECLARE iRowCount INT;
DECLARE vUpdateToken DATETIME;
DECLARE iDupCheckRowCount INT;
DECLARE iNewCheckRowCount INT;
DECLARE iStartDateCount INT;
DECLARE iEndDateCount INT;

DECLARE EXIT  HANDLER FOR SQLEXCEPTION
BEGIN

	SET SQL_SAFE_UPDATES = 0;
	SET vReturn = -1;
    SET vupdateToken = now();
	
    GET STACKED DIAGNOSTICS CONDITION 1  @code = RETURNED_SQLSTATE, @errno = MYSQL_ERRNO, @error_string = MESSAGE_TEXT;
    
    ROLLBACK;
    
	INSERT INTO `execution_logs`
	(`tenants_id`,`groups_id`,`entities_id`, `m_processing_layer_id`, `m_processing_sub_layer_id`, `processing_layer_id`, `file_id`, `targets_id`, `m_sources_id`, `gst_remittance_month`, `start_date`, `end_date`, `status`, `duration`, `comments`)   
	SELECT vTenantsId, vGroupsId, vEntitiesId, vMProcessingLayerId, VMProcessingSubLayerId, vProcessingLayerId, vFileId, vTargetId, vMsourcesId, vGSTRemittanceMonth, now(), now(), 'ERROR' , 0 ,  concat('p_transfer_records: ', @errno , ' ->Error:' , @error_string);
    
    -- Updating the Staging Table Records to Error
	update stg_report_storage
		set processing_status = 'Error', 
			modified_date = vupdateToken
	where processing_status = 'New' 
        and tenants_id = vTenantsId
        and groups_id = vGroupsId
        and entities_id = vEntitiesId
        and m_processing_layer_id = vMProcessingLayerId
        and m_processing_sub_layer_id = VMProcessingSubLayerId
        and processing_layer_id = vProcessingLayerId
        and file_id = vFileId
        and target_id = vTargetId
        and m_sources_id = vMsourcesId
        and gst_remittance_month = vGSTRemittanceMonth
        and is_active = 1;
        
	-- Updating the File Uplaods Table Record to Error
    update file_uploads
		set status = 'ERROR',
			comments = 'Error in Loading the File!!!',
            is_processed = 1,
            modified_date = vupdateToken
		where tenants_id = vTenantsId
            and groups_id = vGroupsId
            and entities_id = vEntitiesId
            and m_processing_layer_id = vMProcessingLayerId
            and m_processing_sub_layer_id = VMProcessingSubLayerId
            and processing_layer_id = vProcessingLayerId
            and id = vFileId
            and m_sources_id = vMsourcesId
            and gst_month = vGSTRemittanceMonth
            and is_active = 1;
    
    RESIGNAL;

END;

START TRANSACTION;

	SET vReturn = -1;
	SET vProcessingStatus = 'New';
	SET vUpdateToken = now();
	SET SQL_SAFE_UPDATES = 0;
    
    -- Taking Starting Date of the Record from Staging
    drop temporary table if exists tmp_start_date;
    create temporary table tmp_start_date
    select vPrimaryDateField as start_date from consolidation_files.stg_report_storage
		where processing_status = vProcessingStatus
            and tenants_id = vTenantsId
            and groups_id = vGroupsId
            and entities_id = vEntitiesId
            and m_processing_layer_id = vMProcessingLayerId
            and m_processing_sub_layer_id = VMProcessingSubLayerId
            and processing_layer_id = vProcessingLayerId
            and file_id = vFileId
            and target_id = vTargetId
            and m_sources_id = vMsourcesId
            and substring_index(vPrimaryDateField, '-', 2) = vGSTRemittanceMonth
            and length(vPrimaryDateField) > 1
            and length(vUniqueField) > 1
            and ( lower(vStatusField) = 'approved' OR lower(vStatusField) = 'a' )
            and is_active = 1
            order by vPrimaryDateField asc limit 1;
        
	set iStartDateCount = row_count();

    -- Taking End Date of the Record from Staging
    drop temporary table if exists tmp_end_date;
    create temporary table tmp_end_date
    select vPrimaryDateField as end_date from consolidation_files.stg_report_storage
        where processing_status = vProcessingStatus
            and tenants_id = vTenantsId
            and groups_id = vGroupsId
            and entities_id = vEntitiesId
            and m_processing_layer_id = vMProcessingLayerId
            and m_processing_sub_layer_id = VMProcessingSubLayerId
            and processing_layer_id = vProcessingLayerId
            and file_id = vFileId
            and target_id = vTargetId
            and m_sources_id = vMsourcesId
            and substring_index(vPrimaryDateField, '-', 2) = vGSTRemittanceMonth
            and length(vPrimaryDateField) > 1
            and length(vUniqueField) > 1
            and ( lower(vStatusField) = 'approved' OR lower(vStatusField) = 'a' )
            and is_active = 1
            order by vPrimaryDateField desc limit 1;

    set iEndDateCount = row_count();

    if (iStartDateCount == 0 and iEndDateCount == 0) then

        -- Updating the Status to Processed for Staging Records
        update consolidation_files.stg_report_storage
            set processing_status = 'Processed',
                modified_date = vupdateToken
            where processing_status = vProcessingStatus
                and tenants_id = vTenantsId
                and groups_id = vGroupsId
                and entities_id = vEntitiesId
                and m_processing_layer_id = vMProcessingLayerId
                and m_processing_sub_layer_id = VMProcessingSubLayerId
                and processing_layer_id = vProcessingLayerId
                and file_id = vFileId
                and target_id = vTargetId
                and m_sources_id = vMsourcesId
                and is_active = 1;

        -- Updating Status to Processed for File Uploads
        update consolidation_files.file_uploads
            set status = 'COMPLETED',
                comments = 'Data Not Found in File for GST Remittance Month Chosen!!!',
                is_processed = 1,
                modified_date = vupdateToken
            where processing_status = vProcessingStatus
                and tenants_id = vTenantsId
                and groups_id = vGroupsId
                and entities_id = vEntitiesId
                and m_processing_layer_id = vMProcessingLayerId
                and m_processing_sub_layer_id = VMProcessingSubLayerId
                and processing_layer_id = vProcessingLayerId
                and id = vFileId
                and m_sources_id = vMsourcesId
                and is_active = 1;

    end if;

    -- Checking Source Relations and Get an Id of Mapped Sources
    drop temporary table if exists tmp_source_relations;
    create temporary table tmp_source_relations
    select m_source_relation_id from consolidation_files.source_relations 
        where tenants_id = vTenantsId
            and groups_id = vGroupsId
            and entities_id = vEntitiesId
            and m_processing_layer_id = vMProcessingLayerId
            and m_processing_sub_layer_id = VMProcessingSubLayerId
            and processing_layer_id = vProcessingLayerId
            and m_sources_id = vMsourcesId
            and is_active = 1;

    -- Creating New Records From Staging Table
    drop temporary table if exists tmp_stg;
    create temporary table tmp_stg
    select id as s_pk, vUniqueField as stg_unique_field from consolidation_files.stg_report_storage
        where processing_status = vProcessingStatus
            and tenants_id = vTenantsId
            and groups_id = vGroupsId
            and entities_id = vEntitiesId
            and m_processing_layer_id = vMProcessingLayerId
            and m_processing_sub_layer_id = VMProcessingSubLayerId
            and processing_layer_id = vProcessingLayerId
            and file_id = vFileId
            and target_id = vTargetId
            and m_sources_id = vMsourcesId
            and substring_index(vPrimaryDateField, '-', 2) = vGSTRemittanceMonth
            and length(vPrimaryDateField) > 1
            and length(vUniqueField) > 1
            and ( lower(vStatusField) = 'approved' OR lower(vStatusField) = 'a' )
            and is_active = 1;

    -- Creating Existing Records From Transaction Table
    drop temporary table if exists tmp_main;
    create temporary table tmp_main
    select id, vTUniqueField as main_unique_field from consolidation_files.t_report_storage
        where tenants_id = vTenantsId
            and groups_id = vGroupsId
            and entities_id = vEntitiesId
            and m_processing_layer_id = vMProcessingLayerId
            and m_processing_sub_layer_id = VMProcessingSubLayerId
            and processing_layer_id = vProcessingLayerId
            and m_sources_id in
                (
                    select m_source_relation_id from tmp_source_relations
                );
    
    -- Creating Duplicate Records 
    drop temporary table if exists tmp_old_check;
    create temporary table tmp_old_check
    select s_pk from tmp_stg
    where stg_unique_field in
    (
        select main_unique_field from tmp_main
    );

    set iDupCheckRowCount = row_count();

    -- Creating id of New Records
    if (iDupCheckRowCount <= 0) then

        drop temporary table if exists tmp_new;
        create temporary table tmp_new
        select id as s_pk from consolidation_files.stg_report_storage
            where processing_status = 'New'
            and gst_remittance_month = vGSTRemittanceMonth
            and tenants_id = vTenantsId
            and entities_id = vEntitiesId
            and m_processing_layer_id = vMProcessingLayerId
            and m_processing_sub_layer_id = VMProcessingSubLayerId
            and processing_layer_id = vProcessingLayerId
            and m_sources_id = vMsourcesId
            and file_id = vFileId
            and length(vPrimaryDateField) > 1
            and length(vUniqueField) > 1
            and ( lower(vStatusField) = 'approved' or lower(vStatusField) = 'a' );

        set iNewCheckRowCount = row_count();

    elseif (iDupCheckRowCount > 0) then

        -- creating all records of New
        drop temporary table if exists tmp_all_records;
        create temporary table tmp_all_records
        select id from consolidation_files.stg_report_storage
            where processing_status = 'New'
            and gst_remittance_month = vGSTRemittanceMonth
            and tenants_id = vTenantsId
            and entities_id = vEntitiesId
            and m_processing_layer_id = vMProcessingLayerId
            and m_processing_sub_layer_id = VMProcessingSubLayerId
            and processing_layer_id = vProcessingLayerId
            and m_sources_id = vMsourcesId
            and file_id = vFileId
            and length(vPrimaryDateField) > 1
            and length(vUniqueField) > 1
            and ( lower(vStatusField) = 'approved' or lower(vStatusField) = 'a' );

        set iNewCheckRowCount = row_count();

        -- Taking the id of New Records from the Staging Table by subtracting the above two tables
        drop temporary table if exists temp_new;
        create temporary table temp_new
        select id as s_pk from tmp_all_records where id not in
        (
            select s_pk from tmp_old_check
        );

        set iNewCheckRowCount = row_count();

    end if;

    -- For New Records
    if (iNewCheckRowCount) > 0 then

        insert into consolidation_files.t_report_storage
        (
            tenants_id, groups_id, entities_id, m_processing_layer_id, m_processing_sub_layer_id, processing_layer_id, file_id, m_sources_id, m_source_name, target_id, target_name, gst_remittance_month,
            processing_status, reference_text_1, reference_text_2, reference_text_3, reference_text_4, reference_text_5, reference_text_6, reference_text_7, reference_text_8, reference_text_9, reference_text_10,
            reference_text_11, reference_text_12, reference_text_13, reference_text_14, reference_text_15, reference_text_16, reference_text_17, reference_text_18, reference_text_19, reference_text_20,
            reference_text_21, reference_text_22, reference_text_23, reference_text_24, reference_text_25, reference_text_26, reference_text_27, reference_text_28, reference_text_29, reference_text_30,
            reference_text_31, reference_text_32, reference_text_33, reference_text_34, reference_text_35, reference_text_36, reference_text_37, reference_text_38, reference_text_39, reference_text_40,
            reference_text_41, reference_text_42, reference_text_43, reference_text_44, reference_text_45, reference_text_46, reference_text_47, reference_text_48, reference_text_49, reference_text_50,
            reference_text_51, reference_text_52, reference_text_53, reference_text_54, reference_text_55, reference_text_56, reference_text_57, reference_text_58, reference_text_59, reference_text_60,
            reference_text_61, reference_text_62, reference_text_63, reference_text_64, reference_text_65, reference_text_66, reference_text_67, reference_text_68, reference_text_69, reference_text_70,
            reference_text_71, reference_text_72, reference_text_73, reference_text_74, reference_text_75, reference_text_76, reference_text_77, reference_text_78, reference_text_79, reference_text_80,
            reference_text_81, reference_text_82, reference_text_83, reference_text_84, reference_text_85, reference_text_86, reference_text_87, reference_text_88, reference_text_89, reference_text_90,
            reference_text_91, reference_text_92, reference_text_93, reference_text_94, reference_text_95, reference_text_96, reference_text_97, reference_text_98, reference_text_99, reference_text_100,
            reference_int_1, reference_int_2, reference_int_3, reference_int_4, reference_int_5, reference_int_6, reference_int_7, reference_int_8, reference_int_9, reference_int_10,
            reference_int_11, reference_int_12, reference_int_13, reference_int_14, reference_int_15, reference_int_16, reference_int_17, reference_int_18, reference_int_19, reference_int_20,
            reference_int_21, reference_int_22, reference_int_23, reference_int_24, reference_int_25, reference_int_26, reference_int_27, reference_int_28, reference_int_29, reference_int_30,
            reference_int_31, reference_int_32, reference_int_33, reference_int_34, reference_int_35, reference_int_36, reference_int_37, reference_int_38, reference_int_39, reference_int_40,
            reference_int_41, reference_int_42, reference_int_43, reference_int_44, reference_int_45, reference_int_46, reference_int_47, reference_int_48, reference_int_49, reference_int_50,
            reference_int_51, reference_int_52, reference_int_53, reference_int_54, reference_int_55, reference_int_56, reference_int_57, reference_int_58, reference_int_59, reference_int_60,
            reference_int_61, reference_int_62, reference_int_63, reference_int_64, reference_int_65, reference_int_66, reference_int_67, reference_int_68, reference_int_69, reference_int_70,
            reference_dec_1, reference_dec_2, reference_dec_3, reference_dec_4, reference_dec_5, reference_dec_6, reference_dec_7, reference_dec_8, reference_dec_9, reference_dec_10,
            reference_dec_11, reference_dec_12, reference_dec_13, reference_dec_14, reference_dec_15, reference_dec_16, reference_dec_17, reference_dec_18, reference_dec_19, reference_dec_20,
            reference_dec_21, reference_dec_22, reference_dec_23, reference_dec_24, reference_dec_25, reference_dec_26, reference_dec_27, reference_dec_28, reference_dec_29, reference_dec_30,
            reference_dec_31, reference_dec_32, reference_dec_33, reference_dec_34, reference_dec_35, reference_dec_36, reference_dec_37, reference_dec_38, reference_dec_39, reference_dec_40,
            reference_dec_41, reference_dec_42, reference_dec_43, reference_dec_44, reference_dec_45, reference_dec_46, reference_dec_47, reference_dec_48, reference_dec_49, reference_dec_50,
            reference_dec_51, reference_dec_52, reference_dec_53, reference_dec_54, reference_dec_55, reference_dec_56, reference_dec_57, reference_dec_58, reference_dec_59, reference_dec_60,
            reference_dec_61, reference_dec_62, reference_dec_63, reference_dec_64, reference_dec_65, reference_dec_66, reference_dec_67, reference_dec_68, reference_dec_69, reference_dec_70,
            reference_date_1, reference_date_2, reference_date_3, reference_date_4, reference_date_5, reference_date_6, reference_date_7, reference_date_8, reference_date_9, reference_date_10,
            reference_date_11, reference_date_12, reference_date_13, reference_date_14, reference_date_15, reference_date_16, reference_date_17, reference_date_18, reference_date_19, reference_date_20,
            reference_date_21, reference_date_22, reference_date_23, reference_date_24, reference_date_25, reference_date_26, reference_date_27, reference_date_28, reference_date_29, reference_date_30,
            is_invoice, is_invoice, created_by, created_date, modified_by, modified_date
        )
        select tenants_id, groups_id, entities_id, m_processing_layer_id, m_processing_sub_layer_id, processing_layer_id, file_id, m_source_id, m_source_name, target_id, target_name, gst_remittance_month, 'New',
        	case when reference_text_1 is null then
                    null
                when length(replace(reference_text_1, ' ', '')) > 0 then
                    replace(replace(reference_text_1, '/#/', ''''), '/##/', '\\ ')
            end reference_text_1,
        	case when reference_text_2 is null then
                    null
                when length(replace(reference_text_2, ' ', '')) > 0 then
                    replace(replace(reference_text_2, '/#/', ''''), '/##/', '\\ ')
            end reference_text_2,
        	case when reference_text_3 is null then
                    null
                when length(replace(reference_text_3, ' ', '')) > 0 then
                    replace(replace(reference_text_3, '/#/', ''''), '/##/', '\\ ')
            end reference_text_3,
        	case when reference_text_4 is null then
                    null
                when length(replace(reference_text_4, ' ', '')) > 0 then
                    replace(replace(reference_text_4, '/#/', ''''), '/##/', '\\ ')
            end reference_text_4,
        	case when reference_text_5 is null then
                    null
                when length(replace(reference_text_5, ' ', '')) > 0 then
                    replace(replace(reference_text_5, '/#/', ''''), '/##/', '\\ ')
            end reference_text_5,
        	case when reference_text_6 is null then
                    null
                when length(replace(reference_text_6, ' ', '')) > 0 then
                    replace(replace(reference_text_6, '/#/', ''''), '/##/', '\\ ')
            end reference_text_6,
        	case when reference_text_7 is null then
                    null
                when length(replace(reference_text_7, ' ', '')) > 0 then
                    replace(replace(reference_text_7, '/#/', ''''), '/##/', '\\ ')
            end reference_text_7,
        	case when reference_text_8 is null then
                    null
                when length(replace(reference_text_8, ' ', '')) > 0 then
                    replace(replace(reference_text_8, '/#/', ''''), '/##/', '\\ ')
            end reference_text_8,
        	case when reference_text_9 is null then
                    null
                when length(replace(reference_text_9, ' ', '')) > 0 then
                    replace(replace(reference_text_9, '/#/', ''''), '/##/', '\\ ')
            end reference_text_9,
        	case when reference_text_10 is null then
                    null
                when length(replace(reference_text_10, ' ', '')) > 0 then
                    replace(replace(reference_text_10, '/#/', ''''), '/##/', '\\ ')
            end reference_text_10,
        	case when reference_text_11 is null then
                    null
                when length(replace(reference_text_11, ' ', '')) > 0 then
                    replace(replace(reference_text_11, '/#/', ''''), '/##/', '\\ ')
            end reference_text_11,
        	case when reference_text_12 is null then
                    null
                when length(replace(reference_text_12, ' ', '')) > 0 then
                    replace(replace(reference_text_12, '/#/', ''''), '/##/', '\\ ')
            end reference_text_12,
        	case when reference_text_13 is null then
                    null
                when length(replace(reference_text_13, ' ', '')) > 0 then
                    replace(replace(reference_text_13, '/#/', ''''), '/##/', '\\ ')
            end reference_text_13,
        	case when reference_text_14 is null then
                    null
                when length(replace(reference_text_14, ' ', '')) > 0 then
                    replace(replace(reference_text_14, '/#/', ''''), '/##/', '\\ ')
            end reference_text_14,
        	case when reference_text_15 is null then
                    null
                when length(replace(reference_text_15, ' ', '')) > 0 then
                    replace(replace(reference_text_15, '/#/', ''''), '/##/', '\\ ')
            end reference_text_15,
        	case when reference_text_16 is null then
                    null
                when length(replace(reference_text_16, ' ', '')) > 0 then
                    replace(replace(reference_text_16, '/#/', ''''), '/##/', '\\ ')
            end reference_text_16,
        	case when reference_text_17 is null then
                    null
                when length(replace(reference_text_17, ' ', '')) > 0 then
                    replace(replace(reference_text_17, '/#/', ''''), '/##/', '\\ ')
            end reference_text_17,
        	case when reference_text_18 is null then
                    null
                when length(replace(reference_text_18, ' ', '')) > 0 then
                    replace(replace(reference_text_18, '/#/', ''''), '/##/', '\\ ')
            end reference_text_18,
        	case when reference_text_19 is null then
                    null
                when length(replace(reference_text_19, ' ', '')) > 0 then
                    replace(replace(reference_text_19, '/#/', ''''), '/##/', '\\ ')
            end reference_text_19,
        	case when reference_text_20 is null then
                    null
                when length(replace(reference_text_20, ' ', '')) > 0 then
                    replace(replace(reference_text_20, '/#/', ''''), '/##/', '\\ ')
            end reference_text_20,
        	case when reference_text_21 is null then
                    null
                when length(replace(reference_text_21, ' ', '')) > 0 then
                    replace(replace(reference_text_21, '/#/', ''''), '/##/', '\\ ')
            end reference_text_21,
        	case when reference_text_22 is null then
                    null
                when length(replace(reference_text_22, ' ', '')) > 0 then
                    replace(replace(reference_text_22, '/#/', ''''), '/##/', '\\ ')
            end reference_text_22,
        	case when reference_text_23 is null then
                    null
                when length(replace(reference_text_23, ' ', '')) > 0 then
                    replace(replace(reference_text_23, '/#/', ''''), '/##/', '\\ ')
            end reference_text_23,
        	case when reference_text_24 is null then
                    null
                when length(replace(reference_text_24, ' ', '')) > 0 then
                    replace(replace(reference_text_24, '/#/', ''''), '/##/', '\\ ')
            end reference_text_24,
        	case when reference_text_25 is null then
                    null
                when length(replace(reference_text_25, ' ', '')) > 0 then
                    replace(replace(reference_text_25, '/#/', ''''), '/##/', '\\ ')
            end reference_text_25,
        	case when reference_text_26 is null then
                    null
                when length(replace(reference_text_26, ' ', '')) > 0 then
                    replace(replace(reference_text_26, '/#/', ''''), '/##/', '\\ ')
            end reference_text_26,
        	case when reference_text_27 is null then
                    null
                when length(replace(reference_text_27, ' ', '')) > 0 then
                    replace(replace(reference_text_27, '/#/', ''''), '/##/', '\\ ')
            end reference_text_27,
        	case when reference_text_28 is null then
                    null
                when length(replace(reference_text_28, ' ', '')) > 0 then
                    replace(replace(reference_text_28, '/#/', ''''), '/##/', '\\ ')
            end reference_text_28,
        	case when reference_text_29 is null then
                    null
                when length(replace(reference_text_29, ' ', '')) > 0 then
                    replace(replace(reference_text_29, '/#/', ''''), '/##/', '\\ ')
            end reference_text_29,
        	case when reference_text_30 is null then
                    null
                when length(replace(reference_text_30, ' ', '')) > 0 then
                    replace(replace(reference_text_30, '/#/', ''''), '/##/', '\\ ')
            end reference_text_30,
        	case when reference_text_31 is null then
                    null
                when length(replace(reference_text_31, ' ', '')) > 0 then
                    replace(replace(reference_text_31, '/#/', ''''), '/##/', '\\ ')
            end reference_text_31,
        	case when reference_text_32 is null then
                    null
                when length(replace(reference_text_32, ' ', '')) > 0 then
                    replace(replace(reference_text_32, '/#/', ''''), '/##/', '\\ ')
            end reference_text_32,
        	case when reference_text_33 is null then
                    null
                when length(replace(reference_text_33, ' ', '')) > 0 then
                    replace(replace(reference_text_33, '/#/', ''''), '/##/', '\\ ')
            end reference_text_33,
        	case when reference_text_34 is null then
                    null
                when length(replace(reference_text_34, ' ', '')) > 0 then
                    replace(replace(reference_text_34, '/#/', ''''), '/##/', '\\ ')
            end reference_text_34,
        	case when reference_text_35 is null then
                    null
                when length(replace(reference_text_35, ' ', '')) > 0 then
                    replace(replace(reference_text_35, '/#/', ''''), '/##/', '\\ ')
            end reference_text_35,
        	case when reference_text_36 is null then
                    null
                when length(replace(reference_text_36, ' ', '')) > 0 then
                    replace(replace(reference_text_36, '/#/', ''''), '/##/', '\\ ')
            end reference_text_36,
        	case when reference_text_37 is null then
                    null
                when length(replace(reference_text_37, ' ', '')) > 0 then
                    replace(replace(reference_text_37, '/#/', ''''), '/##/', '\\ ')
            end reference_text_37,
        	case when reference_text_38 is null then
                    null
                when length(replace(reference_text_38, ' ', '')) > 0 then
                    replace(replace(reference_text_38, '/#/', ''''), '/##/', '\\ ')
            end reference_text_38,
        	case when reference_text_39 is null then
                    null
                when length(replace(reference_text_39, ' ', '')) > 0 then
                    replace(replace(reference_text_39, '/#/', ''''), '/##/', '\\ ')
            end reference_text_39,
        	case when reference_text_40 is null then
                    null
                when length(replace(reference_text_40, ' ', '')) > 0 then
                    replace(replace(reference_text_40, '/#/', ''''), '/##/', '\\ ')
            end reference_text_40,
        	case when reference_text_41 is null then
                    null
                when length(replace(reference_text_41, ' ', '')) > 0 then
                    replace(replace(reference_text_41, '/#/', ''''), '/##/', '\\ ')
            end reference_text_41,
        	case when reference_text_42 is null then
                    null
                when length(replace(reference_text_42, ' ', '')) > 0 then
                    replace(replace(reference_text_42, '/#/', ''''), '/##/', '\\ ')
            end reference_text_42,
        	case when reference_text_43 is null then
                    null
                when length(replace(reference_text_43, ' ', '')) > 0 then
                    replace(replace(reference_text_43, '/#/', ''''), '/##/', '\\ ')
            end reference_text_43,
        	case when reference_text_44 is null then
                    null
                when length(replace(reference_text_44, ' ', '')) > 0 then
                    replace(replace(reference_text_44, '/#/', ''''), '/##/', '\\ ')
            end reference_text_44,
        	case when reference_text_45 is null then
                    null
                when length(replace(reference_text_45, ' ', '')) > 0 then
                    replace(replace(reference_text_45, '/#/', ''''), '/##/', '\\ ')
            end reference_text_45,
        	case when reference_text_46 is null then
                    null
                when length(replace(reference_text_46, ' ', '')) > 0 then
                    replace(replace(reference_text_46, '/#/', ''''), '/##/', '\\ ')
            end reference_text_46,
        	case when reference_text_47 is null then
                    null
                when length(replace(reference_text_47, ' ', '')) > 0 then
                    replace(replace(reference_text_47, '/#/', ''''), '/##/', '\\ ')
            end reference_text_47,
        	case when reference_text_48 is null then
                    null
                when length(replace(reference_text_48, ' ', '')) > 0 then
                    replace(replace(reference_text_48, '/#/', ''''), '/##/', '\\ ')
            end reference_text_48,
        	case when reference_text_49 is null then
                    null
                when length(replace(reference_text_49, ' ', '')) > 0 then
                    replace(replace(reference_text_49, '/#/', ''''), '/##/', '\\ ')
            end reference_text_49,
        	case when reference_text_50 is null then
                    null
                when length(replace(reference_text_50, ' ', '')) > 0 then
                    replace(replace(reference_text_50, '/#/', ''''), '/##/', '\\ ')
            end reference_text_50,
        	case when reference_text_51 is null then
                    null
                when length(replace(reference_text_51, ' ', '')) > 0 then
                    replace(replace(reference_text_51, '/#/', ''''), '/##/', '\\ ')
            end reference_text_51,
        	case when reference_text_52 is null then
                    null
                when length(replace(reference_text_52, ' ', '')) > 0 then
                    replace(replace(reference_text_52, '/#/', ''''), '/##/', '\\ ')
            end reference_text_52,
        	case when reference_text_53 is null then
                    null
                when length(replace(reference_text_53, ' ', '')) > 0 then
                    replace(replace(reference_text_53, '/#/', ''''), '/##/', '\\ ')
            end reference_text_53,
        	case when reference_text_54 is null then
                    null
                when length(replace(reference_text_54, ' ', '')) > 0 then
                    replace(replace(reference_text_54, '/#/', ''''), '/##/', '\\ ')
            end reference_text_54,
        	case when reference_text_55 is null then
                    null
                when length(replace(reference_text_55, ' ', '')) > 0 then
                    replace(replace(reference_text_55, '/#/', ''''), '/##/', '\\ ')
            end reference_text_55,
        	case when reference_text_56 is null then
                    null
                when length(replace(reference_text_56, ' ', '')) > 0 then
                    replace(replace(reference_text_56, '/#/', ''''), '/##/', '\\ ')
            end reference_text_56,
        	case when reference_text_57 is null then
                    null
                when length(replace(reference_text_57, ' ', '')) > 0 then
                    replace(replace(reference_text_57, '/#/', ''''), '/##/', '\\ ')
            end reference_text_57,
        	case when reference_text_58 is null then
                    null
                when length(replace(reference_text_58, ' ', '')) > 0 then
                    replace(replace(reference_text_58, '/#/', ''''), '/##/', '\\ ')
            end reference_text_58,
        	case when reference_text_59 is null then
                    null
                when length(replace(reference_text_59, ' ', '')) > 0 then
                    replace(replace(reference_text_59, '/#/', ''''), '/##/', '\\ ')
            end reference_text_59,
        	case when reference_text_60 is null then
                    null
                when length(replace(reference_text_60, ' ', '')) > 0 then
                    replace(replace(reference_text_60, '/#/', ''''), '/##/', '\\ ')
            end reference_text_60,
        	case when reference_text_61 is null then
                    null
                when length(replace(reference_text_61, ' ', '')) > 0 then
                    replace(replace(reference_text_61, '/#/', ''''), '/##/', '\\ ')
            end reference_text_61,
        	case when reference_text_62 is null then
                    null
                when length(replace(reference_text_62, ' ', '')) > 0 then
                    replace(replace(reference_text_62, '/#/', ''''), '/##/', '\\ ')
            end reference_text_62,
        	case when reference_text_63 is null then
                    null
                when length(replace(reference_text_63, ' ', '')) > 0 then
                    replace(replace(reference_text_63, '/#/', ''''), '/##/', '\\ ')
            end reference_text_63,
        	case when reference_text_64 is null then
                    null
                when length(replace(reference_text_64, ' ', '')) > 0 then
                    replace(replace(reference_text_64, '/#/', ''''), '/##/', '\\ ')
            end reference_text_64,
        	case when reference_text_65 is null then
                    null
                when length(replace(reference_text_65, ' ', '')) > 0 then
                    replace(replace(reference_text_65, '/#/', ''''), '/##/', '\\ ')
            end reference_text_65,
        	case when reference_text_66 is null then
                    null
                when length(replace(reference_text_66, ' ', '')) > 0 then
                    replace(replace(reference_text_66, '/#/', ''''), '/##/', '\\ ')
            end reference_text_66,
        	case when reference_text_67 is null then
                    null
                when length(replace(reference_text_67, ' ', '')) > 0 then
                    replace(replace(reference_text_67, '/#/', ''''), '/##/', '\\ ')
            end reference_text_67,
        	case when reference_text_68 is null then
                    null
                when length(replace(reference_text_68, ' ', '')) > 0 then
                    replace(replace(reference_text_68, '/#/', ''''), '/##/', '\\ ')
            end reference_text_68,
        	case when reference_text_69 is null then
                    null
                when length(replace(reference_text_69, ' ', '')) > 0 then
                    replace(replace(reference_text_69, '/#/', ''''), '/##/', '\\ ')
            end reference_text_69,
        	case when reference_text_70 is null then
                    null
                when length(replace(reference_text_70, ' ', '')) > 0 then
                    replace(replace(reference_text_70, '/#/', ''''), '/##/', '\\ ')
            end reference_text_70,
        	case when reference_text_71 is null then
                    null
                when length(replace(reference_text_71, ' ', '')) > 0 then
                    replace(replace(reference_text_71, '/#/', ''''), '/##/', '\\ ')
            end reference_text_71,
        	case when reference_text_72 is null then
                    null
                when length(replace(reference_text_72, ' ', '')) > 0 then
                    replace(replace(reference_text_72, '/#/', ''''), '/##/', '\\ ')
            end reference_text_72,
        	case when reference_text_73 is null then
                    null
                when length(replace(reference_text_73, ' ', '')) > 0 then
                    replace(replace(reference_text_73, '/#/', ''''), '/##/', '\\ ')
            end reference_text_73,
        	case when reference_text_74 is null then
                    null
                when length(replace(reference_text_74, ' ', '')) > 0 then
                    replace(replace(reference_text_74, '/#/', ''''), '/##/', '\\ ')
            end reference_text_74,
        	case when reference_text_75 is null then
                    null
                when length(replace(reference_text_75, ' ', '')) > 0 then
                    replace(replace(reference_text_75, '/#/', ''''), '/##/', '\\ ')
            end reference_text_75,
        	case when reference_text_76 is null then
                    null
                when length(replace(reference_text_76, ' ', '')) > 0 then
                    replace(replace(reference_text_76, '/#/', ''''), '/##/', '\\ ')
            end reference_text_76,
        	case when reference_text_77 is null then
                    null
                when length(replace(reference_text_77, ' ', '')) > 0 then
                    replace(replace(reference_text_77, '/#/', ''''), '/##/', '\\ ')
            end reference_text_77,
        	case when reference_text_78 is null then
                    null
                when length(replace(reference_text_78, ' ', '')) > 0 then
                    replace(replace(reference_text_78, '/#/', ''''), '/##/', '\\ ')
            end reference_text_78,
        	case when reference_text_79 is null then
                    null
                when length(replace(reference_text_79, ' ', '')) > 0 then
                    replace(replace(reference_text_79, '/#/', ''''), '/##/', '\\ ')
            end reference_text_79,
        	case when reference_text_80 is null then
                    null
                when length(replace(reference_text_80, ' ', '')) > 0 then
                    replace(replace(reference_text_80, '/#/', ''''), '/##/', '\\ ')
            end reference_text_80,
        	case when reference_text_81 is null then
                    null
                when length(replace(reference_text_81, ' ', '')) > 0 then
                    replace(replace(reference_text_81, '/#/', ''''), '/##/', '\\ ')
            end reference_text_81,
        	case when reference_text_82 is null then
                    null
                when length(replace(reference_text_82, ' ', '')) > 0 then
                    replace(replace(reference_text_82, '/#/', ''''), '/##/', '\\ ')
            end reference_text_82,
        	case when reference_text_83 is null then
                    null
                when length(replace(reference_text_83, ' ', '')) > 0 then
                    replace(replace(reference_text_83, '/#/', ''''), '/##/', '\\ ')
            end reference_text_83,
        	case when reference_text_84 is null then
                    null
                when length(replace(reference_text_84, ' ', '')) > 0 then
                    replace(replace(reference_text_84, '/#/', ''''), '/##/', '\\ ')
            end reference_text_84,
        	case when reference_text_85 is null then
                    null
                when length(replace(reference_text_85, ' ', '')) > 0 then
                    replace(replace(reference_text_85, '/#/', ''''), '/##/', '\\ ')
            end reference_text_85,
        	case when reference_text_86 is null then
                    null
                when length(replace(reference_text_86, ' ', '')) > 0 then
                    replace(replace(reference_text_86, '/#/', ''''), '/##/', '\\ ')
            end reference_text_86,
        	case when reference_text_87 is null then
                    null
                when length(replace(reference_text_87, ' ', '')) > 0 then
                    replace(replace(reference_text_87, '/#/', ''''), '/##/', '\\ ')
            end reference_text_87,
        	case when reference_text_88 is null then
                    null
                when length(replace(reference_text_88, ' ', '')) > 0 then
                    replace(replace(reference_text_88, '/#/', ''''), '/##/', '\\ ')
            end reference_text_88,
        	case when reference_text_89 is null then
                    null
                when length(replace(reference_text_89, ' ', '')) > 0 then
                    replace(replace(reference_text_89, '/#/', ''''), '/##/', '\\ ')
            end reference_text_89,
        	case when reference_text_90 is null then
                    null
                when length(replace(reference_text_90, ' ', '')) > 0 then
                    replace(replace(reference_text_90, '/#/', ''''), '/##/', '\\ ')
            end reference_text_90,
        	case when reference_text_91 is null then
                    null
                when length(replace(reference_text_91, ' ', '')) > 0 then
                    replace(replace(reference_text_91, '/#/', ''''), '/##/', '\\ ')
            end reference_text_91,
        	case when reference_text_92 is null then
                    null
                when length(replace(reference_text_92, ' ', '')) > 0 then
                    replace(replace(reference_text_92, '/#/', ''''), '/##/', '\\ ')
            end reference_text_92,
        	case when reference_text_93 is null then
                    null
                when length(replace(reference_text_93, ' ', '')) > 0 then
                    replace(replace(reference_text_93, '/#/', ''''), '/##/', '\\ ')
            end reference_text_93,
        	case when reference_text_94 is null then
                    null
                when length(replace(reference_text_94, ' ', '')) > 0 then
                    replace(replace(reference_text_94, '/#/', ''''), '/##/', '\\ ')
            end reference_text_94,
        	case when reference_text_95 is null then
                    null
                when length(replace(reference_text_95, ' ', '')) > 0 then
                    replace(replace(reference_text_95, '/#/', ''''), '/##/', '\\ ')
            end reference_text_95,
        	case when reference_text_96 is null then
                    null
                when length(replace(reference_text_96, ' ', '')) > 0 then
                    replace(replace(reference_text_96, '/#/', ''''), '/##/', '\\ ')
            end reference_text_96,
        	case when reference_text_97 is null then
                    null
                when length(replace(reference_text_97, ' ', '')) > 0 then
                    replace(replace(reference_text_97, '/#/', ''''), '/##/', '\\ ')
            end reference_text_97,
        	case when reference_text_98 is null then
                    null
                when length(replace(reference_text_98, ' ', '')) > 0 then
                    replace(replace(reference_text_98, '/#/', ''''), '/##/', '\\ ')
            end reference_text_98,
        	case when reference_text_99 is null then
                    null
                when length(replace(reference_text_99, ' ', '')) > 0 then
                    replace(replace(reference_text_99, '/#/', ''''), '/##/', '\\ ')
            end reference_text_99,
        	case when reference_text_100 is null then
                    null
                when length(replace(reference_text_100, ' ', '')) > 0 then
                    replace(replace(reference_text_100, '/#/', ''''), '/##/', '\\ ')
            end reference_text_100,
        	case when reference_int_1 is null then
                    null
                when length(replace(reference_int_1, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_1, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_1,
        	case when reference_int_2 is null then
                    null
                when length(replace(reference_int_2, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_2, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_2,
        	case when reference_int_3 is null then
                    null
                when length(replace(reference_int_3, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_3, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_3,
        	case when reference_int_4 is null then
                    null
                when length(replace(reference_int_4, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_4, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_4,
        	case when reference_int_5 is null then
                    null
                when length(replace(reference_int_5, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_5, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_5,
        	case when reference_int_6 is null then
                    null
                when length(replace(reference_int_6, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_6, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_6,
        	case when reference_int_7 is null then
                    null
                when length(replace(reference_int_7, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_7, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_7,
        	case when reference_int_8 is null then
                    null
                when length(replace(reference_int_8, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_8, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_8,
        	case when reference_int_9 is null then
                    null
                when length(replace(reference_int_9, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_9, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_9,
        	case when reference_int_10 is null then
                    null
                when length(replace(reference_int_10, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_10, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_10,
        	case when reference_int_11 is null then
                    null
                when length(replace(reference_int_11, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_11, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_11,
        	case when reference_int_12 is null then
                    null
                when length(replace(reference_int_12, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_12, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_12,
        	case when reference_int_13 is null then
                    null
                when length(replace(reference_int_13, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_13, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_13,
        	case when reference_int_14 is null then
                    null
                when length(replace(reference_int_14, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_14, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_14,
        	case when reference_int_15 is null then
                    null
                when length(replace(reference_int_15, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_15, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_15,
        	case when reference_int_16 is null then
                    null
                when length(replace(reference_int_16, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_16, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_16,
        	case when reference_int_17 is null then
                    null
                when length(replace(reference_int_17, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_17, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_17,
        	case when reference_int_18 is null then
                    null
                when length(replace(reference_int_18, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_18, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_18,
        	case when reference_int_19 is null then
                    null
                when length(replace(reference_int_19, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_19, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_19,
        	case when reference_int_20 is null then
                    null
                when length(replace(reference_int_20, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_20, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_20,
        	case when reference_int_21 is null then
                    null
                when length(replace(reference_int_21, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_21, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_21,
        	case when reference_int_22 is null then
                    null
                when length(replace(reference_int_22, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_22, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_22,
        	case when reference_int_23 is null then
                    null
                when length(replace(reference_int_23, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_23, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_23,
        	case when reference_int_24 is null then
                    null
                when length(replace(reference_int_24, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_24, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_24,
        	case when reference_int_25 is null then
                    null
                when length(replace(reference_int_25, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_25, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_25,
        	case when reference_int_26 is null then
                    null
                when length(replace(reference_int_26, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_26, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_26,
        	case when reference_int_27 is null then
                    null
                when length(replace(reference_int_27, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_27, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_27,
        	case when reference_int_28 is null then
                    null
                when length(replace(reference_int_28, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_28, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_28,
        	case when reference_int_29 is null then
                    null
                when length(replace(reference_int_29, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_29, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_29,
        	case when reference_int_30 is null then
                    null
                when length(replace(reference_int_30, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_30, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_30,
        	case when reference_int_31 is null then
                    null
                when length(replace(reference_int_31, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_31, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_31,
        	case when reference_int_32 is null then
                    null
                when length(replace(reference_int_32, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_32, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_32,
        	case when reference_int_33 is null then
                    null
                when length(replace(reference_int_33, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_33, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_33,
        	case when reference_int_34 is null then
                    null
                when length(replace(reference_int_34, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_34, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_34,
        	case when reference_int_35 is null then
                    null
                when length(replace(reference_int_35, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_35, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_35,
        	case when reference_int_36 is null then
                    null
                when length(replace(reference_int_36, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_36, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_36,
        	case when reference_int_37 is null then
                    null
                when length(replace(reference_int_37, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_37, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_37,
        	case when reference_int_38 is null then
                    null
                when length(replace(reference_int_38, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_38, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_38,
        	case when reference_int_39 is null then
                    null
                when length(replace(reference_int_39, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_39, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_39,
        	case when reference_int_40 is null then
                    null
                when length(replace(reference_int_40, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_40, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_40,
        	case when reference_int_41 is null then
                    null
                when length(replace(reference_int_41, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_41, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_41,
        	case when reference_int_42 is null then
                    null
                when length(replace(reference_int_42, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_42, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_42,
        	case when reference_int_43 is null then
                    null
                when length(replace(reference_int_43, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_43, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_43,
        	case when reference_int_44 is null then
                    null
                when length(replace(reference_int_44, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_44, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_44,
        	case when reference_int_45 is null then
                    null
                when length(replace(reference_int_45, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_45, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_45,
        	case when reference_int_46 is null then
                    null
                when length(replace(reference_int_46, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_46, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_46,
        	case when reference_int_47 is null then
                    null
                when length(replace(reference_int_47, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_47, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_47,
        	case when reference_int_48 is null then
                    null
                when length(replace(reference_int_48, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_48, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_48,
        	case when reference_int_49 is null then
                    null
                when length(replace(reference_int_49, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_49, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_49,
        	case when reference_int_50 is null then
                    null
                when length(replace(reference_int_50, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_50, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_50,
        	case when reference_int_51 is null then
                    null
                when length(replace(reference_int_51, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_51, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_51,
        	case when reference_int_52 is null then
                    null
                when length(replace(reference_int_52, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_52, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_52,
        	case when reference_int_53 is null then
                    null
                when length(replace(reference_int_53, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_53, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_53,
        	case when reference_int_54 is null then
                    null
                when length(replace(reference_int_54, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_54, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_54,
        	case when reference_int_55 is null then
                    null
                when length(replace(reference_int_55, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_55, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_55,
        	case when reference_int_56 is null then
                    null
                when length(replace(reference_int_56, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_56, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_56,
        	case when reference_int_57 is null then
                    null
                when length(replace(reference_int_57, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_57, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_57,
        	case when reference_int_58 is null then
                    null
                when length(replace(reference_int_58, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_58, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_58,
        	case when reference_int_59 is null then
                    null
                when length(replace(reference_int_59, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_59, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_59,
        	case when reference_int_60 is null then
                    null
                when length(replace(reference_int_60, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_60, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_60,
        	case when reference_int_61 is null then
                    null
                when length(replace(reference_int_61, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_61, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_61,
        	case when reference_int_62 is null then
                    null
                when length(replace(reference_int_62, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_62, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_62,
        	case when reference_int_63 is null then
                    null
                when length(replace(reference_int_63, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_63, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_63,
        	case when reference_int_64 is null then
                    null
                when length(replace(reference_int_64, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_64, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_64,
        	case when reference_int_65 is null then
                    null
                when length(replace(reference_int_65, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_65, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_65,
        	case when reference_int_66 is null then
                    null
                when length(replace(reference_int_66, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_66, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_66,
        	case when reference_int_67 is null then
                    null
                when length(replace(reference_int_67, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_67, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_67,
        	case when reference_int_68 is null then
                    null
                when length(replace(reference_int_68, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_68, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_68,
        	case when reference_int_69 is null then
                    null
                when length(replace(reference_int_69, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_69, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_69,
        	case when reference_int_70 is null then
                    null
                when length(replace(reference_int_70, ' ', '')) > 0 then
                    convert(replace(replace(reference_int_70, '/#/', ''''), '/##/', '\\ '), signed)
            end reference_int_70,
        	case when reference_dec_1 is null then
                    null
                when length(replace(reference_dec_1, ' ', '')) > 0 then
                    cast(replace(replace(reference_dec_1, '/#/', ''''), '/##/', '\\ '), as decimal(15, 2))
            end reference_dec_1,



    elseif (iNewCheckRowCount = 0 and iDupCheckRowCount > 0) then
        
        -- Updating Status as Duplicate
        update consolidation_files.stg_report_storage s
            inner join tmp_old_check t
                on s.id = t.s_pk
            set
                s.processing_status = 'Duplicate',
                s.modified_by = vUserId,
                s.modified_date = vUpdateToken
            where
                s.processing_status = 'New'
                and s.file_id = vFileId
                and tenants_id = vTenantsId
                and entities_id = vEntitiesId
                and m_processing_layer_id = vMProcessingLayerId
                and m_processing_sub_layer_id = VMProcessingSubLayerId
                and processing_layer_id = vProcessingLayerId
                and m_sources_id = vMsourcesId;

        -- Update File Uploads Table Status to Success
        update consolidation_files.file_uploads
            set 
                status = 'SUCCESS',
                comments = 'File Processing Completed Successfully!!!',
                modified_by = vUserId,
                modified_date = vUpdateToken,
                is_processed = 1;

    end if;
    
COMMIT;

END
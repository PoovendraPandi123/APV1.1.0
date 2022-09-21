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

        ------- Need to DO

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
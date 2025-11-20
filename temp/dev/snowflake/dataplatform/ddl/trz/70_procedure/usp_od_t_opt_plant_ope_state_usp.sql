```sql
CREATE OR REPLACE PROCEDURE USP_OD_T_OPT_PLANT_OPE_STATE()
RETURNS OBJECT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    // Start transaction
    snowflake.execute({ sqlText: 'BEGIN;' });
    // Use the appropriate schema
    snowflake.execute({ sqlText: 'USE SCHEMA dlz;' });

    // Step 1: Data Extraction
    // Insert data from the source stream into the temporary table where the action is 'INSERT'
    snowflake.execute({ sqlText: 'INSERT INTO dlz.tmp_str_dl_t_opt_plant_ope_state_01 SELECT * FROM dlz.str_dl_t_opt_plant_ope_state_01 WHERE METADATA$ACTION = \'INSERT\';' });

    // Step 2: Metadata Assignment
    // Call the metadata assignment procedure
    snowflake.execute({ sqlText: 'CALL rfz_vc_opt.usp_sub_common_mtdata(\'dlz\', \'tmp_str_dl_t_opt_plant_ope_state_01\', \'rfz_vc_opt\', \'dw_v_opt_supply_deman_ext_ind_mgmt\', \'[\"if_id\", \"product_id\"]\', \'[\"area_name\"]\', TO_VARCHAR(CURRENT_DATE(), \'YYYY/MM/DD\'), \'USP_OD_T_OPT_PLANT_OPE_STATE\', \'dlz\');' });

    // Step 3: Validation - NULL Check
    // Call the null validation procedure
    snowflake.execute({ sqlText: 'CALL trz_vc_opt.usp_sub_null_validation(\'tmp_str_dl_t_opt_plant_ope_state_01\', \'dlz\', \'od_t_opt_plant_ope_state\', \'trz_vc_opt\', \'USP_OD_T_OPT_PLANT_OPE_STATE\', \'dlz\');' });

    // Step 4: Validation - Data Type Check
    // Call the data type validation procedure
    snowflake.execute({ sqlText: 'CALL trz_vc_opt.usp_sub_type_digit_validation(\'tmp_str_dl_t_opt_plant_ope_state_01\', \'dlz\', \'od_t_opt_plant_ope_state\', \'trz_vc_opt\', \'USP_OD_T_OPT_PLANT_OPE_STATE\', \'dlz\');' });

    // Step 5: Validation - Duplication Check
    // Call the duplication validation procedure
    snowflake.execute({ sqlText: 'CALL trz_vc_opt.usp_sub_duplication_validation(\'tmp_str_dl_t_opt_plant_ope_state_01\', \'dlz\', \'[\"plant_id\", \"unit_name\", \"stop_datetime\"]\', \'USP_OD_T_OPT_PLANT_OPE_STATE\', \'dlz\');' });

    // Step 6: Data Insertion
    // Delete old records and insert new data into the target table
    snowflake.execute({ sqlText: 'DELETE FROM trz_vc_opt.od_t_opt_plant_ope_state WHERE process_at < DATEADD(month, -1, CURRENT_DATE());' });
    snowflake.execute({ sqlText: 'INSERT INTO trz_vc_opt.od_t_opt_plant_ope_state (section_datetime, area_name, generation_company_name, plant_id, plant_name, generation_type_text, unit_name, auth_output_value, stop_class, type_name, decrease_value, stop_datetime, sfr_forecast_text, sfr_date, stop_cause_text, last_update_datetime, product_id, ubis_data_id, ind_name, ind_jpn_name, unit, data_creator_id, data_creator_name, process_flag, aggregation_unit, cale_id, cale_name, if_id, if_file_name, if_row_number, created_by, create_at, update_at, process_at, process_id) SELECT TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()), CAST(area_code AS VARCHAR(2)), CAST(area_name AS VARCHAR(10)), CAST(generation_company_name AS VARCHAR(100)), CAST(plant_id AS VARCHAR(15)), CAST(plant_name AS VARCHAR(150)), CAST(generation_type_text AS VARCHAR(20)), CAST(unit_name AS VARCHAR(50)), CAST(auth_output_value AS NUMBER(17,2)), CAST(stop_class AS VARCHAR(20)), CAST(type_name AS VARCHAR(40)), CAST(decrease_value AS NUMBER(17,2)), TO_TIMESTAMP_NTZ(stop_datetime, \'YYYY/MM/DD HH24:MI\'), CAST(sfr_forecast_text AS VARCHAR(10)), TO_DATE(sfr_date, \'YYYY/MM/DD\'), CAST(stop_cause_text AS VARCHAR), TO_TIMESTAMP_NTZ(last_update_datetime, \'YYYY/MM/DD HH24:MI\'), CAST(product_id AS VARCHAR(255)), CAST(ubis_data_id AS VARCHAR(6)), CAST(ind_name AS VARCHAR(255)), CAST(ind_jpn_name AS VARCHAR(255)), CAST(unit AS VARCHAR(255)), CAST(data_creator_id AS VARCHAR(6)), CAST(data_creator_name AS VARCHAR(255)), CAST(process_flag AS BOOLEAN), CAST(aggregation_unit AS VARCHAR(255)), CAST(cale_id AS VARCHAR(6)), CAST(cale_name AS VARCHAR(255)), CAST(if_id AS VARCHAR(255)), CAST(if_file_name AS VARCHAR(255)), CAST(if_row_number AS NUMBER(38,0)), CAST(created_by AS VARCHAR(255)), CURRENT_TIMESTAMP(), NULL, CURRENT_TIMESTAMP(), \'USP_OD_T_OPT_PLANT_OPE_STATE\' FROM dlz.tmp_str_dl_t_opt_plant_ope_state_01;' });

    // Commit the transaction
    snowflake.execute({ sqlText: 'COMMIT;' });

    return { "status": "SUCCESS" };
}
catch (err) {
    // Rollback the transaction in case of error
    snowflake.execute({ sqlText: 'ROLLBACK;' });
    throw err;
}
$$;
```
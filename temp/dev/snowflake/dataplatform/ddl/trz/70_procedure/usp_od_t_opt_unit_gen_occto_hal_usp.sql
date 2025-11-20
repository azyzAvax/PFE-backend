```sql
CREATE OR REPLACE PROCEDURE USP_OD_T_OPT_UNIT_GEN_OCCTO_HALFHOUR()
RETURNS OBJECT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    // Start transaction
    snowflake.execute({ sqlText: 'BEGIN;' });
    
    // Use the appropriate schema
    snowflake.execute({ sqlText: 'USE SCHEMA TRZ_VC_OPT;' });

    // Step 1: Data Extraction
    // Extract data from the source table into the temporary table
    snowflake.execute({ sqlText: `
        INSERT INTO tmp_str_dl_t_opt_unit_gen_occto_halfhour_01
        SELECT plant_code, area_code, area_name, plant_name, unit_name, gen_class, target_date,
               gen_0030_value, gen_0100_value, gen_0130_value, gen_0200_value, gen_0230_value,
               gen_0300_value, gen_0330_value, gen_0400_value, gen_0430_value, gen_0500_value,
               gen_0530_value, gen_0600_value, gen_0630_value, gen_0700_value, gen_0730_value,
               gen_0800_value, gen_0830_value, gen_0900_value, gen_0930_value, gen_1000_value,
               gen_1030_value, gen_1100_value, gen_1130_value, gen_1200_value, gen_1230_value,
               gen_1300_value, gen_1330_value, gen_1400_value, gen_1430_value, gen_1500_value,
               gen_1530_value, gen_1600_value, gen_1630_value, gen_1700_value, gen_1730_value,
               gen_1800_value, gen_1830_value, gen_1900_value, gen_1930_value, gen_2000_value,
               gen_2030_value, gen_2100_value, gen_2130_value, gen_2200_value, gen_2230_value,
               gen_2300_value, gen_2330_value, gen_2400_value, gen_daily_value, source_update_at,
               url_text, product_id, ubis_data_id, ind_name, ind_jpn_name, unit, data_creator_id,
               data_creator_name, process_flag, aggregation_unit, cale_id, cale_name, if_id,
               if_file_name, if_row_number, created_by, create_at, update_at, process_at, process_id
        FROM dlz.dl_t_opt_unit_gen_occto_halfhour
        WHERE <conditions>;
    ` });

    // Step 2: Validation - NULL Check
    // Call the stored procedure for NULL validation
    snowflake.execute({ sqlText: `
        CALL USP_SUB_NULL_VALIDATION('tmp_str_dl_t_opt_unit_gen_occto_halfhour_01', 'dlz', 
        'od_t_opt_unit_gen_occto_halfhour', 'TRZ_VC_OPT', 'USP_OD_T_OPT_UNIT_GEN_OCCTO_HALFHOUR', 'TRZ_VC_OPT');
    ` });

    // Step 3: Validation - Data Type Check
    // Call the stored procedure for data type validation
    snowflake.execute({ sqlText: `
        CALL USP_SUB_TYPE_DIGIT_VALIDATION('tmp_str_dl_t_opt_unit_gen_occto_halfhour_01', 'dlz', 
        'od_t_opt_unit_gen_occto_halfhour', 'TRZ_VC_OPT', 'USP_OD_T_OPT_UNIT_GEN_OCCTO_HALFHOUR', 'TRZ_VC_OPT');
    ` });

    // Step 4: Validation - Duplication Check
    // Call the stored procedure for duplication validation
    snowflake.execute({ sqlText: `
        CALL USP_SUB_DUPLICATION_VALIDATION('tmp_str_dl_t_opt_unit_gen_occto_halfhour_01', 'dlz', 
        'USP_OD_T_OPT_UNIT_GEN_OCCTO_HALFHOUR', 'TRZ_VC_OPT');
    ` });

    // Step 5: Merging/Updating Data
    // Merge the data from the temporary table into the target table
    snowflake.execute({ sqlText: `
        MERGE INTO od_t_opt_unit_gen_occto_halfhour AS target
        USING tmp_str_dl_t_opt_unit_gen_occto_halfhour_01 AS source
        ON (target.plant_code = source.plant_code AND target.unit_name = source.unit_name 
            AND target.target_date = source.target_date)
        WHEN MATCHED THEN UPDATE SET 
            target.area_code = source.area_code,
            target.area_name = source.area_name,
            target.plant_name = source.plant_name,
            target.gen_class = source.gen_class,
            target.gen_0030_value = source.gen_0030_value,
            target.gen_0100_value = source.gen_0100_value,
            target.gen_0130_value = source.gen_0130_value,
            target.gen_0200_value = source.gen_0200_value,
            target.gen_0230_value = source.gen_0230_value,
            target.gen_0300_value = source.gen_0300_value,
            target.gen_0330_value = source.gen_0330_value,
            target.gen_0400_value = source.gen_0400_value,
            target.gen_0430_value = source.gen_0430_value,
            target.gen_0500_value = source.gen_0500_value,
            target.gen_0530_value = source.gen_0530_value,
            target.gen_0600_value = source.gen_0600_value,
            target.gen_0630_value = source.gen_0630_value,
            target.gen_0700_value = source.gen_0700_value,
            target.gen_0730_value = source.gen_0730_value,
            target.gen_0800_value = source.gen_0800_value,
            target.gen_0830_value = source.gen_0830_value,
            target.gen_0900_value = source.gen_0900_value,
            target.gen_0930_value = source.gen_0930_value,
            target.gen_1000_value = source.gen_1000_value,
            target.gen_1030_value = source.gen_1030_value,
            target.gen_1100_value = source.gen_1100_value,
            target.gen_1130_value = source.gen_1130_value,
            target.gen_1200_value = source.gen_1200_value,
            target.gen_1230_value = source.gen_1230_value,
            target.gen_1300_value = source.gen_1300_value,
            target.gen_1330_value = source.gen_1330_value,
            target.gen_1400_value = source.gen_1400_value,
            target.gen_1430_value = source.gen_1430_value,
            target.gen_1500_value = source.gen_1500_value,
            target.gen_1530_value = source.gen_1530_value,
            target.gen_1600_value = source.gen_1600_value,
            target.gen_1630_value = source.gen_1630_value,
            target.gen_1700_value = source.gen_1700_value,
            target.gen_1730_value = source.gen_1730_value,
            target.gen_1800_value = source.gen_1800_value,
            target.gen_1830_value = source.gen_1830_value,
            target.gen_1900_value = source.gen_1900_value,
            target.gen_1930_value = source.gen_1930_value,
            target.gen_2000_value = source.gen_2000_value,
            target.gen_2030_value = source.gen_2030_value,
            target.gen_2100_value = source.gen_2100_value,
            target.gen_2130_value = source.gen_2130_value,
            target.gen_2200_value = source.gen_2200_value,
            target.gen_2230_value = source.gen_2230_value,
            target.gen_2300_value = source.gen_2300_value,
            target.gen_2330_value = source.gen_2330_value,
            target.gen_2400_value = source.gen_2400_value,
            target.gen_daily_value = source.gen_daily_value,
            target.source_update_at = source.source_update_at,
            target.url_text = source.url_text,
            target.product_id = source.product_id,
            target.ubis_data_id = source.ubis_data_id,
            target.ind_name = source.ind_name,
            target.ind_jpn_name = source.ind_jpn_name,
            target.unit = source.unit,
            target.data_creator_id = source.data_creator_id,
            target.data_creator_name = source.data_creator_name,
            target.process_flag = source.process_flag,
            target.aggregation_unit = source.aggregation_unit,
            target.cale_id = source.cale_id,
            target.cale_name = source.cale_name,
            target.if_id = source.if_id,
            target.if_file_name = source.if_file_name,
            target.if_row_number = source.if_row_number,
            target.created_by = source.created_by;
    ` });

    // Step 6: Cleanup
    // Drop the temporary table after processing
    snowflake.execute({ sqlText: 'DROP TABLE IF EXISTS tmp_str_dl_t_opt_unit_gen_occto_halfhour_01;' });

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
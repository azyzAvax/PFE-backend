```sql
CREATE OR REPLACE PROCEDURE trz_vc_opt.usp_sub_power_ind_mtdata()
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
    // Create a temporary table to hold extracted data from the source table
    snowflake.execute({ sqlText: `
        CREATE OR REPLACE TEMPORARY TABLE dlz.tmp_str_dl_t_opt_power_fut_vanir AS 
        SELECT * 
        FROM dlz.str_dl_t_opt_power_fut_vanir_01 no1 
        LEFT JOIN rfz_vc_opt.dw_v_opt_power_ind_mgmt_m no3 
        ON no1.product_id = no3.product_id;` 
    });

    // Step 2: Metadata Assignment
    // Call the metadata assignment procedure to process the temporary table into the target table
    snowflake.execute({ sqlText: `
        CALL trz_vc_opt.usp_sub_power_ind_mtdata(
            'dlz.tmp_str_dl_t_opt_power_fut_vanir', 
            'dlz', 
            'trz_vc_opt.od_t_opt_power_fut_vanir', 
            'trz_vc_opt', 
            CURRENT_DATE, 
            'usp_od_t_opt_power_fut_vanir'
        );` 
    });

    // Step 3: Validation
    // Perform various validation checks on the temporary table
    snowflake.execute({ sqlText: `
        CALL trz_vc_opt.usp_sub_null_validation(
            'tmp_str_dl_t_opt_power_fut_vanir', 
            'dlz', 
            'od_t_opt_power_fut_vanir', 
            'trz_vc_opt', 
            'usp_od_t_opt_power_fut_vanir', 
            'trz_vc_opt'
        );` 
    });
    
    snowflake.execute({ sqlText: `
        CALL trz_vc_opt.usp_sub_type_digit_validation(
            'tmp_str_dl_t_opt_power_fut_vanir', 
            'dlz', 
            'od_t_opt_power_fut_vanir', 
            'trz_vc_opt', 
            'usp_od_t_opt_power_fut_vanir', 
            'trz_vc_opt'
        );` 
    });
    
    snowflake.execute({ sqlText: `
        CALL trz_vc_opt.usp_sub_duplication_validation(
            'tmp_str_dl_t_opt_power_fut_vanir', 
            'dlz', 
            '[product_id, price_date, deliv_month]', 
            'usp_od_t_opt_power_fut_vanir', 
            'trz_vc_opt'
        );` 
    });

    // Step 4: Update
    // Merge the temporary table data into the target table
    snowflake.execute({ sqlText: `
        MERGE INTO trz_vc_opt.od_t_opt_power_fut_vanir AS target 
        USING (SELECT * FROM dlz.tmp_str_dl_t_opt_power_fut_vanir) AS source 
        ON (target.product_id = source.product_id 
            AND target.price_date = TO_DATE(source.price_date, 'YYYY/MM/DD') 
            AND target.deliv_month = TO_DATE(source.deliv_month, 'YYYY/MM/DD')) 
        WHEN MATCHED THEN 
            UPDATE SET target.price_value = source.price_value, 
                        target.data_class = source.data_class 
        WHEN NOT MATCHED THEN 
            INSERT (product_id, price_date, deliv_month, price_value, data_class) 
            VALUES (source.product_id, source.price_date, source.deliv_month, source.price_value, source.data_class);` 
    });

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
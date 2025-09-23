CREATE OR REPLACE PIPE dlz.pip_dl_t_opt_power_fut_vanir
  INTEGRATION = NTF_INT_EVENTS
  AUTO_INGEST = TRUE
  COMMENT = '電力_Futures_Vanir_DLZデータ取込'
  AS
  COPY INTO dlz.dl_t_opt_power_fut_vanir (
    product_id,
    price_date,
    deliv_month,
    price_value,
    data_class,
    trade_type_name,
    create_date,
    if_id,
    if_file_name,
    if_row_number,
    created_by,
    rpa_process_at,
    create_at,
    update_at,
    process_at,
    process_id
  )
  FROM (
    SELECT
      t.$1 AS product_id,
      t.$2 AS price_date,
      t.$3 AS deliv_month,
      t.$4 AS price_value,
      t.$5 AS data_class,
      t.$6 AS trade_type_name,
      TO_VARCHAR(CURRENT_DATE(), 'YYYY/MM/DD') AS create_date,
      t.$7 AS if_id,
      METADATA$FILENAME AS if_file_name,
      METADATA$FILE_ROW_NUMBER AS if_row_number,
      t.$8 AS created_by,
      t.$9 AS rpa_process_at,
      CURRENT_TIMESTAMP() AS create_at,
      NULL AS update_at,
      CURRENT_TIMESTAMP() AS process_at,
      'PIP_DL_T_OPT_POWER_FUT_VANIR' AS process_id
    FROM
      @dlz.stg_vcopt_snowflake/dl_t_opt_power_fut_vanir/
    (
        FILE_FORMAT => 'dlz.fmt_csv_vcopt_01',
        pattern=>'.*[.]csv'
      ) t
  );
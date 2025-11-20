CREATE OR REPLACE TABLE rfz_vc_opt.dw_v_opt_power_cap_market_by_area_auc_cont_rslt_latest (
  ubis_data_id STRING COMMENT 'データID',
  ind_name STRING COMMENT '指標名',
  ind_jpn_name STRING COMMENT '指標名_日本語',
  cont_rslt_rls_date DATE COMMENT '約定結果公表日',
  target_fiscal_year STRING COMMENT '対象実需給年度',
  auc_type_name STRING COMMENT 'オークション種別',
  area_code STRING COMMENT 'エリアコード',
  area_name STRING COMMENT 'エリア名',
  area_unitprice FLOAT COMMENT 'エリアプライス(円/kW)',
  cont_cap_volume FLOAT COMMENT '約定容量(kW)',
  after_ded_cont_ttl_amount FLOAT COMMENT '約定総額（経過措置控除後）(円)',
  data_creator_id STRING COMMENT 'データ諸元ID',
  data_creator_name STRING COMMENT 'データ諸元名',
  url_text STRING COMMENT 'URL',
  process_flag STRING COMMENT '加工フラグ',
  created_by STRING COMMENT '作成者',
  create_at TIMESTAMP COMMENT '作成日時',
  update_at TIMESTAMP COMMENT '更新日時',
  process_at TIMESTAMP COMMENT '処理日時',
  process_id STRING COMMENT '処理ID'
) COMMENT = '容量市場_エリア別_オークション約定結果_最新断面' 
DATA_RETENTION_TIME_IN_DAYS = 30;
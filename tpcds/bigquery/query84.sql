SELECT c_customer_id   AS customer_id, 
               c_last_name 
               || ', ' 
               || c_first_name AS customername 
FROM   `hadoop-benchmarking01`.TPCDS_1TB.customer, 
       `hadoop-benchmarking01`.TPCDS_1TB.customer_address, 
       `hadoop-benchmarking01`.TPCDS_1TB.customer_demographics, 
       `hadoop-benchmarking01`.TPCDS_1TB.household_demographics, 
       `hadoop-benchmarking01`.TPCDS_1TB.income_band, 
       `hadoop-benchmarking01`.TPCDS_1TB.store_returns 
WHERE  ca_city = 'Green Acres' 
       AND c_current_addr_sk = ca_address_sk 
       AND ib_lower_bound >= 54986 
       AND ib_upper_bound <= 54986 + 50000 
       AND ib_income_band_sk = hd_income_band_sk 
       AND cd_demo_sk = c_current_cdemo_sk 
       AND hd_demo_sk = c_current_hdemo_sk 
       AND sr_cdemo_sk = cd_demo_sk 
ORDER  BY c_customer_id;
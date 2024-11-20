SELECT i_item_id, 
               ca_country, 
               ca_state, 
               ca_county, 
               Avg(Cast(cs_quantity AS NUMERIC))      agg1, 
               Avg(Cast(cs_list_price AS NUMERIC))    agg2, 
               Avg(Cast(cs_coupon_amt AS NUMERIC))    agg3, 
               Avg(Cast(cs_sales_price AS NUMERIC))   agg4, 
               Avg(Cast(cs_net_profit AS NUMERIC))    agg5, 
               Avg(Cast(c_birth_year AS NUMERIC))     agg6, 
               Avg(Cast(cd1.cd_dep_count AS NUMERIC)) agg7 
FROM   `hadoop-benchmarking01`.TPCDS_1TB.catalog_sales, 
       `hadoop-benchmarking01`.TPCDS_1TB.customer_demographics cd1, 
       `hadoop-benchmarking01`.TPCDS_1TB.customer_demographics cd2, 
       `hadoop-benchmarking01`.TPCDS_1TB.customer, 
       `hadoop-benchmarking01`.TPCDS_1TB.customer_address, 
       `hadoop-benchmarking01`.TPCDS_1TB.date_dim, 
       `hadoop-benchmarking01`.TPCDS_1TB.item 
WHERE  cs_sold_date_sk = d_date_sk 
       AND cs_item_sk = i_item_sk 
       AND cs_bill_cdemo_sk = cd1.cd_demo_sk 
       AND cs_bill_customer_sk = c_customer_sk 
       AND cd1.cd_gender = 'F' 
       AND cd1.cd_education_status = 'Secondary' 
       AND c_current_cdemo_sk = cd2.cd_demo_sk 
       AND c_current_addr_sk = ca_address_sk 
       AND c_birth_month IN ( 8, 4, 2, 5, 
                              11, 9 ) 
       AND d_year = 2001 
       AND ca_state IN ( 'KS', 'IA', 'AL', 'UT', 
                         'VA', 'NC', 'TX' ) 
GROUP  BY rollup ( i_item_id, ca_country, ca_state, ca_county ) 
ORDER  BY ca_country, 
          ca_state, 
          ca_county, 
          i_item_id;
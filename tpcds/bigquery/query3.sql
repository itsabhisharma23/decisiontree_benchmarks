## Query 3
SELECT dt.d_year, 
       item.i_brand_id          brand_id, 
       item.i_brand             brand, 
       Sum(ss_ext_discount_amt) sum_agg 
FROM   `hadoop-benchmarking01`.TPCDS_1TB.date_dim dt, 
       `hadoop-benchmarking01`.TPCDS_1TB.store_sales, 
       `hadoop-benchmarking01`.TPCDS_1TB.item 
WHERE  dt.d_date_sk = store_sales.ss_sold_date_sk 
       AND store_sales.ss_item_sk = item.i_item_sk 
       AND item.i_manufact_id = 427 
       AND dt.d_moy = 11 
GROUP  BY dt.d_year, 
          item.i_brand, 
          item.i_brand_id 
ORDER  BY dt.d_year, 
          sum_agg DESC, 
          brand_id;
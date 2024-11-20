WITH ss 
     AS (SELECT i_item_id, 
                Sum(ss_ext_sales_price) total_sales 
         FROM   `hadoop-benchmarking01`.TPCDS_1TB.store_sales, 
                `hadoop-benchmarking01`.TPCDS_1TB.date_dim, 
                `hadoop-benchmarking01`.TPCDS_1TB.customer_address, 
                `hadoop-benchmarking01`.TPCDS_1TB.item 
         WHERE  i_item_id IN (SELECT i_item_id 
                              FROM   `hadoop-benchmarking01`.TPCDS_1TB.item 
                              WHERE  i_category IN ( 'Jewelry' )) 
                AND ss_item_sk = i_item_sk 
                AND ss_sold_date_sk = d_date_sk 
                AND d_year = 1999 
                AND d_moy = 8 
                AND ss_addr_sk = ca_address_sk 
                AND ca_gmt_offset = -6 
         GROUP  BY i_item_id), 
     cs 
     AS (SELECT i_item_id, 
                Sum(cs_ext_sales_price) total_sales 
         FROM   `hadoop-benchmarking01`.TPCDS_1TB.catalog_sales, 
                `hadoop-benchmarking01`.TPCDS_1TB.date_dim, 
                `hadoop-benchmarking01`.TPCDS_1TB.customer_address, 
                `hadoop-benchmarking01`.TPCDS_1TB.item 
         WHERE  i_item_id IN (SELECT i_item_id 
                              FROM   `hadoop-benchmarking01`.TPCDS_1TB.item 
                              WHERE  i_category IN ( 'Jewelry' )) 
                AND cs_item_sk = i_item_sk 
                AND cs_sold_date_sk = d_date_sk 
                AND d_year = 1999 
                AND d_moy = 8 
                AND cs_bill_addr_sk = ca_address_sk 
                AND ca_gmt_offset = -6 
         GROUP  BY i_item_id), 
     ws 
     AS (SELECT i_item_id, 
                Sum(ws_ext_sales_price) total_sales 
         FROM   `hadoop-benchmarking01`.TPCDS_1TB.web_sales, 
                `hadoop-benchmarking01`.TPCDS_1TB.date_dim, 
                `hadoop-benchmarking01`.TPCDS_1TB.customer_address, 
                `hadoop-benchmarking01`.TPCDS_1TB.item 
         WHERE  i_item_id IN (SELECT i_item_id 
                              FROM   `hadoop-benchmarking01`.TPCDS_1TB.item 
                              WHERE  i_category IN ( 'Jewelry' )) 
                AND ws_item_sk = i_item_sk 
                AND ws_sold_date_sk = d_date_sk 
                AND d_year = 1999 
                AND d_moy = 8 
                AND ws_bill_addr_sk = ca_address_sk 
                AND ca_gmt_offset = -6 
         GROUP  BY i_item_id) 
SELECT i_item_id, 
               Sum(total_sales) total_sales 
FROM   (SELECT * 
        FROM   ss 
        UNION ALL 
        SELECT * 
        FROM   cs 
        UNION ALL 
        SELECT * 
        FROM   ws) tmp1 
GROUP  BY i_item_id 
ORDER  BY i_item_id, 
          total_sales;
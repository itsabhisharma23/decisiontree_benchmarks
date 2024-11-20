SELECT Sum(ss_net_profit) / Sum(ss_ext_sales_price)                 AS 
               gross_margin, 
               i_category, 
               i_class, 
               Grouping(i_category) + Grouping(i_class)                     AS 
               lochierarchy, 
               Rank() 
                 OVER ( 
                   partition BY Grouping(i_category)+Grouping(i_class), CASE 
                 WHEN Grouping( 
                 i_class) = 0 THEN i_category END 
                   ORDER BY Sum(ss_net_profit)/Sum(ss_ext_sales_price) ASC) AS 
               rank_within_parent 
FROM   `hadoop-benchmarking01`.TPCDS_1TB.store_sales, 
       `hadoop-benchmarking01`.TPCDS_1TB.date_dim d1, 
       `hadoop-benchmarking01`.TPCDS_1TB.item, 
       `hadoop-benchmarking01`.TPCDS_1TB.store 
WHERE  d1.d_year = 2000 
       AND d1.d_date_sk = ss_sold_date_sk 
       AND i_item_sk = ss_item_sk 
       AND s_store_sk = ss_store_sk 
       AND s_state IN ( 'TN', 'TN', 'TN', 'TN', 
                        'TN', 'TN', 'TN', 'TN' ) 
GROUP  BY rollup( i_category, i_class ) 
ORDER  BY lochierarchy DESC, 
          CASE 
            WHEN lochierarchy = 0 THEN i_category 
          END, 
          rank_within_parent;
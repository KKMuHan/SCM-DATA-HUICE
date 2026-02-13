WITH fx AS (SELECT
                company_shop_id        AS 'gx_shop_id',
                provider_nick_no       AS 'gx_nick_no',
                se.sid                 AS 'gx_sid',
                distributor_shop_id    AS 'fx_shop_id',
                fenxiao_nick_no        AS 'fx_nick_no',
                fx.sid                 AS 'fx_sid',
                fx.nick_name,
                fx.company_name,
                get_json_string(se.main_category_desc, '$.s_format')
                                       AS 'shop_category',
                request_source         AS 're_source',
                fx.client_id,
			    fx.role_mask,
                auth_mode
             FROM gx.dwd_sync_main_kd_supplier_distributor_relevance sup_dis_relevance
                  inner join gx.dwd_sync_main_kd_supplier_settle se
                      on sup_dis_relevance.company_shop_id = se.shop_id
                  inner join gx.dwd_sync_main_kd_supplier_settle fx
                      on sup_dis_relevance.fenxiao_nick_no = fx.nick_no
             WHERE se.merchant_status = 50
             AND se.is_delete = 0
             AND fx.merchant_status = 50
             AND fx.is_delete = 0
             AND cooperation_status = 70
	         [[ AND FIND_IN_SET(fx.fx_sid, REPLACE({{fx_sid}}, "'", '')) > 0 ]]
            ),
	 refoud AS (SELECT fx_nick_no,
                       COUNT(refoud_id)                                AS 'refoud_count',
                       SUM(return_main_total_amount)                   AS 'return_amount'                 
              FROM scm.dwd_refoud_order
              WHERE 1 = 1
			  [[AND created_time >= DATE_SUB(CURDATE(), INTERVAL {{stats_day}} DAY)]]
			  GROUP BY fx_nick_no
                ),
	 sku AS (SELECT gp.sys_shop_id                                     AS 'shop_id',
                    COUNT(gp.id)                                       AS 'sku_count',
			        COUNT(ext.id)                                      AS 'sku_count_gx',
                    COUNT(IF(barcode != '',  barcode, NULL))           AS 'barcode_sku'
           FROM gx.dwd_sync_scmgx_kd_goods_pool_product gp
				LEFT JOIN gx.dwd_sku_spu_ext_mv ext 
					ON gp.sys_shop_id = ext.sys_shop_id AND gp.id = ext.id
           GROUP BY gp.sys_shop_id    
			),
	 order_info AS (SELECT trade_order.gx_nick_no,
                           trade_order.fx_nick_no,
                           trade_count,
                           fx_receivable
                  FROM scm.dws_daily_trade_order trade_order
                      INNER JOIN fx ON fx.gx_nick_no = trade_order.gx_nick_no
                          AND fx.fx_nick_no = trade_order.fx_nick_no
                  WHERE  1 = 1
				        	[[AND trade_date >= date_sub(curdate(), INTERVAL {{stats_day}} day) ]]
                    ),
	gx_count AS (SELECT
                      fx.gx_sid,
                      fx.fx_sid,
                      SUM(order_info.trade_count)                 AS 'gf_order_num'
                  FROM fx
				      LEFT JOIN order_info ON fx.gx_nick_no = order_info.gx_nick_no
                        AND fx.fx_nick_no = order_info.fx_nick_no
				  WHERE 1 = 1
				  [[ AND fx.gx_sid IN ({{gx_sid}}) ]]
                  GROUP BY fx.gx_sid, fx.fx_sid),	
	 count AS (SELECT
                   fx.fx_sid,
					         SUM(order_info.trade_count)             AS 'order_num',
					 				 SUM(CASE WHEN fx.re_source = 1 THEN order_info.trade_count END)
                                                           AS 'order_num_1',
					 				 SUM(order_info.fx_receivable)			     AS `gmv_fx`,
                   COUNT(DISTINCT fx.gx_shop_id)           AS 'gx_count',
                   GROUP_CONCAT(fx.shop_category, '\n')    AS 'category'
              FROM fx
				          LEFT JOIN order_info ON fx.gx_nick_no = order_info.gx_nick_no
                      AND fx.fx_nick_no = order_info.fx_nick_no
              GROUP BY fx.fx_sid
     )	
SELECT DISTINCT
    fx.fx_sid                           AS `分销商卖家账号`,
	fx.fx_shop_id                       AS `分销商shopID`,
  	fx.nick_name                        AS `店铺名称`,
  	gx_count                            AS `合作的供销商数量`,
  	order_num                           AS `分销总单量`,
	IFNULL(gf_order_num, 0)             AS `该供销商下的单量`,  
  	order_num_1                         AS `分找供单量`,
    refoud_count                        AS `退款单量`,
  	return_amount                       AS `退款金额`,
    gmv_fx                              AS `分销商gmv`,
    CASE fx.client_id 
		    WHEN 1 THEN '跨境'
			WHEN 2 THEN '企业版'
			WHEN 3 THEN '旗舰版'
			WHEN 4 THEN 'ekb老账户'
			WHEN 5 THEN '发得快'
			WHEN 6 THEN '超群' 
			WHEN 8 THEN 'Y'
			WHEN 9 THEN 'SCM'
			WHEN 10 THEN '慧经营'
			WHEN 11 THEN '奇门'
			WHEN 12 THEN 'SCM-EKB'
	END                                 AS `分销业务线`,
	sku.sku_count                       AS `分销商货品数量`,
  	sku.sku_count_gx                    AS `供销侧货品数量`,
    sku.barcode_sku                     AS `有条码的货品数量`

FROM fx
    LEFT JOIN sku ON fx.fx_shop_id = sku.shop_id
  	LEFT JOIN count ON count.fx_sid = fx.fx_sid
	LEFT JOIN gx_count ON gx_count.fx_sid = fx.fx_sid
  	LEFT JOIN refoud ro
        ON fx.fx_nick_no = ro.fx_nick_no 
WHERE 1 = 1
[[ AND gx_count > {{gx_num_check}} ]]
ORDER BY `分找供单量` DESC;

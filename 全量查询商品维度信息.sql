WITH sku AS (SELECT sku.id,
                    sku.nick_no,
                    sku.sid,
                    goods_name,
				    brand_name,
				    cate_name,
				    sku.role_mask,
                    goods_id,
                    item_id,
                    barcode,
                    cost_price,
                    dis_price_lv1
             FROM gx.dwd_sku_spu_ext_mv sku
                      INNER JOIN gx.dwd_sync_main_kd_supplier_settle shop_info ON sku.sys_shop_id = shop_info.shop_id),
     order_info AS (SELECT o.gx_nick_no,
                           o.item_id,
                           sum(if(is_main = 1, 1, 0))  AS trade_count,
                           count(*)                    AS trade_order_count,
                           sum(goods_num)              AS item_count,
                           sum(trade_sub_paid) / 10000 AS gx_recive
                    FROM scm.dwd_trade_order o
                    WHERE 1 = 1
                      [[ AND o.trade_created >= date_sub(curdate(), INTERVAL {{stats_day}} DAY)]]
                      AND trade_status IN (30, 80, 100)
                    GROUP BY o.gx_nick_no, o.item_id)
SELECT DISTINCT
    sku.barcode                                 AS `条码`,
    sku.sid                                     AS `商家sid`,
    sku.goods_id                                AS `货品id`,
    sku.goods_name                              AS `商品名称`,
	IFNULL(sku.brand_name, '无')                AS `品牌`,
	sku.cate_name                               AS `商品类目`,
	CASE sku.role_mask
		WHEN 1 THEN '分销商'
		WHEN 2 THEN '供销商'
		WHEN 3 THEN '供+分'
	END                                         AS `供分销角色`,
    IFNULL(order_info.trade_count, 0)           AS `主单数`,
    IFNULL(order_info.trade_order_count, 0)     AS `子单数`,
    IFNULL(order_info.item_count, 0)            AS `销售数量`,
    IFNULL(sku.cost_price, 0)                   AS `成本价`,
    sku.dis_price_lv1                           AS `一级分销价`
FROM sku
         LEFT JOIN order_info ON order_info.gx_nick_no = sku.nick_no AND order_info.item_id = sku.item_id
WHERE 1 = 1
[[AND sa.barcode in ({{barcode}})]]
[[AND CASE 
	      WHEN {{check_barcode}} = 1 THEN barcode  = ''
	      WHEN {{check_barcode}} = 0 THEN barcode != ''
      END]]
AND order_info.trade_count IS NOT NULL
ORDER BY `主单数` DESC
LIMIT 2000;

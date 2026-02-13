WITH refoud AS (SELECT gx_nick_no,
                       fx_nick_no,
                       refoud_id,
                       refund_type,
                       return_main_total_amount        AS 'return_amount',
                       created_time
                FROM scm.dwd_refoud_order
                WHERE created_time >= DATE_SUB(CURDATE(), INTERVAL 10 DAY)
                )
SELECT
    fx_sid,
    o.fx_nick_no,
    o.gx_nick_no,
    trade_count                AS `订单总量`,
    COUNT(ro.refoud_id)        AS `退款总单量`,
    SUM(ro.return_amount)      AS `退款单售后总金额`
FROM scm.dws_daily_trade_order o
    LEFT JOIN refoud ro
        ON o.fx_nick_no = ro.fx_nick_no AND o.gx_nick_no = ro.gx_nick_no
WHERE trade_date >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
AND fx_sid = 'chonglou2'
GROUP BY fx_sid, o.fx_nick_no, o.gx_nick_no, trade_count;

WITH ext AS (
    SELECT
        erp.sid,
        erp.barcode,
        erp.sku_id,
        o.avg_one_cost_price,
        IFNULL(IF(ss.ratio > 0, ss.ratio, ss.global_ratio), 0) AS ratio,
        IFNULL(o.one_trade_count, 0)                           AS one_trade_count,
        IFNULL(o.one_trade_count * IFNULL(IF(ss.ratio > 0, ss.ratio, ss.global_ratio), 0) / 100,0)
                                                               AS one_trade_count_ratio
    FROM dw.dwd_erp_sku_all erp
    LEFT JOIN dw.dws_erp_smart_fx_sku_info ss
               ON erp.sku_id = ss.sku_id AND erp.sid = ss.sid
    LEFT JOIN dw.dws_erp_sales_trade o
               ON erp.sid_ascii = o.sid_ascii AND erp.sid = o.sid AND erp.sku_id = o.sku_id
)
WITH barcode_info AS (
    SELECT
	    barcode,
		one_trade_count_ratio,
	    one_trade_count            AS b_one_trade_count
    FROM dw.dws_erp_barcode_analysis
	WHERE 1 = 1
	LIMIT 1000
)
SELECT
    st.sid                          AS `卖家账号`,
    ext.barcode                     AS `条码`,
    ext.sku_id                      AS `商家编码`,
    st.avg_one_cost_price           AS `平均成本价-一单一品`,
    st.avg_post_price               AS `平均邮费`,
    ext.ratio                       AS `切换比例`,
    st.one_trade_count              AS `一单一品单量`,
    bi.one_trade_count_ratio        AS `预计切换单量`,
    st.sid_from                     AS `产品线`
FROM dw.dws_erp_sales_trade  st
    LEFT JOIN ext ON ext.sid = st.sid AND ext.sku_id = st.sku_id
    LEFT JOIN barcode_info bi on bi.barcode = ext.barcode
where true
ORDER BY bi.b_one_trade_count DESC, st.avg_one_cost_price DESC
LIMIT 1000;


SELECT
    COUNT(id),
    COUNT(DISTINCT id),
    COUNT(IF(barcode != '', id, NULL))
FROM gx.dwd_sku_spu_ext_mv;

SELECT
    COUNT(id),
    COUNT(goods_id),
    COUNT(DISTINCT goods_id)
FROM gx.dwd_sync_scmgx_kd_goods_pool_product;

SELECT DISTINCT
    COUNT(goods_id),
    COUNT(DISTINCT goods_id),
    COUNT(IF(sku.barcode != '', sku.barcode, NULL))
FROM gx.dwd_sku_spu_ext_mv sku
WHERE sid = 'gylzyzh5';


SELECT * FROM gx.dwd_sku_spu_ext_mv WHERE item_id = 37873672;
SELECT * FROM gx.dwd_sync_scmgx_kd_goods_pool_product WHERE sys_shop_id = 303418;


SELECT
    COUNT(*)
FROM scm.dwd_trade_order
WHERE is_main = 1 AND trade_created >= date_sub(curdate(), interval 30 day);

select
	sum(trade_count) '订单量'
from scm.dws_daily_trade_order
where trade_date >= date_sub(curdate(), interval 30 day);

SELECT SUM(trade_count_30) FROM dw.dws_erp_sales_trade;

SELECT * FROM gx.dwd_sync_main_kd_supplier_settle WHERE company_name = '罗蒙维塔利专卖店';

SELECT DISTINCT
    item_id,
    goods_name,
    goods_num,
    fx_sid,
    gx_sid,
    goods_spu_id
FROM scm.dwd_trade_order
WHERE
    trade_created >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
    AND fx_sid = '13958480448';

SELECT
    se.sid                                       AS `供销商卖家账号`,
    rel.company_shop_id                          AS `供销商shopID`,
    rel.provider_nick_no                         AS `供销商编码`,
    COUNT(DISTINCT rel.distributor_shop_id)      AS `分销商总数`,
    COUNT(sku.goods_id)                          AS `供销商货品数量`,
    COUNT(IF(sku.barcode != '', sku.barcode, NULL))
                                                 AS `有条码的货品数量`
FROM gx.dwd_sync_main_kd_supplier_distributor_relevance rel
LEFT JOIN gx.dwd_sync_main_kd_supplier_settle se
         ON rel.company_shop_id = se.shop_id
     LEFT JOIN gx.dwd_sku_spu_ext_mv sku
         ON sku.sid = se.sid
WHERE 1 = 1
   AND se.sid IN ('ymgyl77')
GROUP BY se.sid, rel.company_shop_id, rel.provider_nick_no;

WITH sku AS (SELECT
                 item_id,
                 goods_id,
                 barcode

             FROM gx.dwd_sku_spu_ext_mv
             WHERE sys_shop_id = 2068
                  )

SELECT
    o.trade_id              AS `中间件订单ID`,
    o.goods_no              AS `货品编码`,
    o.fx_sid                AS `分销卖家账号`,
    o.gx_sid                AS `供销商卖家账号`,
    o.fx_shop_id,
    o.gx_nick_no,
    o.goods_name            AS `货品名称`,
    sku.barcode             AS `条码`,
    CASE rel_gx.request_source
        WHEN 1 THEN '分找供'
        WHEN 2 THEN '供找分'
        WHEN 3 THEN '互为合作'
    END                     AS `申请来源`,
    trade_created           AS `订单创建时间`

FROM scm.dwd_trade_order o
    LEFT JOIN sku ON sku.item_id = o.item_id
    INNER JOIN gx.dwd_sync_main_kd_supplier_distributor_relevance rel_gx
        ON rel_gx.provider_nick_no = o.gx_nick_no
    INNER JOIN gx.dwd_sync_main_kd_supplier_distributor_relevance rel_fx
        ON rel_fx.distributor_shop_id = o.fx_shop_id
WHERE trade_created >= DATE_SUB(CURDATE(), interval 1 day)
LIMIT 2000;

WITH fx AS (SELECT fx_sid                 AS fx_sid,
                   SUM(trade_order_count) AS orderCount,
                   SUM(goods_amount)      AS goodsAmount,
                   SUM(post_amount)       AS postAmount

            FROM scm.dws_daily_trade_order o
            WHERE o.trade_date >= date_sub(curdate(), interval 30 day)
            GROUP BY fx_sid
            HAVING orderCount >= 1000
    )
SELECT
    fx.fx_sid                 AS `分销商ID`,
    fx.orderCount             AS `30天子单总量`,
    fx.goodsAmount            AS `30天货品总金额`,
    fx.postAmount             AS `30天总邮费`,
    SUM(ssa.sale_sku_num_30)  AS `30销售总SKU数-ERP`,
    SUM(ssa.sale_gmv_30)      AS `30天总GMV-ERP`
FROM fx
LEFT JOIN dw.dwd_erp_sku_sale_all ssa
    ON ssa.sid = fx.fx_sid
    AND ssa.date = date_sub(curdate(), interval 1 day)
    GROUP BY fx.fx_sid, fx.orderCount, fx.goodsAmount, fx.postAmount;

SELECT COUNT(*) as zero_num_but_has_cost
FROM dw.dwd_ultimate_erp_sales_trade
WHERE o_num = 0 AND goods_cost != 0;

SELECT
    SUM(stss.smart_trade_count)         AS `智能采购30天订单量`,
    SUM(st.trade_count_30)              AS `小鲸月总订单量`
FROM
    gx.dws_gx_sku_top_stats_summary stss
    LEFT JOIN
        dw.dws_erp_sales_trade st ON stss.supplier_sid = st.sid and stss.supplier_sku_id = st.sku_id;

SELECT
    COUNT(*) AS 符合条件的记录数
FROM dw.dws_erp_sales_trade st
WHERE
    st.avg_cost_price > 1000
    OR st.avg_price > 1000;

SELECT
    search_word AS `搜索词` ,
    COUNT(*) AS `搜索次数`
FROM kd_main.kd_search_history
WHERE 1 = 1
AND search_time >= date_sub(curdate(), interval 5 day)
GROUP BY search_word
ORDER BY `搜索次数` DESC
LIMIT 20;

SELECT
    st.sid              AS `商家ID`,
    sku_id,
    st.avg_cost_price   AS `平均成本价`,
    st.avg_price        AS `平均售价`

FROM dw.dws_erp_sales_trade st
WHERE st.avg_cost_price > 1000
    OR st.avg_price > 1000
ORDER BY st.avg_cost_price DESC
LIMIT 500;

select bs.barcode                              as `条码`,
       sku.sid                                 as `卖家账号`,
       bs.sku_name                             as `sku名称`,
       bs.spu_name                             as `spu名称`,
       bs.sales_gmv_30                         as `近30天GMV`,
       bs.sales_sku_num_30                     as `近30天销量`,
	   ba.one_trade_count                      as '近30天一单一货的单量',
       CONCAT_WS(',', IF((sku.business_model & 1) > 0, '普通分销-分销商', NULL),
                 IF((sku.business_model & 2) > 0, '工厂代发-分销商', NULL),
                 IF((sku.business_model & 4) > 0, '工厂代发-供销商', NULL),
                 IF((sku.business_model & 8) > 0, '普通分销-供销商', NULL),
				 IF((sku.business_model &16)>0,'供销商',NULL),
				 IF((sku.business_model &32)>0,'分销商',NULL))
				                               as '业务模式',
       bs.same_barcode_sku_num                 as `erp侧平台内相同条码的数量`,
       bs.same_barcode_sid_num                 as `erp侧有该条码的商家数量`
    from dw.dws_barcode_sale_info bs
		left join dws_erp_barcode_analysis ba ON ba.barcode = bs.barcode
        left join dw.dws_erp_smart_fx_sku_info sku ON sku.st_barcode = bs.barcode
WHERE 1=1
[[and barcode in ({{baroce}})]]
[[and spu_name MATCH_ALL {{spu_name}} ]]
[[and sales_gmv_30 >= {{sales_gmv_30}}]]
[[and sales_sku_num_30 >= {{sales_sku_num_30}}]]
[[and same_barcode_sku_num >= {{same_barcode_sku_num}}]]
[[and same_barcode_sid_num >= {{same_barcode_sid_num}}]]
[[AND (sku.business_model & (
     array_sum([{{business_model}}])
)) = (
     array_sum([{{business_model}}])
)]]
order by ba.one_trade_count desc, same_barcode_sku_num desc
limit 1000;

SELECT
    ss.sid                                     AS `卖家id`,
    IF(ss.global_open = 1, '是', '否')          AS `是否开启全局配置`,
    CAST(ss.global_ratio AS INT)               AS `全局配置比例`,
    IF(ss.fx_open = 1, '是', '否')         AS `是否开启小鲸智能采购`


    FROM dw.dws_erp_sid_setting ss
--    LEFT JOIN dw.dws_erp_smart_fx_sku_info sfs
--        ON sfs.sid = ss.sid AND sfs.st_barcode != ''

LIMIT 2000;

SELECT
    sid                 AS'卖家账号',
    business_model      AS '业务模式',
    spu_name            AS 'spu商品名称',
    CONCAT_WS(', ',
        IF(business_model & 1 > 0, '普通分销-分销商', NULL),
        IF(business_model & 2 > 0, '工厂代发-分销商', NULL),
        IF(business_model & 4 > 0, '工厂代发-供销商', NULL),
        IF(business_model & 8 > 0, '普通分销-供销商', NULL),
    	IF(business_model & 16 > 0,'供销商',NULL),
		IF(business_model & 32 > 0,'分销商',NULL))
   AS label

FROM dws_erp_smart_fx_sku_info
ORDER BY one_order_count_30 DESC;

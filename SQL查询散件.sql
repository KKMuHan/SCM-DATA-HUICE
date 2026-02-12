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

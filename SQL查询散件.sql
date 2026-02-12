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



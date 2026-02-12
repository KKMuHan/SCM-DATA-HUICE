SELECT
    scm.trade_count          AS `总订单量`,
    gx_stats. `小鲸30天总订单量`,
	scm.fx_sid_count         AS `分销商总数`,
	scm.gx_sid_count         AS `供销商总数`,
	scm.delivery_count       AS `日发货订单总量`,
	scm.fx_cancel            AS `分销取消订单总量`,
	scm.gx_cancel            AS `供销取消订单总量`,
	scm.refund_count         AS `日退款订单总量`,
    gx_stats.`智能采购30天订单量`
FROM
    (
    SELECT
        SUM(trade_count)              AS trade_count,
		COUNT(DISTINCT fx_sid)        AS fx_sid_count,
		COUNT(DISTINCT gx_sid)        AS gx_sid_count,
		SUM(delivery_trade_count)     AS delivery_count,
		SUM(fx_cancel_trade_count)    AS fx_cancel,
		SUM(gx_cancel_trade_count)    AS gx_cancel,
		SUM(refund_count)             AS refund_count
    FROM scm.dws_daily_trade_order
    WHERE 1 = 1
	[[AND trade_date >= DATE_SUB(CURDATE(), INTERVAL {{stats_day}} DAY)]]
    ) AS scm
CROSS JOIN
    (
    SELECT
        SUM(stss.smart_trade_count)   AS `智能采购30天订单量`,
        SUM(st.trade_count_30)        AS `小鲸30天总订单量`
    FROM
        gx.dws_gx_sku_top_stats_summary stss
    LEFT JOIN
        dw.dws_erp_sales_trade st ON stss.supplier_sid = st.sid AND stss.supplier_sku_id = st.sku_id
    ) AS gx_stats;

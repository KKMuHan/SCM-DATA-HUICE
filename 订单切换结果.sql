WITH log AS (
    SELECT sid,
           trade_id,
           GROUP_CONCAT(message, '\n=>' ORDER BY create_date) AS message
    FROM ods_x_erp_trade_order_log
    GROUP BY sid, trade_id

    UNION ALL
    SELECT sid,
           trade_id,
           GROUP_CONCAT(message, '\n=>' ORDER BY created) AS message
    FROM ods_enterprise_erp_trade_order_log
    GROUP BY sid, trade_id
    UNION ALL
    SELECT sid,
           trade_id,
           GROUP_CONCAT(message, '\n=>' ORDER BY created) AS message
    FROM ods_ultimate_erp_trade_order_log
    GROUP BY sid, trade_id
)
SELECT tr.sid                    AS `分销商卖家账号`,
       tr.spec_barcode           AS `货品条码`,
       tr.sku_id                 AS `SKU ID`,
       tr.trade_no               AS `分销商系统单号`,
       IFNULL(fos.receivable, 0) AS `商家应收金额`,
       tr.sys_cost               AS `货品单件成本`,
       tr.num                    AS `货品数量`,
       tr.sys_postage            AS `系统订单邮费`,
       tr.sys_sum_cost           AS `系统总成本`,
       tr.fx_cost                AS `分销单件成本`,
       tr.fx_postage             AS `分销邮费`,
       tr.fx_sum_cost            AS `分销总成本`,
       IF(tr.result_type = 0, '成功', '失败')
                                 AS `是否切换成功`,
       tr.result                 AS `切换结果`,

       CASE
           WHEN tr.sid_from = 2 THEN '企业版'
           WHEN tr.sid_from = 3 THEN '旗舰版'
           ELSE 'Y'
       END                       AS '产品线',
       tr.created                AS `校验时间`,
       IFNULL(CASE
                  WHEN fos.sid_from = 2 THEN
                      CASE
                          WHEN fos.trade_status = 30 AND warehouse_id > 0 THEN '已选仓待审核'
                          WHEN fos.trade_status = 55 AND warehouse_id > 0 AND status = 0 THEN '已审核未推单'
                          WHEN status = 1 THEN '推单失败'
                          WHEN status = 2 THEN '推单成功'
                      END
                  WHEN fos.sid_from = 3 THEN
                      CASE
                          WHEN fos.trade_status = 30 AND warehouse_id > 0 THEN '已选仓待审核'
                          WHEN warehouse_id > 0 AND status = 61 THEN '已审核未推单'
                          WHEN warehouse_id > 0 AND status = 65 THEN '推单失败'
                          WHEN status = 53 THEN '推单成功'
                      END
                  WHEN fos.sid_from = 8 THEN
                      CASE
                          WHEN fos.trade_status = 30 THEN '已选仓待审核'
                          WHEN fos.trade_status = 51 THEN '已审核未推单'
                          WHEN fos.trade_status = 53 THEN '推单失败'
                          WHEN fos.trade_status = 55 THEN '推单成功'
                      END
                  ELSE ''
              END, '')
                                 AS `订单状态`,
       IFNULL(log.message, '')   AS `订单日志信息`

FROM dw.dwd_erp_xjyx_switch_trade_record tr
     LEFT JOIN dw.dwd_erp_fail_order_status fos
               ON fos.sid = tr.sid AND fos.trade_no = tr.trade_no AND tr.result_type = 0
     LEFT JOIN log ON log.sid = fos.sid AND log.trade_id = fos.trade_id	 
WHERE tr.sid not in ('qybhb', 'qybhz', 'qybzx', 'qybsl', 'qybhs', 'qybzj', 'qybfj', 'qybgf', 'qybsy', 'qybka',
                     'qybsq', 'qybdx', 'qybgfd', 'yanshi1', 'yanshi', 'yanshiqyb1', 'yanshiqjb1', 'yanshijl',
                     'chentingting', 'prodgray5', 'demoab','qytest','demoab')
AND spec_barcode != ''
[[AND tr.sid IN ({{sid}})]]
[[AND tr.sid NOT IN ({{no_sid}})]]
[[AND tr.spec_barcode IN ({{barcode}}) ]]
[[AND tr.trade_no IN ({{trade_no}}) ]]
[[AND result_type = {{result_check}}]]
[[AND tr.sid_from IN ({{sid_from}}) ]]
[[AND tr.created >= {{start_date}} ]] 
[[ AND tr.created < {{end_date}}]]
HAVING 1=1
[[AND 订单状态 IN ({{trade_status}}) ]];

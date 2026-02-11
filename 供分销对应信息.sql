WITH merchant_info AS (SELECT company_shop_id     AS 'gx_shop_id',
                              provider_nick_no    AS 'gx_nick_no',
                              se.sid              AS 'gx_sid',
                              distributor_shop_id AS 'fx_shop_id',
                              fenxiao_nick_no     AS 'fx_nick_no',
							  fx.sid              AS `fx_sid`,
							  fenxiao_source      AS `fx_source`,
							  request_source      AS `re_source`,
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
                           [[AND provider_nick_no in ({{nick_no}})]]
                           [[AND company_shop_id in ({{shop_id}})]]
                           [[AND se.sid in ({{sid}})]]
						   [[AND fx.sid in ({{fx_sid}})]]
   						   [[AND auth_mode in ({{auth_mode}})]]
						 
),           
     fx_shop_info AS (SELECT shop_id,
                             nick_no,
                             sid,
                             company_name,
                             client_id,
                             get_json_string(main_category_desc, '$.s_format') AS 'main_cate'
                      FROM gx.dwd_sync_main_kd_supplier_settle se
                               inner join merchant_info on se.shop_id = merchant_info.fx_shop_id
                      WHERE se.merchant_status = 50
                        AND se.is_delete = 0)
SELECT DISTINCT 
	merchant_info.gx_shop_id         AS '供销商shop_id',
    merchant_info.gx_nick_no         AS '供销商编码',
    merchant_info.gx_sid             AS `供销商卖家账号`,
    merchant_info.fx_shop_id         AS '分销商shop_id',
    merchant_info.fx_nick_no         AS '分销商编码',
    merchant_info.fx_sid             AS `分销商卖家账号`,
	IF(merchant_info.fx_source = 1, '线下', '线上')  
	                                 AS `分销商来源`,
    CASE merchant_info.re_source
        WHEN 1 THEN '分找供'
        WHEN 2 THEN '供找分'
        WHEN 3 THEN '互为合作'
    END                              AS `申请来源`,
	CASE 
        WHEN pd.distribution_shop_id IS NOT NULL THEN '是' 
        ELSE '否' 
    END                              AS '是否为私域',
    CASE merchant_info.auth_mode
        WHEN 1 THEN '普通模式'
        WHEN 2 THEN '急速打单模式'
        WHEN 3 THEN '普通模式 + 急速打单模式'
        WHEN 4 THEN '采购模式'
        WHEN 5 THEN '普通模式 + 采购'
        WHEN 6 THEN '采购 + 急速'
        WHEN 7 THEN '普通 + 急速 + 采购'
    ELSE '未知' 
	END                              AS "业务模式",
    CASE fx_shop_info.client_id
        WHEN 1 THEN '跨境'
        WHEN 2 THEN '企业版'
        WHEN 3 THEN '旗舰版'
        WHEN 4 THEN 'ekb老账户'
        WHEN 5 THEN '发得快'
        WHEN 6 THEN '超群'
        WHEN 8 THEN 'erpx'
        WHEN 9 THEN 'scm'
        WHEN 10 THEN '慧经营'
        WHEN 11 THEN 'scmqm'
        WHEN 12 THEN 'scmekb'
        ELSE '未知' 
		END                          AS "分销商产品线",
    fx_shop_info.company_name        AS '分销商公司名称',
    fx_shop_info.main_cate           AS '分销商主营类目'
FROM merchant_info
     LEFT JOIN gx.kd_gx_user_private_domain_relation pd 
         ON merchant_info.gx_shop_id = pd.supplier_shop_id AND merchant_info.fx_shop_id = pd.distribution_shop_id
     LEFT JOIN fx_shop_info ON merchant_info.fx_nick_no = fx_shop_info.nick_no
HAVING 1 = 1
[[AND `是否为私域` = {{private_check}}]]
[[AND `分销商来源` = {{source}}]]

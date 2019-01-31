package com.tuandai.ceo.enums;

public enum  PrepareSql {
    //撮合业务余额  贷后
    BUSINESS_BALANCE_SQL1(" SELECT SUM(\n" +
            "     CASE t2.plan_item_type\n" +
            "     WHEN 10\n" +
            "     THEN IFNULL(t2.fact_amount, 0)\n" +
            "     ELSE 0 END\n" +
            " ) factAmount FROM `hongte_alms`.tb_repayment_biz_plan_list_detail t2"),
    //撮合业务余额  贷前
    BUSINESS_BALANCE_SQL2("SELECT SUM(t.principal) AS actual_total FROM `tb_yidian_repayment_actual` t"),
    //客户撮合情况
    APPLY_OUT_PUT_SQL("-- 申请\n" +
            "SELECT t.cnt,t.date_time,t.status_type FROM (SELECT COUNT(DISTINCT business_id) cnt,DATE_FORMAT(apply_dt,'%Y-%m') date_time,'1' AS status_type\n" +
            "FROM \n" +
            "(SELECT a.`business_id`,MIN(DATE_FORMAT(CASE WHEN a.business_type IN ('房速贷标准件','房速贷非标准件','T600') AND b.`reserve_1` IN ('H0530','B0350','F0530') AND b.reserve_6 = 1 THEN b.create_time \n" +
            "                  WHEN a.business_type NOT IN ('房速贷标准件','房速贷非标准件','T600') AND (b.reserve_6 = 1 OR b.reserve_6 =4) THEN b.create_time ELSE NULL END,'%Y%m%d')\n" +
            "                  ) AS apply_dt\n" +
            "FROM tb_business a\n" +
            "JOIN tb_car_business_log b ON a.`business_id` = b.`car_business_id` \n" +
            "WHERE a.business_type IN ('房速贷标准件','房速贷非标准件','T600') \n" +
            "GROUP BY a.`business_id`\n" +
            ") tt\n" +
            "GROUP BY DATE_FORMAT(apply_dt,'%Y%m')\n" +
            "ORDER BY DATE_FORMAT(apply_dt,'%Y%m') DESC\n" +
            "LIMIT 6\n" +
            ") t\n" +
            "UNION ALL\n" +
            "-- 撮合\n" +
            "SELECT COUNT(DISTINCT t.business_id) AS cnt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 0 MONTH),'%Y-%m') date_time,'2' AS status_type FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` JOIN tb_business tb ON t.business_id=tb.business_id WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 0 MONTH),'%Y-%m')\n" +
            "AND (tb.business_type LIKE '%房速贷%' OR tb.`business_type` IN ('T100','T110','T251','T170'))\n" +
            "UNION ALL\n" +
            "SELECT COUNT(DISTINCT t.business_id) AS cnt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -1 MONTH),'%Y-%m') date_time,'2' AS status_type FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` JOIN tb_business tb ON t.business_id=tb.business_id WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -1 MONTH),'%Y-%m')\n" +
            "AND (tb.business_type LIKE '%房速贷%' OR tb.`business_type` IN ('T100','T110','T251','T170'))\n" +
            "UNION ALL\n" +
            "SELECT COUNT(DISTINCT t.business_id) AS cnt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -2 MONTH),'%Y-%m') date_time,'2' AS status_type FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` JOIN tb_business tb ON t.business_id=tb.business_id WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -2 MONTH),'%Y-%m')\n" +
            "AND (tb.business_type LIKE '%房速贷%' OR tb.`business_type` IN ('T100','T110','T251','T170'))\n" +
            "UNION ALL\n" +
            "SELECT COUNT(DISTINCT t.business_id) AS cnt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -3 MONTH),'%Y-%m') date_time,'2' AS status_type FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` JOIN tb_business tb ON t.business_id=tb.business_id WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -3 MONTH),'%Y-%m')\n" +
            "AND (tb.business_type LIKE '%房速贷%' OR tb.`business_type` IN ('T100','T110','T251','T170'))\n" +
            "UNION ALL\n" +
            "SELECT COUNT(DISTINCT t.business_id) AS cnt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -4 MONTH),'%Y-%m') date_time,'2' AS status_type FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` JOIN tb_business tb ON t.business_id=tb.business_id WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -4 MONTH),'%Y-%m')\n" +
            "AND (tb.business_type LIKE '%房速贷%' OR tb.`business_type` IN ('T100','T110','T251','T170'))\n" +
            "UNION ALL\n" +
            "SELECT COUNT(DISTINCT t.business_id) AS cnt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -5 MONTH),'%Y-%m') date_time,'2' AS status_type FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` JOIN tb_business tb ON t.business_id=tb.business_id WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -5 MONTH),'%Y-%m')\n" +
            "AND (tb.business_type LIKE '%房速贷%' OR tb.`business_type` IN ('T100','T110','T251','T170'))"),
    BORROW_LIMIT_SQL("SELECT  tt.PeriodMonth AS borrow_limit,COUNT(1) cnt FROM (SELECT ti.`PeriodMonth` FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "UNION ALL\n" +
            "SELECT ti.`PeriodMonth` FROM tb_issue_business t JOIN tb_issue ti ON  t.`issue_id`=ti.`IssueId` JOIN tb_yidian_business_information tbi ON t.`business_id`=tbi.`business_id`\n" +
            "WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            ") tt\n" +
            "GROUP BY tt.PeriodMonth"),
    CENTRE_MAP_SQL("SELECT SUM(tt.sum_amt) AS total_sum_amt,province FROM (\n" +
            "SELECT IFNULL(SUM(t.`full_borrow_money`),0) AS sum_amt,tc.`province` FROM  tb_fsd_customer tc JOIN tb_issue_business t ON tc.`business_id`=t.`business_id` JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND t.`business_id` IN (SELECT tb.`business_id` FROM tb_business tb WHERE tb.`business_type` LIKE '%房速贷%' OR tb.`business_type`='T251')\n" +
            "GROUP BY tc.`province` \n" +
            "UNION ALL\n" +
            "SELECT IFNULL(SUM(t.`full_borrow_money`),0) AS sum_amt,tc.`id_card_sheng` AS province FROM tb_car_personal tc JOIN tb_issue_business t ON tc.`car_business_id`=t.`business_id` JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND t.`business_id` IN (SELECT tb.`business_id` FROM tb_business tb WHERE tb.`business_type` LIKE '%车易贷%')\n" +
            "GROUP BY tc.`id_card_sheng`\n" +
            "UNION ALL\n" +
            "SELECT IFNULL(SUM(t.`full_borrow_money`),0) AS sum_amt,tt.`live_province` AS province  FROM tb_yidian_car_personal_information tt JOIN tb_issue_business t ON tt.`business_id`=t.`business_id` JOIN tb_issue ti ON  t.`issue_id`=ti.`IssueId` JOIN tb_yidian_business_information tbi ON t.`business_id`=tbi.`business_id`\n" +
            "WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "GROUP BY tt.`account_province`\n" +
            ") tt WHERE tt.province IS NOT NULL\n" +
            "GROUP BY province;"),
    //各月撮合金额趋势
    MONTH_AMOUNT_SQL("SELECT IFNULL(SUM(t.`full_borrow_money`),0) AS sum_amt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -5 MONTH),'%Y-%m') date_time,'house' AS business_type FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -5 MONTH),'%Y-%m')\n" +
            "AND t.`business_id` IN (SELECT tb.`business_id` FROM tb_business tb WHERE tb.`business_type` LIKE '%房速贷%' OR tb.`business_type` IN ('T100','T110'))\n" +
            "UNION\n" +
            "SELECT IFNULL(SUM(t.`full_borrow_money`),0) AS sum_amt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -4 MONTH),'%Y-%m') date_time,'house' AS business_type FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -4 MONTH),'%Y-%m')\n" +
            "AND t.`business_id` IN (SELECT tb.`business_id` FROM tb_business tb WHERE tb.`business_type` LIKE '%房速贷%' OR tb.`business_type` IN ('T100','T110'))\n" +
            "UNION\n" +
            "SELECT IFNULL(SUM(t.`full_borrow_money`),0) AS sum_amt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -3 MONTH),'%Y-%m') date_time,'house' AS business_type FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -3 MONTH),'%Y-%m')\n" +
            "AND t.`business_id` IN (SELECT tb.`business_id` FROM tb_business tb WHERE tb.`business_type` LIKE '%房速贷%' OR tb.`business_type` IN ('T100','T110'))\n" +
            "UNION\n" +
            "SELECT IFNULL(SUM(t.`full_borrow_money`),0) AS sum_amt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -2 MONTH),'%Y-%m') date_time,'house' AS business_type FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -2 MONTH),'%Y-%m')\n" +
            "AND t.`business_id` IN (SELECT tb.`business_id` FROM tb_business tb WHERE tb.`business_type` LIKE '%房速贷%' OR tb.`business_type` IN ('T100','T110'))\n" +
            "UNION\n" +
            "SELECT IFNULL(SUM(t.`full_borrow_money`),0) AS sum_amt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -1 MONTH),'%Y-%m') date_time,'house' AS business_type FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -1 MONTH),'%Y-%m')\n" +
            "AND t.`business_id` IN (SELECT tb.`business_id` FROM tb_business tb WHERE tb.`business_type` LIKE '%房速贷%' OR tb.`business_type` IN ('T100','T110'))\n" +
            "UNION\n" +
            "SELECT IFNULL(SUM(t.`full_borrow_money`),0) AS sum_amt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 0 MONTH),'%Y-%m') date_time,'house' AS business_type FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 0 MONTH),'%Y-%m')\n" +
            "AND t.`business_id` IN (SELECT tb.`business_id` FROM tb_business tb WHERE tb.`business_type` LIKE '%房速贷%' OR tb.`business_type` IN ('T100','T110'))\n" +
            "UNION\n" +
            "-- 车易贷\n" +
            "SELECT SUM(t.sum_amt) AS sum_amt,date_time,'car' AS business_type FROM (\n" +
            "SELECT IFNULL(SUM(t.`full_borrow_money`),0) AS sum_amt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -5 MONTH),'%Y-%m') date_time,'car_easy'  FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -5 MONTH),'%Y-%m')\n" +
            "AND t.`business_id` IN (SELECT tb.`business_id` FROM tb_business tb WHERE tb.`business_type` LIKE '%车易贷%')\n" +
            "UNION\n" +
            "SELECT IFNULL(SUM(t.`full_borrow_money`),0) AS sum_amt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -4 MONTH),'%Y-%m') date_time,'car_easy'  FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -4 MONTH),'%Y-%m')\n" +
            "AND t.`business_id` IN (SELECT tb.`business_id` FROM tb_business tb WHERE tb.`business_type` LIKE '%车易贷%')\n" +
            "UNION \n" +
            "SELECT IFNULL(SUM(t.`full_borrow_money`),0) AS sum_amt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -3 MONTH),'%Y-%m') date_time,'car_easy'  FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -3 MONTH),'%Y-%m')\n" +
            "AND t.`business_id` IN (SELECT tb.`business_id` FROM tb_business tb WHERE tb.`business_type` LIKE '%车易贷%')\n" +
            "UNION\n" +
            "SELECT IFNULL(SUM(t.`full_borrow_money`),0) AS sum_amt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -2 MONTH),'%Y-%m') date_time,'car_easy'  FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -2 MONTH),'%Y-%m')\n" +
            "AND t.`business_id` IN (SELECT tb.`business_id` FROM tb_business tb WHERE tb.`business_type` LIKE '%车易贷%')\n" +
            "UNION\n" +
            "SELECT IFNULL(SUM(t.`full_borrow_money`),0) AS sum_amt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -1 MONTH),'%Y-%m') date_time,'car_easy'  FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -1 MONTH),'%Y-%m')\n" +
            "AND t.`business_id` IN (SELECT tb.`business_id` FROM tb_business tb WHERE tb.`business_type` LIKE '%车易贷%')\n" +
            "UNION\n" +
            "SELECT IFNULL(SUM(t.`full_borrow_money`),0) AS sum_amt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 0 MONTH),'%Y-%m') date_time,'car_easy'  FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 0 MONTH),'%Y-%m')\n" +
            "AND t.`business_id` IN (SELECT tb.`business_id` FROM tb_business tb WHERE tb.`business_type` LIKE '%车易贷%')\n" +
            "-- 一点车贷\n" +
            "UNION\n" +
            "SELECT IFNULL(SUM(ti.`Amount`),0) AS sum_amt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -5 MONTH),'%Y-%m') date_time,'yidian_car'  FROM tb_issue_business t JOIN tb_issue ti ON  t.`issue_id`=ti.`IssueId` \n" +
            "WHERE ti.queryFullsuccessDate IS NOT NULL  AND ti.`ProjectFrom`=1\n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -5 MONTH),'%Y-%m')\n" +
            "UNION\n" +
            "SELECT IFNULL(SUM(ti.`Amount`),0) AS sum_amt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -4 MONTH),'%Y-%m') date_time,'yidian_car'  FROM tb_issue_business t JOIN tb_issue ti ON  t.`issue_id`=ti.`IssueId` \n" +
            "WHERE ti.queryFullsuccessDate IS NOT NULL AND ti.`ProjectFrom`=1\n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -4 MONTH),'%Y-%m')\n" +
            "UNION\n" +
            "SELECT IFNULL(SUM(ti.`Amount`),0) AS sum_amt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -3 MONTH),'%Y-%m') date_time,'yidian_car'  AS business_type FROM tb_issue_business t JOIN tb_issue ti ON  t.`issue_id`=ti.`IssueId` \n" +
            "WHERE ti.queryFullsuccessDate IS NOT NULL AND ti.`ProjectFrom`=1\n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -3 MONTH),'%Y-%m')\n" +
            "UNION\n" +
            "SELECT IFNULL(SUM(ti.`Amount`),0) AS sum_amt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -2 MONTH),'%Y-%m') date_time,'yidian_car'  FROM tb_issue_business t JOIN tb_issue ti ON  t.`issue_id`=ti.`IssueId` \n" +
            "WHERE ti.queryFullsuccessDate IS NOT NULL AND ti.`ProjectFrom`=1\n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -2 MONTH),'%Y-%m')\n" +
            "UNION\n" +
            "SELECT IFNULL(SUM(ti.`Amount`),0) AS sum_amt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -1 MONTH),'%Y-%m') date_time,'yidian_car'  FROM tb_issue_business t JOIN tb_issue ti ON  t.`issue_id`=ti.`IssueId` \n" +
            "WHERE ti.queryFullsuccessDate IS NOT NULL AND ti.`ProjectFrom`=1\n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -1 MONTH),'%Y-%m')\n" +
            "UNION\n" +
            "SELECT IFNULL(SUM(ti.`Amount`),0) AS sum_amt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 0 MONTH),'%Y-%m') date_time,'yidian_car'  FROM tb_issue_business t JOIN tb_issue ti ON  t.`issue_id`=ti.`IssueId` \n" +
            "WHERE ti.queryFullsuccessDate IS NOT NULL AND ti.`ProjectFrom`=1\n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 0 MONTH),'%Y-%m')\n" +
            ") t GROUP BY t.date_time\n" +
            "UNION\n" +
            "-- 小微贷\n" +
            "SELECT IFNULL(SUM(t.`full_borrow_money`),0) AS sum_amt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -5 MONTH),'%Y-%m') date_time,'small' AS business_type FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -5 MONTH),'%Y-%m')\n" +
            "AND t.`business_id` IN (SELECT tb.`business_id` FROM tb_business tb WHERE tb.`business_type`='T251' OR tb.`business_type` IN('T160','T170'))\n" +
            "UNION\n" +
            "SELECT IFNULL(SUM(t.`full_borrow_money`),0) AS sum_amt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -4 MONTH),'%Y-%m') date_time,'small' AS business_type FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -4 MONTH),'%Y-%m')\n" +
            "AND t.`business_id` IN (SELECT tb.`business_id` FROM tb_business tb WHERE tb.`business_type`='T251' OR tb.`business_type` IN('T160','T170'))\n" +
            "UNION\n" +
            "SELECT IFNULL(SUM(t.`full_borrow_money`),0) AS sum_amt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -3 MONTH),'%Y-%m') date_time,'small' AS business_type FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -3 MONTH),'%Y-%m')\n" +
            "AND t.`business_id` IN (SELECT tb.`business_id` FROM tb_business tb WHERE tb.`business_type`='T251' OR tb.`business_type` IN('T160','T170'))\n" +
            "UNION\n" +
            "SELECT IFNULL(SUM(t.`full_borrow_money`),0) AS sum_amt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -2 MONTH),'%Y-%m') date_time,'small' AS business_type FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -2 MONTH),'%Y-%m')\n" +
            "AND t.`business_id` IN (SELECT tb.`business_id` FROM tb_business tb WHERE tb.`business_type`='T251' OR tb.`business_type` IN('T160','T170'))\n" +
            "UNION\n" +
            "SELECT IFNULL(SUM(t.`full_borrow_money`),0) AS sum_amt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -1 MONTH),'%Y-%m') date_time,'small' AS business_type FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL -1 MONTH),'%Y-%m')\n" +
            "AND t.`business_id` IN (SELECT tb.`business_id` FROM tb_business tb WHERE tb.`business_type`='T251' OR tb.`business_type` IN('T160','T170'))\n" +
            "UNION\n" +
            "SELECT IFNULL(SUM(t.`full_borrow_money`),0) AS sum_amt,DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 0 MONTH),'%Y-%m') date_time,'small' AS business_type FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` WHERE ti.queryFullsuccessDate IS NOT NULL\n" +
            "AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 0 MONTH),'%Y-%m')\n" +
            "AND t.`business_id` IN (SELECT tb.`business_id` FROM tb_business tb WHERE tb.`business_type`='T251' OR tb.`business_type` IN('T160','T170'))"),
    //当月分公司撮合金额排名
    MONTH_COMPANY_TYPE_SQL("-- 房速贷\n" +
            "SELECT t.company,t.sum_amt,t.business_type FROM (\n" +
            "SELECT tp.`DEPT_NAME` AS company,IFNULL(SUM(t.`full_borrow_money`),0) AS sum_amt,'house' AS business_type FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` \n" +
            "JOIN tb_business tb ON t.`business_id`=tb.`business_id` JOIN tb_department tp ON tb.`branch_id`=tp.`DEPT_ID` WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            " AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 0 MONTH),'%Y-%m')\n" +
            " AND (tb.`business_type` LIKE '%房速贷%' OR tb.`business_type` IN ('T100','T110','T160','T170','T251'))\n" +
            "GROUP BY tb.`branch_id`\n" +
            "UNION ALL\n" +
            "-- 车易贷\n" +
            "SELECT t.company,IFNULL(SUM(t.sum_amt),0) sum_amt,t.business_type FROM (\n" +
            "SELECT tp.`DEPT_NAME` AS company,IFNULL(SUM(t.`full_borrow_money`),0) AS sum_amt,'car' AS business_type FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` \n" +
            "JOIN tb_business tb ON t.`business_id`=tb.`business_id` JOIN tb_department tp ON tb.`branch_id`=tp.`DEPT_ID` WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "  AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 0 MONTH),'%Y-%m')\n" +
            "  AND tb.`business_type` LIKE '%车易贷%'\n" +
            "GROUP BY tb.`branch_id`\n" +
            "UNION ALL\n" +
            "-- 一点车贷\n" +
            "SELECT tbi.`branch_name` AS company,IFNULL(SUM(ti.`Amount`),0) AS sum_amt,'car' AS business_type FROM tb_issue_business t JOIN tb_issue ti ON  t.`issue_id`=ti.`IssueId`\n" +
            " JOIN tb_yidian_business_information tbi ON t.`business_id`=tbi.`business_id`\n" +
            "WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            " AND DATE_FORMAT(ti.queryFullsuccessDate,'%Y-%m')=DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 0 MONTH),'%Y-%m')\n" +
            "GROUP BY tbi.`branch_name`\n" +
            ") t\n" +
            "  GROUP BY t.company\n" +
            ") t WHERE t.company IS NOT NULL"),
    //服务客户地区分布top10
    SERVICE_AREA_SQL("SELECT t.province,COUNT(1) cnt FROM (\n" +
            "SELECT tt.`branch_province` AS province FROM tb_department tt JOIN tb_business tb ON tt.`DEPT_ID`=tb.`branch_id` JOIN tb_fsd_customer  t ON tb.`business_id`=t.`business_id`\n" +
            "JOIN (SELECT aa.car_business_id FROM tb_car_business_log aa  WHERE  aa.log_content='出款申请' AND aa.log_type!='50' GROUP BY aa.car_business_id)  aa ON t.`business_id`=aa.`car_business_id`  \n" +
            "UNION ALL\n" +
            "SELECT CONCAT(t.id_card_sheng,'省') AS province FROM tb_car_personal  t JOIN  \n" +
            "(SELECT aa.car_business_id FROM tb_car_business_log aa  WHERE  aa.log_content='出款申请' AND aa.log_type!='50' GROUP BY aa.car_business_id) aa ON t.`car_business_id`=aa.`car_business_id`\n" +
            "UNION ALL\n" +
            "SELECT a.`account_province` AS province FROM tb_yidian_car_personal_information a JOIN tb_issue_business t ON a.`business_id`=t.`business_id` JOIN tb_issue ti ON  t.`issue_id`=ti.`IssueId` JOIN tb_yidian_business_information tbi ON t.`business_id`=tbi.`business_id`\n" +
            "WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            ") t WHERE t.province !=''\n" +
            "GROUP BY t.province"),
    //服务客户行业分布
    SERVICE_CUSTOMER_SQL("SELECT tt.profession,COUNT(1) cnt FROM (\n" +
            "SELECT tp.PARA_VALUE AS  profession FROM tb_parameter  tp JOIN  tb_fsd_customer  t ON tp.para_order=t.`profession` JOIN  (SELECT aa.car_business_id FROM tb_car_business_log aa \n" +
            " WHERE aa.log_content='出款申请' GROUP BY aa.car_business_id) aa ON t.`business_id`=aa.`car_business_id` WHERE  tp.PARA_NAME = \"职业信息\" AND tp.`PARA_TYPE` = \"行业类别\" \n" +
            "UNION ALL\n" +
            "SELECT tj.job_duty AS profession FROM  tb_car_job tj JOIN  tb_car_personal  t ON tj.car_business_id=t.car_business_id JOIN\n" +
            " (SELECT aa.car_business_id FROM tb_car_business_log aa  WHERE  aa.log_content='出款申请' AND aa.log_type!='50' GROUP BY aa.car_business_id) aa ON t.`car_business_id`=aa.`car_business_id`\n" +
            " UNION ALL\n" +
            " SELECT tp.PARA_VALUE AS  profession FROM tb_parameter  tp JOIN  tb_fsd_customer  t ON tp.para_order=t.`profession` JOIN  (SELECT aa.car_business_id FROM tb_car_business_log aa \n" +
            " WHERE aa.log_content='出款申请' GROUP BY aa.car_business_id) aa ON t.`business_id`=aa.`car_business_id` JOIN tb_output_list tl ON tl.BUSINESS_ID=t.`business_id`\n" +
            "   JOIN tb_business tb ON tb.business_id=t.`business_id`\n" +
            "  WHERE  tp.PARA_NAME = \"职业信息\" AND tp.`PARA_TYPE` = \"行业类别\"  AND tb.issue_type!=1 \n" +
            " UNION ALL\n" +
            " SELECT tj.job_duty AS profession FROM  tb_car_job tj JOIN  tb_car_personal  t ON tj.car_business_id=t.car_business_id JOIN\n" +
            " (SELECT aa.car_business_id FROM tb_car_business_log aa  WHERE  aa.log_content='出款申请' AND aa.log_type!='50' GROUP BY aa.car_business_id) aa ON t.`car_business_id`=aa.`car_business_id`\n" +
            "  JOIN tb_business tb ON tb.business_id=t.`car_business_id` AND tb.issue_type!=1  \n" +
            ") tt\n" +
            "GROUP BY tt.profession"),
    //累计服务企业与个人
    SUM_SERVICE_PERSON_SQL("SELECT SUM(tt.cnt) AS sum_cnt FROM (\n" +
            "SELECT COUNT(DISTINCT t.identify_card) cnt FROM tb_fsd_customer  t JOIN tb_car_business_log aa ON t.`business_id`=aa.`car_business_id` WHERE t.`customer_type`='个人'\n" +
            "UNION\n" +
            "SELECT COUNT(DISTINCT t.id_card_no) cnt FROM tb_car_personal  t JOIN tb_car_business_log aa ON t.`car_business_id`=aa.`car_business_id` WHERE t.`customer_type`='个人'\n" +
            "UNION\n" +
            "SELECT COUNT(DISTINCT t.business_licence) cnt FROM tb_fsd_customer  t JOIN tb_car_business_log aa ON t.`business_id`=aa.`car_business_id` WHERE t.`customer_type`!='个人'\n" +
            "UNION\n" +
            "SELECT COUNT(DISTINCT t.business_licence) cnt FROM tb_car_personal  t JOIN tb_car_business_log aa ON t.`car_business_id`=aa.`car_business_id` WHERE t.`customer_type`!='个人'\n" +
            "UNION\n" +
            "SELECT COUNT(1) cnt FROM tb_yidian_car_personal_information tf JOIN tb_issue_business t ON tf.`business_id`=t.`business_id`\n" +
            " JOIN tb_issue ti ON  t.`issue_id`=ti.`IssueId` JOIN tb_yidian_business_information tbi ON t.`business_id`=tbi.`business_id`\n" +
            "WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            ") tt\n"),
    SUM_AMOUNT_SQL("SELECT SUM(t.total_sum_amt) AS total_sum_amt FROM(\n" +
            "SELECT IFNULL(SUM(t.`full_borrow_money`),0) AS total_sum_amt FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` \n" +
            "WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "UNION\n" +
            "SELECT IFNULL(SUM(ti.`Amount`),0) AS total_sum_amt FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` \n" +
            "WHERE ti.queryFullsuccessDate IS NOT NULL  AND ti.`ProjectFrom`=1\n" +
            "UNION\n" +
            "SELECT SUM(t.`OUTPUT_MONEY`) AS total_sum_amt FROM tb_output_list t JOIN tb_business tt ON t.`BUSINESS_ID`=tt.`business_id` WHERE tt.issue_type!=1 \n" +
            ") t"),
    SUM_ORDER_SQL("SELECT SUM(t.cnt) AS total_sum_amt FROM (\n" +
            "SELECT COUNT(ti.`IssueId`) AS cnt FROM tb_issue_business t JOIN tb_issue ti ON t.`issue_id`=ti.`IssueId` WHERE ti.queryFullsuccessDate IS NOT NULL \n" +
            "UNION\n" +
            "SELECT COUNT(t.`business_id`) AS cnt FROM tb_output_list t JOIN tb_business tt ON t.`BUSINESS_ID`=tt.`business_id` WHERE tt.issue_type!=1\n" +
            ") t;");
    private String sql;

    PrepareSql(String sql) {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}

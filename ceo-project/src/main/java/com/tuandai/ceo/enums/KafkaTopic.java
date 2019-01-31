package com.tuandai.ceo.enums;

public enum KafkaTopic {
    FULL_ISSUE_PUSH("看板共用topic","full_issue_push_topic"),
    APPROVAL_FINISHED("已审批完成","approval_finished_topic"),
    CEO_TOPIC("看板共用topic","full_issue_push_topic"),
    SUM_AMOUNT("累计撮合金额", "total_out_put_money_topic"),
    SUM_ORDER("累计撮合单数", "total_out_put_orders_topic"),
    SUM_SERVICE_PERSON("累计服务企业与个人", "total_service_company_personal_num_topic"),
    BUSINESS_BALANCE("撮合业务余额", "total_business_balance_topic"),
    MONTH_TYPE_AMOUNT("各月撮合金额趋势", "every_month_out_put_money_trend_topic"),
    MONTH_BRANCH_TREND("当月分公司撮合金额排名top10", "every_month_branch_out_put_money_trend_topic"),
    BORROW_LIMIT("借款期限占比分布", "borrow_limit_spread_topic"),
    APPLY_OUTPUT("客户撮合情况", "apply_output_get_num_topic"),
    SERVICE_CUSTOMER("服务客户行业分布", "service_custorm_profession_spread_topic"),
    SERVICE_CUSTOMER_AREA("服务客户地区分布top10", "service_custom_area_spread_topic"),
    CENTER_MAP_TOPIC("中心地图说明", "center_map");

    private String name;
    private String value;
    private KafkaTopic(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}

const redshift = require('./redshift');
let aw_query;
let ttd_query;
let combined_query;

let receiveIds = function(account) {

aw_query = `SELECT
            DISTINCT to_char(to_date(statistic_dt,
            'YYYY-MM-DD'),
            'YYYYMMDD') AS "Date",
            account_nm AS "AdWords_Account_Name",
            source_system_nm AS "Media",
            device_nm AS "Device_Type",
            ad_group_nm AS "AdWords_Ad_Group",
            campaign_nm AS "AdWords Campaign",
            ad_headline_txt AS "Headline_Text",
            ad_description_txt AS "Ad_Description_Text",
            ad_description_one_txt AS "Ad_Description_1_Text",
            ad_description_two_txt AS "Ad_Description_2_Text",
            SUM(impression_cnt) AS "AdWords_Impressions",
            SUM(click_cnt) AS "AdWords_Clicks",
            SUM(video_view_cnt) AS "AdWords_Video_Views",
            SUM(all_conversion_cnt) AS "AdWords_Conversions",
            SUM(all_conversion_value_num) AS "AdWords_Conversion_Value",
            SUM(cost_amt) AS "AdWords_Media_Cost" 
            FROM vw_search_ad_performance 
            WHERE account_id=${account.adwords} 
            GROUP BY
            statistic_dt,
            account_nm,
            source_system_nm,
            device_nm,
            ad_group_nm,
            campaign_nm,
            ad_headline_txt,
            ad_description_txt,
            ad_description_one_txt,
            ad_description_two_txt 
            ORDER BY
            statistic_dt`

ttd_query = `SELECT
            DISTINCT to_char(to_date(statistic_dt,
            "YYYY-MM-DD"),
            "YYYYMMDD") AS "Date",
            source_system_nm AS "Platform",
            ad_environment_typ AS "TTD_Ad_Environment",
            creative_typ AS "Media",
            campaign_nm AS "TTD_Campaign_Name",
            ad_group_fill_typ AS "TTD_Ad_Group",
            creative_nm AS "TTD_Ad_Name",
            ad_format_txt AS "TTD_Ad_Dimensions",
            SUM(impressions_cnt) AS "TTD_Impressions",
            SUM(clicks_cnt) AS "TTD_Clicks",
            SUM(player_starts_cnt) AS "TTD_Player_Starts",
            SUM(player_views_cnt) AS "TTD_Views",
            SUM(player_completed_views_cnt) AS "TTD_Completed_Views",
            SUM(c01_click_conversion_cnt) AS "KPI_1_Click_Conversions",
            SUM(c01_view_through_conversion_cnt) AS "KPI_1_View_Through_Conversions",
            SUM(c01_click_conversion_revenue_amt) AS "Click_Revenue",
            SUM(c01_view_through_conversion_revenue_amt) AS "View_Through_Revenue",
            SUM(c02_click_conversion_cnt) AS "KPI_2_Click_Conversions",
            SUM(c02_view_through_conversion_cnt) AS "KPI_2_View_Through_Conversions",
            SUM(c03_click_conversion_cnt) AS "KPI_3_Click_Conversions",
            SUM(c03_view_through_conversion_cnt) AS "KPI_3_View_Through_Conversions",
            SUM(c04_click_conversion_cnt) AS "KPI_4_Click_Conversions",
            SUM(c04_view_through_conversion_cnt) AS "KPI_4_View_Through_Conversions",
            SUM(c05_click_conversion_cnt) AS "KPI_5_Click_Conversions",
            SUM(c05_view_through_conversion_cnt) AS "KPI_5_View_Through_Conversions",
            SUM(advertiser_cost_usd_amt) AS "TTD_Media_Cost" 
            FROM vw_ttd_performance 
            WHERE advertiser_id=${account.trade_desk}
            GROUP BY
            statistic_dt,
            source_system_nm,
            ad_environment_typ,
            creative_typ,
            campaign_nm,
            ad_group_fill_typ,
            creative_nm,
            ad_format_txt 
            ORDER BY
            statistic_dt`;

combined_query = `SELECT *
                FROM
                (SELECT
                    DISTINCT statistic_dt,
                    creative_typ,
                    NULL AS source_system_nm,
                    0 AS video_view_cnt,
                    player_views_cnt,
                    impressions_cnt,
                    0 AS impression_cnt,
                    clicks_cnt,
                    0 AS click_cnt,
                    media_cost_usd_amt,
                    0 AS cost_amt 
                FROM
                    vw_ttd_performance 
                WHERE
                    advertiser_id=${account.trade_desk}
                UNION
                ALL SELECT
                    DISTINCT statistic_dt,
                    NULL AS creative_typ,
                    source_system_nm,
                    video_view_cnt,
                    0 AS player_views_cnt,
                    0 AS impressions_cnt,
                    impression_cnt,
                    0 AS clicks_cnt,
                    click_cnt,
                    0 AS media_cost_usd_amt,
                    cost_amt 
                FROM
                    vw_search_account_performance 
                WHERE
                    account_id=${account.adwords} 
                ) data 
                GROUP BY
                data.statistic_dt,
                data.creative_typ,
                data.source_system_nm 
                ORDER BY
                statistic_dt`;

       redshift.getRedshiftData(account, aw_query, ttd_query, combined_query)      
}

module.exports = {
    aw_query:aw_query,
    ttd_query:ttd_query,
    combined_query:combined_query,
    receiveIds:receiveIds
}

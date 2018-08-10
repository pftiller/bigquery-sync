const redshift = require('./redshift');
let aw_query;
let ttd_query;
let combined_query;

let receiveIds = function(account) {

aw_query = `SELECT
                DISTINCT to_char(to_date(vw_search_ad_performance.statistic_dt,
                'YYYY-MM-DD'),
                'YYYYMMDD') AS "Date",
                vw_search_ad_performance.account_nm AS "AdWords_Account_Name",
                vw_search_ad_performance.source_system_nm AS "Media",
                vw_search_ad_performance.device_nm AS "Device_Type",
                vw_search_ad_performance.ad_group_nm AS "AdWords_Ad_Group",
                vw_search_ad_performance.campaign_nm AS "AdWords Campaign",
                vw_search_ad_performance.ad_headline_txt AS "Headline_Text",
                vw_search_ad_performance.ad_description_txt AS "Ad_Description_Text",
                vw_search_ad_performance.ad_description_one_txt AS "Ad_Description_1_Text",
                vw_search_ad_performance.ad_description_two_txt AS "Ad_Description_2_Text",
                SUM(vw_search_ad_performance.impression_cnt) AS "AdWords_Impressions",
                SUM(vw_search_ad_performance.click_cnt) AS "AdWords_Clicks",
                SUM(vw_search_ad_performance.video_view_cnt) AS "AdWords_Video_Views",
                SUM(vw_search_ad_performance.all_conversion_cnt) AS "AdWords_Conversions",
                SUM(vw_search_ad_performance.all_conversion_value_num) AS "AdWords_Conversion_Value",
                SUM(vw_search_ad_performance.cost_amt) AS "AdWords_Media_Cost" 
                FROM
                vw_search_ad_performance 
                WHERE
                vw_search_ad_performance.account_id = ${account.adwords}
                GROUP BY
                vw_search_ad_performance.statistic_dt,
                vw_search_ad_performance.account_nm,
                vw_search_ad_performance.source_system_nm,
                vw_search_ad_performance.device_nm,
                vw_search_ad_performance.ad_group_nm,
                vw_search_ad_performance.campaign_nm,
                vw_search_ad_performance.ad_headline_txt,
                vw_search_ad_performance.ad_description_txt,
                vw_search_ad_performance.ad_description_one_txt,
                vw_search_ad_performance.ad_description_two_txt 
                ORDER BY
                vw_search_ad_performance.statistic_dt`

ttd_query = `SELECT
            DISTINCT to_char(to_date(vw_ttd_performance.statistic_dt,
            "YYYY-MM-DD"),
            "YYYYMMDD") AS "Date",
            vw_ttd_performance.source_system_nm AS "Platform",
            vw_ttd_performance.ad_environment_typ AS "TTD_Ad_Environment",
            vw_ttd_performance.creative_typ AS "Media",
            vw_ttd_performance.campaign_nm AS "TTD_Campaign_Name",
            vw_ttd_performance.ad_group_fill_typ AS "TTD_Ad_Group",
            vw_ttd_performance.creative_nm AS "TTD_Ad_Name",
            vw_ttd_performance.ad_format_txt AS "TTD_Ad_Dimensions",
            SUM(vw_ttd_performance.impressions_cnt) AS "TTD_Impressions",
            SUM(vw_ttd_performance.clicks_cnt) AS "TTD_Clicks",
            SUM(vw_ttd_performance.player_starts_cnt) AS "TTD_Player_Starts",
            SUM(vw_ttd_performance.player_views_cnt) AS "TTD_Views",
            SUM(vw_ttd_performance.player_completed_views_cnt) AS "TTD_Completed_Views",
            SUM(vw_ttd_performance.c01_click_conversion_cnt) AS "KPI_1_Click_Conversions",
            SUM(vw_ttd_performance.c01_view_through_conversion_cnt) AS "KPI_1_View_Through_Conversions",
            SUM(vw_ttd_performance.c01_click_conversion_revenue_amt) AS "Click_Revenue",
            SUM(vw_ttd_performance.c01_view_through_conversion_revenue_amt) AS "View_Through_Revenue",
            SUM(vw_ttd_performance.c02_click_conversion_cnt) AS "KPI_2_Click_Conversions",
            SUM(vw_ttd_performance.c02_view_through_conversion_cnt) AS "KPI_2_View_Through_Conversions",
            SUM(vw_ttd_performance.c03_click_conversion_cnt) AS "KPI_3_Click_Conversions",
            SUM(vw_ttd_performance.c03_view_through_conversion_cnt) AS "KPI_3_View_Through_Conversions",
            SUM(vw_ttd_performance.c04_click_conversion_cnt) AS "KPI_4_Click_Conversions",
            SUM(vw_ttd_performance.c04_view_through_conversion_cnt) AS "KPI_4_View_Through_Conversions",
            SUM(vw_ttd_performance.c05_click_conversion_cnt) AS "KPI_5_Click_Conversions",
            SUM(vw_ttd_performance.c05_view_through_conversion_cnt) AS "KPI_5_View_Through_Conversions",
            SUM(vw_ttd_performance.advertiser_cost_usd_amt) AS "TTD_Media_Cost" 
            FROM
            vw_ttd_performance 
            WHERE
            vw_ttd_performance.advertiser_id = ${account.trade_desk}
            AND statistic_dt > 2018-06-30    
            GROUP BY
            vw_ttd_performance.statistic_dt,
            vw_ttd_performance.source_system_nm,
            vw_ttd_performance.ad_environment_typ,
            vw_ttd_performance.creative_typ,
            vw_ttd_performance.campaign_nm,
            vw_ttd_performance.ad_group_fill_typ,
            vw_ttd_performance.creative_nm,
            vw_ttd_performance.ad_format_txt 
            ORDER BY
            vw_ttd_performance.statistic_dt`;

combined_query = `SELECT
                to_char(to_date(data.statistic_dt,
                'YYYY-MM-DD'),
                'YYYYMMDD') AS "Date",
                CASE 
                    WHEN data.creative_typ IS NULL THEN data.source_system_nm 
                    WHEN data.source_system_nm IS NULL THEN data.creative_typ 
                END AS "Media",
                CASE 
                    WHEN Sum(data.impressions_cnt) = 0 THEN Sum(data.impression_cnt) 
                    WHEN Sum(data.impression_cnt) = 0 THEN Sum(data.impressions_cnt) 
                END AS "Total_Impressions",
                CASE 
                    WHEN Sum(data.clicks_cnt) = 0 THEN Sum(data.click_cnt) 
                    WHEN Sum(data.click_cnt) = 0 THEN Sum(data.clicks_cnt) 
                END AS "Total_Clicks",
                CASE 
                    WHEN Sum(data.media_cost_usd_amt) = 0 THEN Sum(data.cost_amt) 
                    WHEN Sum(data.cost_amt) = 0 THEN Sum(data.media_cost_usd_amt) 
                END AS "Total_Cost",
                CASE 
                    WHEN Sum(data.player_views_cnt) = 0 THEN Sum(data.video_view_cnt) 
                    WHEN Sum(data.video_view_cnt) = 0 THEN Sum(data.player_views_cnt) 
                END AS "Total_Video_Views" 
                FROM
                (SELECT
                    DISTINCT vw_ttd_performance.statistic_dt,
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
                    vw_ttd_performance.advertiser_id = ${account.trade_desk}
                UNION
                ALL SELECT
                    DISTINCT vw_search_account_performance.statistic_dt,
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
                    vw_search_account_performance.account_id = ${account.adwords} 
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

const redshift = require('./redshift');
let aw_query;
let ttd_query;
let combined_query;
var todayTimeStamp = +new Date;
var oneDayTimeStamp = 1000 * 60 * 60 * 24;
var diff = todayTimeStamp - oneDayTimeStamp;
var yesterdayDate = new Date(diff);
let yesterday = yesterdayDate.getDate() < 10 ? '0' + yesterdayDate.getDate() : yesterdayDate.getDate();
var yesterdayString = yesterdayDate.getFullYear() + '-' + (yesterdayDate.getMonth() + 1) + '-' + yesterday;

let receiveIds = function(account) {
    if(account.adwords !== null) {
        aw_query = `SELECT
                    DISTINCT to_char(to_date(statistic_dt,
                        'YYYY-MM-DD'),
                        'YYYY-MM-DD') AS "date",
                    source_system_nm AS "platform",
                    account_nm AS "advertiser",
                    device_nm AS "device",
                    campaign_nm AS "campaign",
                    ad_group_nm AS "ad_group",
                    ad_group_typ AS "ad_type",
                    ad_description_txt AS "ad_description",
                    ad_description_one_txt AS "ad_description_1",
                    ad_description_two_txt AS "ad_description_2",
                    ad_headline_txt AS "ad_headline",
                    ad_headline_part_one_txt AS "ad_headline_1", 
                    ad_headline_part_two_txt AS "ad_headline_2",
                    SUM(impression_cnt) AS "impressions",
                    SUM(click_cnt) AS "clicks",
                    SUM(video_view_cnt) AS "video_views",
                    SUM(interaction_cnt) AS "interactions",
                    SUM(engagement_cnt) AS "engagements",
                    SUM(conversion_cnt) AS "conversions",
                    SUM(all_conversion_cnt) AS "total_conversions",
                    SUM(all_conversion_value_num) AS "conversion_value",
                    SUM(cost_amt) AS "media_cost"
                    FROM
                    vw_search_ad_performance 
                    WHERE
                    CAST(statistic_dt AS DATE)='${yesterdayString}'
                    AND
                    account_id = ${account.adwords}
                    GROUP BY
                    statistic_dt,
                    source_system_nm,
                    account_nm,
                    device_nm,
                    campaign_nm,
                    ad_group_nm,
                    ad_group_typ,
                    ad_description_txt,
                    ad_description_one_txt,
                    ad_description_two_txt,
                    ad_headline_txt,
                    ad_headline_part_one_txt,
                    ad_headline_part_two_txt
                    ORDER BY statistic_dt;`    
            redshift.getRedshiftData(account, aw_query);          
    }
    if(account.trade_desk !== null) {    
        ttd_query = `SELECT
                        DISTINCT to_char(to_date(statistic_dt,
                            'YYYY-MM-DD'),
                            'YYYY-MM-DD') AS "date",
                        source_system_nm AS "platform",
                        advertiser_nm AS "advertiser",
                        ad_environment_typ AS "ad_environment",
                        campaign_nm AS "campaign",
                        ad_group_nm AS "ad_group",
                        creative_nm AS "ad",
                        creative_typ AS "ad_type",
                        ad_format_txt AS "ad_dimensions",
                        tracking_tag_1_nm AS "kpi_1_name",
                        tracking_tag_2_nm AS "kpi_2_name",
                        tracking_tag_3_nm AS "kpi_3_name",
                        tracking_tag_4_nm AS "kpi_4_name",
                        tracking_tag_5_nm AS "kpi_5_name",
                        purchase_order_num AS "order_id",
                        SUM(bids_cnt) AS "bids",
                        SUM(impressions_cnt) AS "impressions",
                        SUM(clicks_cnt) AS "clicks",
                        CAST(SUM(advertiser_cost_usd_amt) AS NUMERIC(15,9)) AS "media_cost",
                        SUM(player_complete_quarter_cnt) AS "video_played_to_25",
                        SUM(player_complete_half_cnt) AS "video_played_to_50",
                        SUM(player_complete_three_quarter_cnt) AS "video_played_to_75",
                        SUM(player_close_cnt) AS "video_player_closes",
                        SUM(player_collapse_cnt) AS "video_player_collapses",
                        SUM(player_completed_views_cnt) AS "video_played_to_100",
                        SUM(player_errors_cnt) AS "video_player_errors",
                        SUM(player_full_screen_cnt) AS "video_player_full_screen",
                        SUM(player_invitation_accept_cnt) AS "video_player_invitation_accepts",
                        SUM(player_mute_cnt) AS "video_player_mutes",
                        SUM(player_pause_cnt) AS "video_player_pauses",
                        SUM(player_resume_cnt) AS "video_player_resumes",
                        SUM(player_rewind_cnt) AS "video_player_rewinds",
                        SUM(player_skip_cnt) AS "video_player_skips",
                        SUM(player_starts_cnt) AS "video_player_starts",
                        SUM(player_unmute_cnt) AS "video_player_unmutes",
                        SUM(player_views_cnt) AS "video_views",
                        SUM(player_expansion_cnt) AS "video_player_expansions",
                        SUM(sampled_tracked_impressions_cnt) AS "tracked_impressions",
                        SUM(sampled_viewed_impressions_cnt) AS "viewed_impressions",
                        SUM(c01_click_conversion_cnt) AS "kpi_1_click_conversions",
                        SUM(c01_click_conversion_revenue_amt) AS "kpi_1_click_conversion_revenue",
                        SUM(c01_view_through_conversion_cnt) AS "kpi_1_view_through_conversions",
                        SUM(c01_view_through_conversion_revenue_amt) AS "kpi_1_view_through_conversion_revenue",
                        SUM(c01_total_click_and_view_conversions_cnt) AS "kpi_1_total_conversions",
                        SUM(c02_click_conversion_cnt) AS "kpi_2_click_conversions",
                        SUM(c02_click_conversion_revenue_amt) AS "kpi_2_click_conversion_revenue",
                        SUM(c02_view_through_conversion_cnt) AS "kpi_2_view_through_conversions",
                        SUM(c02_view_through_conversion_revenue_amt) AS "kpi_2_view_through_conversion_revenue",
                        SUM(c02_total_click_and_view_conversions_cnt) AS "kpi_2_total_conversions",
                        SUM(c03_click_conversion_cnt) AS "kpi_3_click_conversions",
                        SUM(c03_click_conversion_revenue_amt) AS "kpi_3_click_conversion_revenue",
                        SUM(c03_view_through_conversion_cnt) AS "kpi_3_view_through_conversions",
                        SUM(c03_view_through_conversion_revenue_amt) AS "kpi_3_view_through_conversion_revenue",
                        SUM(c03_total_click_and_view_conversions_cnt) AS "kpi_3_total_conversions",
                        SUM(c04_click_conversion_cnt) AS "kpi_4_click_conversions",
                        SUM(c04_click_conversion_revenue_amt) AS "kpi_4_click_conversion_revenue",
                        SUM(c04_view_through_conversion_cnt) AS "kpi_4_view_through_conversions",
                        SUM(c04_view_through_conversion_revenue_amt) AS "kpi_4_view_through_conversion_revenue",
                        SUM(c04_total_click_and_view_conversions_cnt) AS "kpi_4_total_conversions",
                        SUM(c05_click_conversion_cnt) AS "kpi_5_click_conversions",
                        SUM(c05_click_conversion_revenue_amt) AS "kpi_5_click_conversion_revenue",
                        SUM(c05_view_through_conversion_cnt) AS "kpi_5_view_through_conversions",
                        SUM(c05_view_through_conversion_revenue_amt) AS "kpi_5_view_through_conversion_revenue",
                        SUM(c05_total_click_and_view_conversions_cnt) AS "kpi_5_total_conversions"
                        FROM
                        vw_ttd_performance 
                        WHERE
                        CAST(statistic_dt AS DATE)='${yesterdayString}'
                        AND
                        advertiser_id ='${account.trade_desk}'
                        GROUP BY
                        statistic_dt,
                        source_system_nm, 
                        advertiser_nm,
                        ad_environment_typ, 
                        campaign_nm,
                        ad_group_nm,
                        creative_nm,
                        creative_typ, 
                        ad_format_txt,
                        tracking_tag_1_nm, 
                        tracking_tag_2_nm,
                        tracking_tag_3_nm,
                        tracking_tag_4_nm,
                        tracking_tag_5_nm,
                        purchase_order_num
                        ORDER BY statistic_dt;`;
                redshift.getRedshiftData(account, ttd_query)             
        }
        if(account.chz !== null) {
            choozle_query = `SELECT  
                            to_char(to_date(statistic_dt,
                            'YYYY-MM-DD'),
                            'YYYY-MM-DD') AS "date",
                            source_system_nm AS "platform",
                            account_nm AS "advertiser",
                            campaign_nm AS "campaign",
                            ad_group_nm AS "ad_group",
                            SUM(bid_cnt) AS "bids",
                            SUM(impressions_won_cnt) AS "impressions",
                            SUM(click_cnt) AS "clicks",
                            SUM(advertiser_cost_amt) AS "media_cost",
                            SUM(conversions_cnt) AS "conversions"
                            FROM
                            vw_chz_ad_group
                            WHERE
                            account_id=${account.chz} 
                            AND
                            CAST(statistic_dt AS DATE)='${yesterdayString}'
                            GROUP BY
                            statistic_dt,
                            source_system_nm, 
                            account_nm,
                            campaign_nm,
                            ad_group_nm
                            ORDER BY statistic_dt;`
                redshift.getRedshiftData(account, choozle_query)  
        }

        combined_query = `SELECT data.date AS "date",
                            data.platform AS "platform",
                            SUM(data.impressions) AS "total_impressions",
                            SUM(data.clicks) AS "total_clicks",
                            SUM(data.media_cost) AS "total_media_cost",
                            SUM(data.total_conversions) AS "total_conversions"
                            FROM`
                    if(account.adwords !== null) {
                        combined_query +=    
                            `(SELECT
                                to_char(to_date(statistic_dt,
                                    'YYYY-MM-DD'),
                                    'YYYY-MM-DD') AS "date",
                            source_system_nm AS "platform",
                            SUM(impression_cnt) AS "impressions",
                            SUM(click_cnt) AS "clicks",
                            SUM(cost_amt) AS "media_cost",
                            SUM(all_conversion_cnt) AS "total_conversions"
                            FROM vw_search_account_performance 
                            WHERE CAST(statistic_dt AS DATE)='${yesterdayString}' AND account_id = ${account.adwords}
                            GROUP BY statistic_dt,
                                source_system_nm`
                    }
                    if(account.trade_desk !== null) {
                        combined_query+=
                            `UNION
                            ALL SELECT
                            to_char(to_date(statistic_dt,
                                'YYYY-MM-DD'),
                                'YYYY-MM-DD') AS "date",
                            source_system_nm AS "platform",
                            SUM(impressions_cnt) AS "impressions",
                            SUM(clicks_cnt) AS "clicks",
                            CAST(SUM(advertiser_cost_usd_amt) AS NUMERIC(15,9)) AS "media_cost",
                            SUM(c01_total_click_and_view_conversions_cnt + c02_total_click_and_view_conversions_cnt + c03_total_click_and_view_conversions_cnt + c04_total_click_and_view_conversions_cnt + c05_total_click_and_view_conversions_cnt) AS "total_conversions"
                            FROM vw_ttd_performance 
                            WHERE CAST(statistic_dt AS DATE)='${yesterdayString}' AND advertiser_id ='${account.trade_desk}'
                            GROUP BY statistic_dt,
                                source_system_nm`
                    }
                    if(account.chz !==  null) {      
                        `SELECT  
                            to_char(to_date(statistic_dt,
                            'YYYY-MM-DD'),
                            'YYYY-MM-DD') AS "date",
                            source_system_nm AS "platform",
                            SUM(impressions_won_cnt) AS "impressions",
                            SUM(click_cnt) AS "clicks",
                            SUM(advertiser_cost_amt) AS "media_cost",
                            SUM(conversions_cnt) AS "total_conversions"
                            FROM
                            vw_chz_ad_group
                            WHERE
                            account_id=${account.chz} AND CAST(statistic_dt AS DATE)='${yesterdayString}'
                            GROUP BY
                            statistic_dt,
                            source_system_nm`
                    }        
                            
                    combined_query +=       
                            
                                `) data 
                            GROUP BY
                            data.date,
                            data.platform
                            ORDER BY
                            data.date`;

            redshift.getRedshiftData(account, combined_query)      
}

module.exports = {
    aw_query:aw_query,
    ttd_query:ttd_query,
    combined_query:combined_query,
    receiveIds:receiveIds
}

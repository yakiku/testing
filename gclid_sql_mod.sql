
INSERT INTO kiranyalavarthi_dev.gclid_linear_attribution (
    with start_date as (
        SELECT MAX(conversion_time) AS date
        FROM google_ads.gclid_linear_attribution),

         window as (
             select cast(14 as int) as window),

         trades as (
             select o.id
                  , u.uuid
                  , o.subtotal
                  , f.amount                                                                        as fees
                  , (oa.country_id)                                                                 as country_id
                  , pmc.product_category                                                            as vertical
                  , pmc.product_uuid
                  , (convert_timezone('UTC', 'America/Detroit', o.created_at))                      as order_date
                  , case
                        when order_date <= date_add('d', 5, release_date) then 'release'
                        when order_date > date_add('d', 5, release_date) and
                             order_date <= date_add('w', 8, release_date)
                            then 'cycle'
                        when order_date > date_add('w', 8, release_date) then 'closet'
                        else 'closet' end                                                           as sale_ind
                  , case when bid_user_id = 29 or status in ('canceled', 'fraud') then 1 else 0 end as logic_flag
                  , NVL(g.shipping_original_amount, g.shipping_amount, 0) + NVL(g.buyfee_rev_amount, g.buyfee_orig_amount, 0) + NVL(o.bid_amount,0) AS buyer_purchase_price
                  , max(convert_timezone('UTC', 'EST', pimm.created_at))                            as bid_date_time --1
             from soletrade.orders o
                      join soletrade.order_addresses oa on o.id = oa.order_id and oa.address_type = 'shipping'
                      join soletrade.users u on u.id = o.bid_user_id
                      join soletrade.portfolio_items_master pimm on pimm.chain_id = o.bid_chain_id and pimm.state = 300
                      left join soletrade.order_items oi on o.id = oi.order_id
                      left join soletrade.product_market_cache as pmc on oi.sku_uuid_bin = pmc.sku_uuid
                      left join (
                 select amount
                      , chain_id
                      , order_id
                 from soletrade.order_adjustments as oa
                 where (oa.type = 'transactional')
             ) as f on cast(f.chain_id as char) = cast(o.ask_chain_id as char) and f.order_id = o.id
             left join (
                 select
                      chain_id
                      , order_id
                      , max(case when type = 'shipping' then amount else null end) as shipping_amount
                      , max(case when type = 'shipping_original' then amount else null end) as shipping_original_amount
                      , max(case when code = 'BUYFEE_ORIG' then amount else null end) as buyfee_orig_amount
                      , max(case when code = 'BUYFEE_REV' then amount else null end) as buyfee_rev_amount
                 from soletrade.order_adjustments as oa
                  where (oa.type = 'shipping' or oa.type = 'shipping_original' or oa.code = 'BUYFEE_ORIG' or oa.code = 'BUYFEE_REV')
                  group by 1,2
             ) as g on cast(g.chain_id as char) = cast(o.ask_chain_id as char) and g.order_id = o.id
             where 1 = 1
               and date(convert_timezone('UTC', 'America/Detroit', o.created_at)) > (select date from start_date)
               AND o.created_at < (SELECT MAX(visit_start_time) FROM google_analytics.sessions)
             group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),

         users as (
             select uuid
             from trades
             group by 1),

         mapping as (
             select client_id
                  , stitching_user_uuid as user_uuid
             from google_analytics.sessions
             where 1 = 1
               and stitching_user_uuid is not null
             group by 1, 2),

         af_installs as (
             select 'AppsFlyer'                                             as d_source
                  , nvl(a.uuid, m.user_uuid)                                as uuid
                  , af_channel                                              as medium
                  , media_source                                            as source
                  , campaign
                  , af_adset                                                as adset
                  , af_ad                                                   as keyword
                  , case
                        when
                                case
                                    when (lower(source) like '%google%' or lower(medium) like '%google%')
                                        then 'Google Paid'
                                    when (lower(source) like '%facebook%' or lower(medium) like '%facebook%')
                                        then 'Facebook Paid'
                                    when (lower(source) like '%criteo%' or lower(medium) like '%criteo%')
                                        then 'Criteo Paid'
                                    when (lower(source) like '%snapchat%' or lower(medium) like '%snapchat%')
                                        then 'Snapchat Paid'
                                    when (lower(source) like '%apple%' or lower(medium) like '%apple%')
                                        then 'Apple Paid'
                                    when (lower(source) like '%blisspoint%' or lower(medium) like '%blisspoint%')
                                        then 'Blisspoint Paid'
                                    when (lower(source) like '%microsoft%' or lower(medium) like '%microsoft%')
                                        then 'Microsoft Paid'
                                    when (lower(source) like '%youtube%' or lower(medium) like '%youtube%')
                                        then 'Youtube Paid'
                                    when (lower(source) like '%manage%' or lower(medium) like '%manage%')
                                        then 'Manage Paid'
                                    when (lower(source) like '%direct%' or lower(medium) like '%direct%')
                                        then 'Direct Paid'
                                    when lower(source) like '%\\\\_int%' or lower(medium) like '%\\\\_int%'
                                        then 'Other Paid'
                                    when lower(source) like '%referral%' or lower(medium) like '%referral%'
                                        then 'Referral'
                                    when lower(source) like '%email%' or lower(medium) like '%email%'
                                        or lower(source) like '%leanplum%' or lower(medium) like '%leanplum%'
                                        or (lower(source) like '%main%' and lower(medium) like '%referrer%')
                                        then 'Email / Leanplum'
                                    else 'Other' end like '%Paid' then 'Paid'
                        else case
                                 when (lower(source) like '%google%' or lower(medium) like '%google%')
                                     then 'Google Paid'
                                 when (lower(source) like '%facebook%' or lower(medium) like '%facebook%')
                                     then 'Facebook Paid'
                                 when (lower(source) like '%criteo%' or lower(medium) like '%criteo%')
                                     then 'Criteo Paid'
                                 when (lower(source) like '%snapchat%' or lower(medium) like '%snapchat%')
                                     then 'Snapchat Paid'
                                 when (lower(source) like '%apple%' or lower(medium) like '%apple%') then 'Apple Paid'
                                 when (lower(source) like '%blisspoint%' or lower(medium) like '%blisspoint%')
                                     then 'Blisspoint Paid'
                                 when (lower(source) like '%microsoft%' or lower(medium) like '%microsoft%')
                                     then 'Microsoft Paid'
                                 when (lower(source) like '%youtube%' or lower(medium) like '%youtube%')
                                     then 'Youtube Paid'
                                 when (lower(source) like '%manage%' or lower(medium) like '%manage%')
                                     then 'Manage Paid'
                                 when (lower(source) like '%direct%' or lower(medium) like '%direct%')
                                     then 'Direct Paid'
                                 when lower(source) like '%\\\\_int%' or lower(medium) like '%\\\\_int%' then 'Other Paid'
                                 when lower(source) like '%referral%' or lower(medium) like '%referral%' then 'Referral'
                                 when lower(source) like '%email%' or lower(medium) like '%email%'
                                     or lower(source) like '%leanplum%' or lower(medium) like '%leanplum%'
                                     or (lower(source) like '%main%' and lower(medium) like '%referrer%')
                                     then 'Email / Leanplum'
                                 else 'Other' end end                       as source_clean
                  , case
                        when (lower(device_type) like 'iphone%' or lower(device_type) like 'ipad%' or
                              lower(device_type) like 'ipod%') then 'iOS'
                        else 'Android' end                                  as device_mapping
                  , country_code
                  , convert_timezone('UTC', 'EST', a.attributed_touch_time) as date
                  , ''                                                      as visit_id
                  , count(*)
             from cac_ltv.appsflyer_data_locker a
                      left join mapping m on a.customer_user_id = m.client_id
             where 1 = 1
               and date(convert_timezone('UTC', 'America/Detroit', a.attributed_touch_time)) >
                   date_add('day', -((select window from window) + (select window from window)),
                            (select date from start_date))
               and nvl(a.uuid, m.user_uuid) in (select uuid from users)
             group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),

         touches as (
             select CASE WHEN datasource = 'app' THEN 'app-107725170' ELSE 'web-107718485' END as d_source
                  , stitching_user_uuid as user_uuid
                  , medium
                  , source
                  , campaign
                  , ad_content                                        as adset
                  , keyword
                  , case
                        when (lower(source || '/' || medium) like '%cpc%'
                            or lower(source || '/' || medium) like '%criteo%'
                            or lower(source || '/' || medium) like '%\\\\_int%'
                            or lower(lower(source || '/' || medium)) like '%apple search ads%'
                            or lower(lower(source || '/' || medium)) like '%doubleclick%') then 'Paid'
                        when (lower(lower(source || '/' || medium)) like '%direct%') then 'Direct'
                        when (lower(lower(source || '/' || medium)) like '%organic%') then 'Organic'
                        when (lower(source || '/' || medium) like '%af/imp%'
                            or lower(source || '/' || medium) like '%af/rak%'
                            or lower(source || '/' || medium) like '%haitao%'
                            or lower(source || '/' || medium) like '%mifanli%'
                            or lower(source || '/' || medium) like '%rakuten%'
                            or lower(source || '/' || medium) like '%ebates%'
                            or lower(source || '/' || medium) like '%slickdeals%'
                            or lower(source || '/' || medium) like '%suplexed%'
                            or lower(source || '/' || medium) like '%lyst%'
                            or lower(source || '/' || medium) like '%srvtrck%'
                            or lower(source || '/' || medium) like '%modesens%'
                            or lower(source || '/' || medium) like '%fkdeals%'
                            or lower(source || '/' || medium) like '%thedropdate%'
                            or lower(source || '/' || medium) like '%duomai%'
                            or lower(source || '/' || medium) like '%rstyle%'
                            or lower(source || '/' || medium) like '%shopstyle%'
                            or lower(source || '/' || medium) like '%solsense%'
                            or lower(source || '/' || medium) like '%hypeanalyzer%') then 'Affiliate'
                        when lower(source) like '%referral%' or lower(medium) like '%referral%' then 'Referral'
                        when lower(source) like '%email%' or lower(medium) like '%email%'
                            or lower(source) like '%leanplum%' or lower(medium) like '%leanplum%'
                            or (lower(source) like '%main%' and lower(medium) like '%referrer%') then 'Email / Leanplum'
                        else 'Other' end                              as source_clean
                  , case
                        when browser = 'GoogleAnalytics' then operating_system
                        when datasource = 'web'
                            and (device_category in ('mobile', 'tablet')
                                or operating_system in ('iOS', 'Android', 'BlackBerry', 'Windows Phone'))
                            then 'Mobile Web'
                        else device_category
                 end                                                  as device_mapping
                  , case when alpha2 = 'Po' then 'PL' else alpha2 end as country_code
                  , convert_timezone('UTC', 'EST', visit_start_time)  as date
                  , visit_id
                  , gcl_id --added line
                  , count(*)
             from google_analytics.sessions t
                      left join bi.iso_2_mapping iso on iso.name = case
                                                                       when country = 'Czechia' then 'Czech Republic'
                                                                       when country = 'DE' then 'Germany'
                                                                       when country = 'Myanmar (Burma)' then 'Myanmar'
                                                                       when country = 'Russia' then 'Russian Federation'
                                                                       when country = 'UK' then 'United Kingdom'
                                                                       when country = 'GB' then 'United Kingdom'
                                                                       when country = 'US' then 'United States'
                                                                       else country end
             where 1 = 1
               and date(convert_timezone('UTC', 'America/Detroit', visit_start_time)) >
                   date_add('day', -((select window from window) + (select window from window)),
                            (select date from start_date))
               and user_uuid in (select uuid from users)
             group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13),

         touches_enh as (
             select case
                        when (ga.medium = '(none)' and ga.source = '(direct)') and (af.d_source is not null)
                            then af.d_source
                        else ga.d_source end       as d_source_c
                  , ga.user_uuid
                  , ga.gcl_id -- added line
                  , case
                        when (ga.medium = '(none)' and ga.source = '(direct)') and nvl(af.source, af.medium) is not null
                            then af.medium
                        else ga.medium end         as medium_c
                  , case
                        when (ga.medium = '(none)' and ga.source = '(direct)') and nvl(af.source, af.medium) is not null
                            then af.source
                        else ga.source end         as source_c
                  , case
                        when (ga.medium = '(none)' and ga.source = '(direct)') and nvl(af.source, af.medium) is not null
                            then af.campaign
                        else ga.campaign end       as campaign_c
                  , case
                        when (ga.medium = '(none)' and ga.source = '(direct)') and nvl(af.source, af.medium) is not null
                            then af.adset
                        else ga.adset end          as adset_c
                  , ga.keyword                     as keyword_c
                  , case
                        when (ga.medium = '(none)' and ga.source = '(direct)') and nvl(af.source, af.medium) is not null
                            then af.source_clean
                        else ga.source_clean end   as source_clean_c
                  , case
                        when (ga.medium = '(none)' and ga.source = '(direct)') and nvl(af.source, af.medium) is not null
                            then af.country_code
                        else ga.country_code end   as country_code_c
                  , case
                        when (ga.medium = '(none)' and ga.source = '(direct)') and nvl(af.source, af.medium) is not null
                            then af.date
                        else ga.date end           as date_c
                  , case
                        when (ga.medium = '(none)' and ga.source = '(direct)') and nvl(af.source, af.medium) is not null
                            then af.visit_id
                        else cast(ga.visit_id as varchar) end       as visit_id_c
                  , case
                        when (ga.medium = '(none)' and ga.source = '(direct)') and nvl(af.source, af.medium) is not null
                            then af.device_mapping
                        else ga.device_mapping end as device_mapping_c
                  , count(*)
             from touches ga
                      left join af_installs af on ga.user_uuid = af.uuid and (case
                                                                                  when lower(af.source) like '%snapchat%'
                                                                                      or
                                                                                       lower(af.medium) like '%snapchat%'
                                                                                      or
                                                                                       lower(af.source) like '%facebook%'
                                                                                      or
                                                                                       lower(af.medium) like '%facebook%'
                                                                                      or
                                                                                       lower(ga.source) like '%snapchat%'
                                                                                      or
                                                                                       lower(ga.medium) like '%snapchat%'
                                                                                      or
                                                                                       lower(ga.source) like '%facebook%'
                                                                                      or
                                                                                       lower(ga.medium) like '%facebook%'
                                                                                      then
                                                                                          date_add('day', -1, ga.date) <= af.date and
                                                                                          date_add('day', 1, ga.date) >= af.date
                                                                                  else date_add('s', -10, ga.date) <= af.date and
                                                                                       date_add('s', 10, ga.date) >= af.date end)
             where 1 = 1
             group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13),

         trade_touches as (
             select *
                  , row_number() over (partition by t.id order by date_c) as touch_rank
             from trades t
                      left join touches_enh ts on t.uuid = ts.user_uuid and
                                                  ts.date_c between date_add('d', -(select * from window), t.bid_date_time) and t.bid_date_time
             where 1 = 1
         ),

         trade_touches_enh as (
             select *
                  , max(touch_rank) over (partition by id) as max_touch_rank
             from trade_touches),

         first_buy as (
             select order_id, count(*)
             from cac_ltv.first_order_bids
             group by 1),

         mta_flat as (
             select d_source_c
                  , uuid
                  , id
                  , subtotal
                  , order_date
                  , date(date_c)                                                 as date
                  , gcl_id --added line
                  , source_clean_c
                  , case
                        when lower(source_c) like '%facebook%' or lower(medium_c) like '%facebook%' then 'Facebook'
                        when lower(source_c) like '%criteo%' or lower(medium_c) like '%criteo%' then 'Criteo'
                        when lower(source_c) = 'google' and lower(medium_c) = 'organic' then 'Other'
                        when lower(source_c) like '%google%' or lower(medium_c) like '%google%' then 'Google'
                        when lower(source_c) like '%manage%' or lower(medium_c) like '%manage%' then 'Manage'
                        when lower(source_c) like '%snapchat%' or lower(medium_c) like '%snapchat%' then 'Snapchat'
                        when lower(source_c) like '%apple%' or lower(medium_c) like '%apple%' then 'Apple'
                        else 'Other' end                                         as map
                  , source_c
                  , medium_c
                  , campaign_c
                  , adset_c
                  , keyword_c
                  , case
                        when lower(source_c) like '%criteo%' or lower(medium_c) like '%criteo%' or
                             lower(source_c) like '%manage%' or lower(medium_c) like '%manage%' then 'Paid - Criteo'
                        when (lower(source_c) like '%google%' and lower(medium_c) like '%cpc%') or
                             lower(source_c) like '%googleads%' or lower(medium_c) like '%googleads%'
                            then 'Paid - Google'
                        when lower(medium_c) like '%editorial%' then 'Paid - Media Buy'
                        when lower(source_c) in ('facebook', 'facebook ads', 'snapchat_int') then 'Paid - Paid Social'
                        when lower(source_c) like '%apple%search%' or lower(source_c) like '%apple%search%'
                            then 'Paid - Other'
                        when lower(source_c) like '%leanplum%' or lower(medium_c) like '%leanplum%' or
                             lower(source_c) like '%sendgrid%' or lower(medium_c) like '%sendgrid%'
                            then 'Owned - Email / Leanplum'
                        when (lower(campaign_c) like '%stockx%' and
                              (lower(source_c) like '%facebook%' or lower(source_c) like '%instagram%' or
                               lower(source_c) like '%snapchat%' or lower(source_c) like '%twitter%'))
                            or (lower(medium_c) like '%curalate%' or lower(source_c) like '%curalate%')
                            then 'Owned - Social'
                        when source_clean_c = 'Direct' and map = 'Other' then 'Direct - Direct'
                        when source_clean_c = 'Organic' and map in ('Other', 'Google') then 'Organic - Organic'
                        when source_clean_c = 'Paid' and map = 'Google' then 'Paid - Google'
                        when source_clean_c = 'Paid' and map in ('Facebook', 'Snapchat') then 'Paid - Paid Social'
                        when source_clean_c = 'Affiliate' and map = 'Other' then 'Paid - Affiliate'
                        when source_clean_c = 'Paid' and map in ('Criteo', 'Manage') then 'Paid - Criteo'
                        when source_clean_c = 'Paid' and map = 'Apple' then 'Paid - Other'
                        when source_clean_c = 'Email / Leanplum' and map = 'Other' then 'Owned - Email / Leanplum'
                        when source_clean_c is null and map = 'Other' then 'Other - Other'
                        else source_clean_c || ' - ' || map end                  as group_raw
                  , trim(split_part(group_raw, '-', 1))                          as group_raw_1
                  , trim(split_part(group_raw, '-', 2))                          as group_raw_2
                  , nvl(country_code_c, country_id)                              as country
                  , device_mapping_c
                  , case
                        when vertical = 0 then 'N/A'
                        when vertical = 1 then 'Sneakers'
                        when vertical = 2 then 'Handbags'
                        when vertical = 3 then 'Watches'
                        when vertical = 4 then 'Streetwear'
                        when vertical = 5 then 'Collectibles' end                as vertical_name
                  , product_uuid
                  , sale_ind
                  , case
                        when alpha2 = 'GB' then 'UK'
                        when financial_regions = 'Core Asia' then 'Rest of World'
                        else financial_regions end                               as financial_region
                  , case when a.id = b.order_id then 'New' else 'Return' end     as type
                  , buyer_purchase_price
                  , sum(cast(1 as float) / cast(max_touch_rank as float))        as source_attr
                  , sum(cast(subtotal as float) / cast(max_touch_rank as float)) as gmv_source_attr
                  , sum(cast(fees as float) / cast(max_touch_rank as float))     as fees_source_attr
             from trade_touches_enh a
             join first_buy b on a.id = b.order_id
             left join bi.iso_2_mapping i on i.alpha2 = case
                         when nvl(country_code_c, country_id) = 'UK'
                             then 'GB'
                         else nvl(country_code_c, country_id) end
             where 1 = 1
             group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,25)

    select gcl_id
         , 'GCLID New Buyer MTA Trades' as conversion_name
         , order_date                       as conversion_time
         , source_attr                      as attributed_credit
         --, source_attr * 30                 as conversion_value
         , GREATEST(10, 0.09 * buyer_purchase_price) AS conversion_value
         , 'USD'                            as conversion_currency
         , NULL                             AS sftp
    from mta_flat
    where 1 = 1
      and type = 'New'
      and gcl_id is not null);

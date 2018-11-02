select
    tran_id,
    TRAN_DATE,
    user_id,
    pro_id,
    amt,
    rand() * 10 as interest,
    int(rand() * 20) as terms,
    int(rand() * 30) * 1000 as start_amt,
    avg(amt) over 7d_37d_user as 30d_user_all_cate_avg,
    max(amt) over 7d_37d_user as 30d_user_all_cate_max,
    min(amt) over 7d_37d_user as 30d_user_all_cate_min,
    mean(amt) over 7d_37d_user as 30d_user_all_cate_mean,
    avg(IF(pro_id >= 1 and pro_id <= 4, amt, 0)) over 7d_37d_user as 30d_user_current_avg,
    max(IF(pro_id >= 1 and pro_id <= 4, amt, 0)) over 7d_37d_user as 30d_user_current_max,
    min(IF(pro_id >= 1 and pro_id <= 4, amt, 0)) over 7d_37d_user as 30d_user_current_min,
    mean(IF(pro_id >= 1 and pro_id <= 4, amt, 0)) over 7d_37d_user as 30d_user_current_mean,
    avg(IF(pro_id >= 5 and pro_id <= 8, amt, 0)) over 7d_37d_user as 30d_user_regular_avg,
    max(IF(pro_id >= 5 and pro_id <= 8, amt, 0)) over 7d_37d_user as 30d_user_regular_max,
    min(IF(pro_id >= 5 and pro_id <= 8, amt, 0)) over 7d_37d_user as 30d_user_regular_min,
    mean(IF(pro_id >= 5 and pro_id <= 8, amt, 0)) over 7d_37d_user as 30d_user_regular_mean,
    avg(IF(pro_id >= 9 and pro_id <= 12, amt, 0)) over 7d_37d_user as 30d_user_financing_avg,
    max(IF(pro_id >= 9 and pro_id <= 12, amt, 0)) over 7d_37d_user as 30d_user_financing_max,
    min(IF(pro_id >= 9 and pro_id <= 12, amt, 0)) over 7d_37d_user as 30d_user_financing_min,
    mean(IF(pro_id >= 9 and pro_id <= 12, amt, 0)) over 7d_37d_user as 30d_user_financing_mean,
    avg(IF(pro_id >= 13 and pro_id <= 16, amt, 0)) over 7d_37d_user as 30d_user_fund_avg,
    max(IF(pro_id >= 13 and pro_id <= 16, amt, 0)) over 7d_37d_user as 30d_user_fund_max,
    min(IF(pro_id >= 13 and pro_id <= 16, amt, 0)) over 7d_37d_user as 30d_user_fund_min,
    mean(IF(pro_id >= 13 and pro_id <= 16, amt, 0)) over 7d_37d_user as 30d_user_fund_mean,
    avg(amt) over 7d_67d_user as 60d_user_all_cate_avg,
    max(amt) over 7d_67d_user as 60d_user_all_cate_max,
    min(amt) over 7d_67d_user as 60d_user_all_cate_min,
    mean(amt) over 7d_67d_user as 60d_user_all_cate_mean,
    avg(IF(pro_id >= 1 and pro_id <= 4, amt, 0)) over 7d_67d_user as 60d_user_current_avg,
    max(IF(pro_id >= 1 and pro_id <= 4, amt, 0)) over 7d_67d_user as 60d_user_current_max,
    min(IF(pro_id >= 1 and pro_id <= 4, amt, 0)) over 7d_67d_user as 60d_user_current_min,
    mean(IF(pro_id >= 1 and pro_id <= 4, amt, 0)) over 7d_67d_user as 60d_user_current_mean,
    avg(IF(pro_id >= 5 and pro_id <= 8, amt, 0)) over 7d_67d_user as 60d_user_regular_avg,
    max(IF(pro_id >= 5 and pro_id <= 8, amt, 0)) over 7d_67d_user as 60d_user_regular_max,
    min(IF(pro_id >= 5 and pro_id <= 8, amt, 0)) over 7d_67d_user as 60d_user_regular_min,
    mean(IF(pro_id >= 5 and pro_id <= 8, amt, 0)) over 7d_67d_user as 60d_user_regular_mean,
    avg(IF(pro_id >= 9 and pro_id <= 12, amt, 0)) over 7d_67d_user as 60d_user_financing_avg,
    max(IF(pro_id >= 9 and pro_id <= 12, amt, 0)) over 7d_67d_user as 60d_user_financing_max,
    min(IF(pro_id >= 9 and pro_id <= 12, amt, 0)) over 7d_67d_user as 60d_user_financing_min,
    mean(IF(pro_id >= 9 and pro_id <= 12, amt, 0)) over 7d_67d_user as 60d_user_financing_mean,
    avg(IF(pro_id >= 13 and pro_id <= 16, amt, 0)) over 7d_67d_user as 60d_user_fund_avg,
    max(IF(pro_id >= 13 and pro_id <= 16, amt, 0)) over 7d_67d_user as 60d_user_fund_max,
    min(IF(pro_id >= 13 and pro_id <= 16, amt, 0)) over 7d_67d_user as 60d_user_fund_min,
    mean(IF(pro_id >= 13 and pro_id <= 16, amt, 0)) over 7d_67d_user as 60d_user_fund_mean,
    avg(amt) over 7d_37d_prd as 30d_prd_all_cate_avg,
    max(amt) over 7d_37d_prd as 30d_prd_all_cate_max,
    min(amt) over 7d_37d_prd as 30d_prd_all_cate_min,
    mean(amt) over 7d_37d_prd as 30d_prd_all_cate_mean,
    avg(IF(pro_id >= 1 and pro_id <= 4, amt, 0)) over 7d_37d_prd as 30d_prd_current_avg,
    max(IF(pro_id >= 1 and pro_id <= 4, amt, 0)) over 7d_37d_prd as 30d_prd_current_max,
    min(IF(pro_id >= 1 and pro_id <= 4, amt, 0)) over 7d_37d_prd as 30d_prd_current_min,
    mean(IF(pro_id >= 1 and pro_id <= 4, amt, 0)) over 7d_37d_prd as 30d_prd_current_mean,
    avg(IF(pro_id >= 5 and pro_id <= 8, amt, 0)) over 7d_37d_prd as 30d_prd_regular_avg,
    max(IF(pro_id >= 5 and pro_id <= 8, amt, 0)) over 7d_37d_prd as 30d_prd_regular_max,
    min(IF(pro_id >= 5 and pro_id <= 8, amt, 0)) over 7d_37d_prd as 30d_prd_regular_min,
    mean(IF(pro_id >= 5 and pro_id <= 8, amt, 0)) over 7d_37d_prd as 30d_prd_regular_mean,
    avg(IF(pro_id >= 9 and pro_id <= 12, amt, 0)) over 7d_37d_prd as 30d_prd_financing_avg,
    max(IF(pro_id >= 9 and pro_id <= 12, amt, 0)) over 7d_37d_prd as 30d_prd_financing_max,
    min(IF(pro_id >= 9 and pro_id <= 12, amt, 0)) over 7d_37d_prd as 30d_prd_financing_min,
    mean(IF(pro_id >= 9 and pro_id <= 12, amt, 0)) over 7d_37d_prd as 30d_prd_financing_mean,
    avg(IF(pro_id >= 13 and pro_id <= 16, amt, 0)) over 7d_37d_prd as 30d_prd_fund_avg,
    max(IF(pro_id >= 13 and pro_id <= 16, amt, 0)) over 7d_37d_prd as 30d_prd_fund_max,
    min(IF(pro_id >= 13 and pro_id <= 16, amt, 0)) over 7d_37d_prd as 30d_prd_fund_min,
    mean(IF(pro_id >= 13 and pro_id <= 16, amt, 0)) over 7d_37d_prd as 30d_prd_fund_mean,
    avg(amt) over 7d_67d_prd as 60d_prd_all_cate_avg,
    max(amt) over 7d_67d_prd as 60d_prd_all_cate_max,
    min(amt) over 7d_67d_prd as 60d_prd_all_cate_min,
    mean(amt) over 7d_67d_prd as 60d_prd_all_cate_mean,
    avg(IF(pro_id >= 1 and pro_id <= 4, amt, 0)) over 7d_67d_prd as 60d_prd_current_avg,
    max(IF(pro_id >= 1 and pro_id <= 4, amt, 0)) over 7d_67d_prd as 60d_prd_current_max,
    min(IF(pro_id >= 1 and pro_id <= 4, amt, 0)) over 7d_67d_prd as 60d_prd_current_min,
    mean(IF(pro_id >= 1 and pro_id <= 4, amt, 0)) over 7d_67d_prd as 60d_prd_current_mean,
    avg(IF(pro_id >= 5 and pro_id <= 8, amt, 0)) over 7d_67d_prd as 60d_prd_regular_avg,
    max(IF(pro_id >= 5 and pro_id <= 8, amt, 0)) over 7d_67d_prd as 60d_prd_regular_max,
    min(IF(pro_id >= 5 and pro_id <= 8, amt, 0)) over 7d_67d_prd as 60d_prd_regular_min,
    mean(IF(pro_id >= 5 and pro_id <= 8, amt, 0)) over 7d_67d_prd as 60d_prd_regular_mean,
    avg(IF(pro_id >= 9 and pro_id <= 12, amt, 0)) over 7d_67d_prd as 60d_prd_financing_avg,
    max(IF(pro_id >= 9 and pro_id <= 12, amt, 0)) over 7d_67d_prd as 60d_prd_financing_max,
    min(IF(pro_id >= 9 and pro_id <= 12, amt, 0)) over 7d_67d_prd as 60d_prd_financing_min,
    mean(IF(pro_id >= 9 and pro_id <= 12, amt, 0)) over 7d_67d_prd as 60d_prd_financing_mean,
    avg(IF(pro_id >= 13 and pro_id <= 16, amt, 0)) over 7d_67d_prd as 60d_prd_fund_avg,
    max(IF(pro_id >= 13 and pro_id <= 16, amt, 0)) over 7d_67d_prd as 60d_prd_fund_max,
    min(IF(pro_id >= 13 and pro_id <= 16, amt, 0)) over 7d_67d_prd as 60d_prd_fund_min,
    mean(IF(pro_id >= 13 and pro_id <= 16, amt, 0)) over 7d_67d_prd as 60d_prd_fund_mean,
    last(amt) over 7d_67d_user as 60d_all_cate_last_amt,
    last(TRAN_DATE) over 7d_67d_user as 60d_all_cate_last_date,
    last(IF(pro_id >= 1 and pro_id <= 4, amt, 0)) over 7d_67d_user as 60d_current_last_amt,
    last(IF(pro_id >= 1 and pro_id <= 4, TRAN_DATE, 0)) over 7d_67d_user as 60d_current_last_date,
    last(IF(pro_id >= 5 and pro_id <= 8, amt, 0)) over 7d_67d_user as 60d_regular_last_amt,
    last(IF(pro_id >= 5 and pro_id <= 8, TRAN_DATE, 0)) over 7d_67d_user as 60d_regular_last_date,
    last(IF(pro_id >= 9 and pro_id <= 12, amt, 0)) over 7d_67d_user as 60d_financing_last_amt,
    last(IF(pro_id >= 9 and pro_id <= 12, TRAN_DATE, 0)) over 7d_67d_user as 60d_financing_last_date,
    last(IF(pro_id >= 13 and pro_id <= 16, amt, 0)) over 7d_67d_user as 60d_fund_last_amt,
    last(IF(pro_id >= 13 and pro_id <= 16, TRAN_DATE, 0)) over 7d_67d_user as 60d_fund_last_date

from trans
window 7d_37d_user as (partition by user_id order by unix_timestamp(TRAN_DATE,'yyyy-MM-dd') range between  3196800 preceding and 604800 preceding),
    7d_67d_user as (partition by user_id order by unix_timestamp(TRAN_DATE,'yyyy-MM-dd') range between  5788800 preceding and 604800 preceding),
    7d_37d_prd as (partition by int(int(pro_id) / 4) order by unix_timestamp(TRAN_DATE,'yyyy-MM-dd') range between  3196800 preceding and 604800 preceding),
    7d_67d_prd as (partition by int(int(pro_id) / 4) order by unix_timestamp(TRAN_DATE,'yyyy-MM-dd') range between  5788800 preceding and 604800 preceding)
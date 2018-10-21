from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import GradientBoostedTrees
from pyspark.mllib.linalg import SparseVector
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import MapType,IntegerType,StringType,DataType
from pyspark.sql.types import StructType,StructField
from pyspark.ml.linalg import Matrices
from pyspark.mllib.util import MLUtils
import pandas

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)
sqlc = SQLContext(sc)

print(StructField , "go go go")

pdline = pandas.read_csv("/Users/xiejinxin/datafloder/test/copy.csv")

spkdf = sqlc.createDataFrame(pdline)

print(pdline)

print(spkdf)
spkdf.createOrReplaceTempView("tmp1")
data = sqlc.sql("select row_number() over(order by relation_year) as id, cust_level,relation_year,sex,age,cust_status,is_xyk_kk,eva_bal_rmb,raroc_bal_rmb,cnt,transamt,liucun_bal,aum_bal,s_aum_bal,h_aum_bal,d_aum_bal,loan_bal,if(finance_bal > 0,1,0) as label from tmp1")

data.show()
tran = data.rdd.map(lambda x:LabeledPoint(list(x)[17], SparseVector(16, [i for i in range(16)],list(x)[1:17])))
#cust_level,relation_year,sex,age,eva_bal_rmb,cnt,transamt,liucun_bal
#cust_level,relation_year,sex,age,cust_status,is_xyk_kk,degree,eva_bal_rmb,raroc_bal_rmb,cnt,transamt,liucun_bal,aum_bal,s_aum_bal,h_aum_bal,d_aum_bal,loan_bal,finance_bal,finance_bal_bb,finance_bal_fbb,invest_bal,ldjj_bal,gz_aum_bal,b_aum_bal,gold_bal,trust_bal,insurance_bal,third_bal,loan_house_bal,loan_car_bal,loan_mana_bal,loan_stuty_bal,loan_other_bal,ola_aum_bal,b_z_cd_aum_bal,loan_z_cd,zhc_aum_bal,jer_bal,dly_bal,hxlc_bal,jeqj_bal,jegd_bal,jewy_bal,dzzh_bal,decd_bal,xfc_aum_bal,jj_tot_vol,card_xy_bal_last_m_avg,card_xy_bal_last_m_avg_y,card_swing_bal_avg,card_swing_bal_avg_y,card_swing_num_avg,card_swing_num_avg_y,corpname,tran_amt_1m,tran_num_1m,tran_amt_3m,tran_num_3m,tran_amt_6m,tran_num_6m,day_cnt,tran_wy_amt_1m,tran_wy_num_1m,tran_wy_amt_3m,tran_wy_num_3m,tran_wy_amt_6m,tran_wy_num_6m,day_wy_cnt,tran_dz_amt_1m,tran_dz_num_1m,tran_dz_amt_3m,tran_dz_num_3m,tran_dz_amt_6m,tran_dz_num_6m,day_dz_cnt,tran_atm_amt_1m,tran_atm_num_1m,tran_atm_amt_3m,tran_atm_num_3m,tran_atm_amt_6m,tran_atm_num_6m,day_atm_cnt,tran_gt_amt_1m,tran_gt_num_1m,tran_gt_amt_3m,tran_gt_num_3m,tran_gt_amt_6m,tran_gt_num_6m,day_gt_cnt,tran_pos_amt_1m,tran_pos_num_1m,tran_pos_amt_3m,tran_pos_num_3m,tran_pos_amt_6m,tran_pos_num_6m,day_pos_cnt,tran_sj_amt_1m,tran_sj_num_1m,tran_sj_amt_3m,tran_sj_num_3m,tran_sj_amt_6m,tran_sj_num_6m,day_sj_cnt,tran_dh_amt_1m,tran_dh_num_1m,tran_dh_amt_3m,tran_dh_num_3m,tran_dh_amt_6m,tran_dh_num_6m,day_dh_cnt,is_despoit,is_fixed,is_finance,is_fund,is_gz_aum,is_insurance,is_gold,is_third,is_trust,is_loan,is_cbank,is_xyk,is_finance_bb,is_finance_fbb,is_ldjj,is_loan_house,is_loan_car,is_loan_mana,is_loan_stuty,is_loan_other,is_ola_aum,is_zhc_aum,is_jer,is_dly,is_hxlc,is_jeqj,is_jewy,is_decd,is_xfc_aum,'

model = GradientBoostedTrees.trainRegressor(tran, {}, numIterations=10)

model.save(sc,"./gbdymodelonlionev1")

a = StructType([
    StructField("ID",StringType(),False),
    StructField("cust_type", StringType(), True),
    StructField("cust_level", IntegerType(), True)
])

print(a)
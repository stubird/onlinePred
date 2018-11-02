import pandas as pd
import numpy as np
from numpy import random
months = ['1m','2m','3m']
days = ['30d_user','60d_user','30d_prd','60d_prd']
days_gap = ['37d_user','67d_user','37d_prd','67d_prd']
preduct_cate = ['all_cate','current','regular','financing','fund']
agg = ['avg','max','min','mean']

def create1():
    for i in months:
        for j in preduct_cate:
            for k in agg:
                print("{}_{}_{} = random.normal(20000,5000,lenght)".format(i,j,k))

def create_sql():
    for i,s in enumerate(days):
        for j in preduct_cate:
            for k in agg:
                wher = ""
                if j != "all_cate":
                    if j == "current":
                        wher = "IF(pro_id >= 1 and pro_id <= 4, amt, 0)"
                    elif j == "regular":
                        wher = "IF(pro_id >= 5 and pro_id <= 8, amt, 0)"
                    elif j == "financing":
                        wher = "IF(pro_id >= 9 and pro_id <= 12, amt, 0)"
                    elif j == "fund":
                        wher = "IF(pro_id >= 13 and pro_id <= 16, amt, 0)"
                else:
                    wher = "amt"
                print("{}({}) over 7d_{} as {}_{}_{},".format(k,wher,days_gap[i],s, j, k))
    for i in preduct_cate:
        for j in ['amt','TRAN_DATE']:
            wher = ""
            if i != "all_cate":
                if i == "current":
                    wher = "IF(pro_id >= 1 and pro_id <= 4, {}, 0)".format(j)
                elif i == "regular":
                    wher = "IF(pro_id >= 5 and pro_id <= 8, {}, 0)".format(j)
                elif i == "financing":
                    wher = "IF(pro_id >= 9 and pro_id <= 12, {}, 0)".format(j)
                elif i == "fund":
                    wher = "IF(pro_id >= 13 and pro_id <= 16, {}, 0)".format(j)
            else:
                wher = j
            print("last({}) over 7d_67d_user as 60d_{}_last,".format(wher, i))

def create_prd():
    ProdId = np.arange(16) + 1
    buy_price = 1000 + 100*np.arange(16)
    interest = 3.2 + (1 + np.arange(16)) / 16
    term = 12 + random.randint(0,7,16)
    prd = pd.DataFrame([ProdId,buy_price,interest,term], index=['ProdId', 'buy_price', 'interest', 'term']).T
    print(prd)
    #prd.to_csv("./prd_data.txt")

#create_sql()
create_prd()
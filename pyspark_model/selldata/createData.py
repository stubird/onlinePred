from numpy import random
import numpy as np
import pandas as pd

userBuy = []

def createData(lenght = 10000, plen = 20):
    ID = np.arange(lenght)
    gender = random.randint(0,2,lenght)
    age = random.randint(14,100,lenght)
    deposit = random.normal(20000,5000,lenght)

    user = pd.DataFrame([ID,gender,age,deposit], index=['ID', 'gender', 'age', 'deposit']).T

    ProdId = np.arange(plen)
    buy_price = 1000 + 100*np.arange(plen)
    interest = 3.2 + (1 + np.arange(plen)) / plen
    term = 12 + random.randint(0,7,plen)

    prd = pd.DataFrame([ProdId,buy_price,interest,term], index=['ProdId', 'buy_price', 'interest', 'term']).T

    #product
    user['value'] = 1
    prd['value'] = 1
    merge_ret = user.merge(prd, on='value', how='left')
    del merge_ret['value']
    return merge_ret

def signLabel(df):
    df['label'] = df.apply(lambda x:1 if (
                x.age > 18 and x.age < 35 and x.deposit > 15000 and x.deposit < 300000 and
                x.ProdId <= 3) or (
                x.age >= 35 and x.age < 55 and x.gender == 1 and x.deposit > 35000 and x.deposit < 300000 and
                x.ProdId <= 10 and x.ProdId > 6) else 0
              , axis = 1)

    return  df

df = signLabel(createData(20000,10))
df.to_csv('selldata')
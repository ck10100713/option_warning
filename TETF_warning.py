import playsound
import datetime as dt
import redis
import pandas as pd
import numpy as np
import sys
import tetrion.db
import json
sys.path.append('/nfs/public_data/optiontrading/scripts/')
import tetrion_backfill_redis_fills
import redis
import time

def get_fills_from_redis(acc='WKF_SHX870',date=dt.date.today(),night_session=False, redishost='prod2.hk'):
    r = redis.StrictRedis(host=redishost, port=6379,db=0)
    rk = '{}:{}'.format(acc, date.strftime('%Y%m%d'))
    if night_session:
        rk = rk + 'E'
    if rk not in [k.decode() for k in r.keys()]:
        #print('Key:{} not exists!'.format(rk))
        return pd.DataFrame()
    msgs = r.lrange(rk, 0, -1)
    trd_df = pd.DataFrame([json.loads(m.decode()) for m in msgs])
    if trd_df.empty:
        #print('Empty redis key')
        return None
    if trd_df['ts'][0] > 1e11:
        trd_df['time'] = trd_df['ts'].apply(lambda x:dt.datetime.fromtimestamp(x/1e6))
    else:
        trd_df['time'] = trd_df['ts'].apply(lambda x:dt.datetime.fromtimestamp(x))
    trd_df.set_index('time',inplace=True)
    return trd_df

redis_key = {}
option_list = ['teo', 'tfo', 'tgo', 'nyo', 'oao', 'obo', 'ojo', 'oko', 'ooo', 'cco', 'cdo', 'cho', 'ddo', 'dqo', 'cko', 'ceo', 'dho'] #delete oco, nzo
for name in option_list:
    if name == 'teo' or name == 'tfo':
        redis_key[name] = 'capital_{}_main'.format(name)
    else:
        redis_key[name] = 'capital_{}_mm'.format(name)
# redis_key = {'teo':'capital_teo_main', 'tfo':'capital_tfo_main', 'tgo':'capital_tgo_mm', 'nyo':'capital_nyo_mm', 'oao':'capital_oao_mm', 'obo':'capital_obo_mm', 'ojo':'capital_ojo_mm', 'oko':'capital_oko_mm', 'ooo':'capital_ooo_mm', 'cco':'capital_cco_mm', 'cdo':'capital_cdo_mm', 'cho':'capital_cho_mm', 'ddo':'capital_ddo_mm', 'dqo':'capital_dqo_mm'}
sound = {}
trade = dict()
day = dt.date.today()

for ins in option_list:
    trade[ins] = len(get_fills_from_redis(redis_key[ins], day, redishost='prod1.capital.radiant-knight.com', night_session=False))
print('start')
print('{} is ready'.format(option_list))
while dt.datetime.now().time() > dt.time(8,20,0) and dt.datetime.now().time() < dt.time(16,15,0):
    for ins in option_list:
        try:
            if trade[ins] != len(get_fills_from_redis(redis_key[ins], day, redishost='prod1.capital.radiant-knight.com', night_session=False)):
                playsound.playsound(ins + '.mp3')
                trade[ins] = len(get_fills_from_redis(redis_key[ins], day, redishost='prod1.capital.radiant-knight.com', night_session=False))
        except:
            pass
    time.sleep(10)



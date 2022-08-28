##

"""

    """

import pandas as pd
from githubdata import GithubData
from mirutil.df_utils import read_data_according_to_type as rdata


allcod_rp_url = 'https://github.com/imahdimir/d-all-Codal-Letters'
t2b_rp_url = 'https://github.com/imahdimir/d-Ticker-2-BaseTicker-map'
c2b_rp_url = 'https://github.com/imahdimir/d-CodalTicker-2-BaseTicker-map'
bt_rp_url = 'https://github.com/imahdimir/d-uniq-BaseTickers'

cur_rp_url = 'https://github.com/imahdimir/make-CodalTicker-2-BaseTicker-map'

tran = 'TracingNo'
codtic = 'CodalTicker'
cname = 'CompanyName'
ltrcod = 'LetterCode'
title = 'Title'
pjdt = 'PublishDateTime'
isest = 'IsEstimate'
tic = 'Ticker'
tid = 'TSETMC_ID'
btic = 'BaseTicker'

def main() :
  pass

  ##

  rp_ac = GithubData(allcod_rp_url)
  ##
  rp_ac.clone()
  ##
  dafp = rp_ac.data_filepath
  da = rdata(dafp)
  ##
  da = da[[codtic]].drop_duplicates()
  da = da.dropna()
  ##
  t2b_rp = GithubData(t2b_rp_url)
  t2b_rp.clone()
  ##
  dftp = t2b_rp.data_filepath
  dft = mf.read_data_according_to_type(dftp)

  ##
  da[btic] = da[codtic].map(dft[btic])

  ##
  msk = da[btic].isna()
  len(msk[msk])

  ##
  da['1'] = da[codtic].apply(mf.normalize_fa_str)

  ##
  dft = dft.reset_index()

  dft['1'] = dft[tic].apply(mf.normalize_fa_str)

  dft = dft.set_index('1')

  ##
  msk = da[btic].isna()
  da.loc[msk , btic] = da.loc[msk , '1'].map(dft[btic])

  ##
  msk = da[btic].isna()
  df1 = da[msk]
  len(msk[msk])

  ##
  bt_rp = GithubData(bt_rp_url)
  bt_rp.clone()

  ##
  dfbp = bt_rp.data_filepath
  dfb = mf.read_data_according_to_type(dfbp)

  ##
  dfb['i'] = dfb[btic]
  dfb = dfb.set_index('i')

  ##
  msk = da[btic].isna()
  da.loc[msk , btic] = da.loc[msk , codtic].map(dfb[btic])

  ##
  msk = da[btic].isna()
  df1 = da[msk]
  len(msk[msk])

  ##
  msk = da[btic].isna()
  da.loc[msk , btic] = da.loc[msk , '1'].map(dfb[btic])

  ##
  msk = da[btic].isna()
  df1 = da[msk]
  len(msk[msk])

  ##
  dfb = dfb.reset_index(drop = True)
  dfb['1'] = dfb[btic].apply(mf.normalize_fa_str)
  dfb = dfb.set_index('1')

  ##
  msk = da[btic].isna()
  da.loc[msk , btic] = da.loc[msk , '1'].map(dfb[btic])

  ##
  msk = da[btic].isna()
  df1 = da[msk]
  len(msk[msk])

  ##
  da = da[[codtic , btic]]
  da = da.dropna()
  da = da.set_index(codtic)

  ##
  c2b_rp = GithubData(c2b_rp_url)
  c2b_rp.clone()

  ##
  dfcp = c2b_rp.data_filepath

  ##
  mf.save_as_prq_wo_index(da , dfcp)

  ##
  msg = 'v1'
  msg += 'by: ' + cur_rp_url

  c2b_rp.commit_push(msg)

  ##

  rp_ac.rmdir()
  c2b_rp.rmdir()
  t2b_rp.rmdir()
  bt_rp.rmdir()

##
##
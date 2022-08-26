##

"""

    """

import pandas as pd
from githubdata import GithubData
from mirutil import funcs as mf


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


  ac_rp = GithubData(allcod_rp_url)

  ##
  ac_rp.clone()

  ##
  dfap = ac_rp.data_filepath
  dfa = mf.read_data_according_to_type(dfap)

  ##
  dfa = dfa[[codtic]].drop_duplicates()
  dfa = dfa.dropna()

  ##
  t2b_rp = GithubData(t2b_rp_url)
  t2b_rp.clone()

  ##
  dftp = t2b_rp.data_filepath
  dft = mf.read_data_according_to_type(dftp)

  ##
  dfa[btic] = dfa[codtic].map(dft[btic])

  ##
  msk = dfa[btic].isna()
  len(msk[msk])

  ##
  dfa['1'] = dfa[codtic].apply(mf.normalize_fa_str)

  ##
  dft = dft.reset_index()

  dft['1'] = dft[tic].apply(mf.normalize_fa_str)

  dft = dft.set_index('1')

  ##
  msk = dfa[btic].isna()
  dfa.loc[msk , btic] = dfa.loc[msk , '1'].map(dft[btic])

  ##
  msk = dfa[btic].isna()
  df1 = dfa[msk]
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
  msk = dfa[btic].isna()
  dfa.loc[msk , btic] = dfa.loc[msk , codtic].map(dfb[btic])

  ##
  msk = dfa[btic].isna()
  df1 = dfa[msk]
  len(msk[msk])

  ##
  msk = dfa[btic].isna()
  dfa.loc[msk , btic] = dfa.loc[msk , '1'].map(dfb[btic])

  ##
  msk = dfa[btic].isna()
  df1 = dfa[msk]
  len(msk[msk])

  ##
  dfb = dfb.reset_index(drop = True)
  dfb['1'] = dfb[btic].apply(mf.normalize_fa_str)
  dfb = dfb.set_index('1')

  ##
  msk = dfa[btic].isna()
  dfa.loc[msk , btic] = dfa.loc[msk , '1'].map(dfb[btic])

  ##
  msk = dfa[btic].isna()
  df1 = dfa[msk]
  len(msk[msk])

  ##
  dfa = dfa[[codtic , btic]]
  dfa = dfa.dropna()
  dfa = dfa.set_index(codtic)

  ##
  c2b_rp = GithubData(c2b_rp_url)
  c2b_rp.clone()

  ##
  dfcp = c2b_rp.data_filepath

  ##
  mf.save_as_prq_wo_index(dfa , dfcp)

  ##
  msg = 'v1'
  msg += 'by: ' + cur_rp_url

  c2b_rp.commit_push(msg)

  ##

  ac_rp.rmdir()
  c2b_rp.rmdir()
  t2b_rp.rmdir()
  bt_rp.rmdir()

##
##
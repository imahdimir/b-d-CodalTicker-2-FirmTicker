##

"""

    """

import pandas as pd
from githubdata import GithubData
from mirutil import funcs as mf
from persiantools import characters


allcod_rp_url = 'https://github.com/imahdimir/d-all-Codal-Letters'
t2b_rp_url = 'https://github.com/imahdimir/d-Ticker-2-BaseTicker-map'
bt_rp_url = 'https://github.com/imahdimir/d-uniq-BaseTickers'

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
  ac_rp.clone()

  ##
  adfp = ac_rp.data_filepath
  adf = pd.read_parquet(adfp)

  ##
  t2b_rp = GithubData(t2b_rp_url)
  t2b_rp.clone()

  ##
  ctdf = adf[[codtic]].drop_duplicates()

  ##
  ctdf = ctdf.dropna()

  ##
  bdf = pd.read_excel(t2b_rp.data_filepath)
  bdf['ind'] = bdf[btic]
  bdf = bdf.set_index('ind')

  ##
  ctdf[btic] = ctdf[codtic].map(bdf[btic])

  ##
  msk = ctdf[btic].isna()
  len(msk[msk])

  ##
  fu = characters.ar_to_fa
  ctdf['1'] = ctdf[codtic].apply(fu)

  ##
  bdf['1'] = bdf[btic].apply(fu)

  ##
  bdf = bdf.reset_index()
  bdf = bdf.set_index('1')

  ##
  msk = ctdf[btic].isna()
  ctdf.loc[msk , btic] = ctdf.loc[msk , '1'].map(bdf[btic])

  ##
  msk = ctdf[btic].isna()
  len(msk[msk])
  df1 = ctdf[msk]

  ##


  ##


  ##
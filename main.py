"""
Makes a map between Codal Tickers and Firm Tickers
  - Gets all Codal Tickers from d-all-Codal-Letters data repository
  - keeps unique ones
  - Gets all Firm Tickers from d-id2ft data repository
  - Maps exact matches
  - applies nfas to Codal Tickers, and Firm Tickers
  - Maps not mathced Codal Tickers if they exactly match after nfas
  - Gets all multi Basetickers which are maps to same Firm Ticker
  - Maps exact matches to same firmticker

  - overwrites d-targ data.prq by builded one
  - commits and pushes to d-targ data repository as a prq file
    """
##
import pandas as pd
from githubdata import GithubData
from mirutil.df_utils import read_data_according_to_type as rdata
from mirutil.string_funcs import normalize_fa_str_completely as nfas
from mirutil import utils as mu
from mirutil import df_utils as dfu


allcodal_url = 'https://github.com/imahdimir/d-all-Codal-Letters'
id2ft_url = 'https://github.com/imahdimir/d-TSETMC_ID-2-FirmTicker'
mb2ft_url = 'https://github.com/imahdimir/d-multi-BaseTickers-2-same-FirmTicker'

targ_url = 'https://github.com/imahdimir/d-CodalTicker-2-FirmTicker'
cur_url = 'https://github.com/imahdimir/b-d-CodalTicker-2-FirmTicker'

ftic = 'FirmTicker'
ctic = 'CodalTicker'
btic = 'BaseTicker'

def main() :
  pass

  ##

  rp_ac = GithubData(allcodal_url)
  ##
  rp_ac.clone()
  ##
  dafp = rp_ac.data_fp
  da = rdata(dafp)
  ##
  da = da[[ctic]].drop_duplicates()
  da = da.dropna()
  ##

  rp_i2f = GithubData(id2ft_url)
  ##
  rp_i2f.clone()
  ##
  dftp = rp_i2f.data_fp
  dft = rdata(dftp)
  ##
  dft = dft[[ftic]].drop_duplicates()
  ##
  dft['i'] = dft[ftic]
  dft = dft.set_index('i')
  ##
  da[ftic] = da[ctic].map(dft[ftic])
  ##
  msk = da[ftic].isna()
  len(msk[msk])
  ##
  da['1'] = da[ctic].apply(nfas)
  ##
  dft['1'] = dft[ftic].apply(nfas)
  dft = dft.set_index('1')
  ##
  msk = da[ftic].isna()
  da.loc[msk , ftic] = da.loc[msk , '1'].map(dft[ftic])
  ##
  msk = da[ftic].isna()
  df1 = da[msk]
  len(msk[msk])
  ##
  msk = ~ dft[ftic].isin(da[ftic])
  df2 = dft[msk]

  ##
  da = da[[ctic , ftic]]

  ##
  rp_mb2ft = GithubData(mb2ft_url)
  rp_mb2ft.clone()
  ##
  dmp = rp_mb2ft.data_fp
  dm = rdata(dmp)
  ##
  dm = dm[[btic , ftic]].drop_duplicates()
  ##
  dm = dm.set_index(btic)
  ##
  msk = da[ftic].isna()
  da.loc[msk , ftic] = da.loc[msk , ctic].map(dm[ftic])
  ##
  msk = da[ftic].isna()
  df1 = da[msk]
  len(msk[msk])
  ##
  dm = dm.reset_index()
  dm['1'] = dm[btic].apply(nfas)
  dm = dm.set_index('1')
  ##
  da['1'] = da[ctic].apply(nfas)
  ##
  msk = da[ftic].isna()
  da.loc[msk , ftic] = da.loc[msk , '1'].map(dm[ftic])
  ##
  msk = da[ftic].isna()
  df1 = da[msk]
  len(msk[msk])

  ##
  da = da[[ctic , ftic]]
  da = da.dropna()

  ##
  rp_targ = GithubData(targ_url)
  rp_targ.clone()

  ##
  dtp = rp_targ.data_fp
  dfu.save_as_prq_wo_index(da , dtp)

  ##
  tokp = '/Users/mahdi/Dropbox/tok.txt'
  tok = mu.get_tok_if_accessible(tokp)

  ##
  msg = 'builded'
  msg += ' by: ' + cur_url

  rp_targ.commit_and_push(msg , user = rp_targ.user_name , token = tok)

  ##


  rp_ac.rmdir()
  rp_i2f.rmdir()
  rp_mb2ft.rmdir()
  rp_targ.rmdir()

  ##

  ##

##


if __name__ == '__main__' :
  main()


##
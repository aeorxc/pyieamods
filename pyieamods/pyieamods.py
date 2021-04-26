import pandas as pd
import os
import dask
import zipfile

names1 = ['commod', 'region', 'balitem', 'date', 'value']
names2 = ['region', 'commod', 'date', 'value']
names3 = ['balitem', 'region', 'commod', 'date', 'value']
names4 = ['region', 'date', 'value']
names5 = ['balitem', 'type', 'date', 'value']

filenames = ['CRUDEDAT.TXT', 'NOECDDE.TXT', 'OECDDE.TXT', 'PRODDAT.TXT', 'STOCKDAT.TXT', 'SUMMARY.TXT', 'SUPPLY.TXT']


sdbstxt_mapping = {
    'PRODDAT.TXT' : { 'prefix' : 'IEA.PROD', 'col_headers' : ['commod', 'region', 'balitem', 'date', 'value'] },
    'CRUDEDAT.TXT' : { 'prefix' : 'IEA.CRUDE', 'col_headers' : ['commod', 'region', 'balitem', 'date', 'value'] },
    'OECDDE.TXT' : { 'prefix' : 'IEA.OECDDE', 'col_headers' : ['region', 'commod', 'date', 'value'] },
    'NOECDDE.TXT' : { 'prefix' : 'IEA.NOECDDE', 'col_headers' : ['region', 'date', 'value'] },
    'SUMMARY.TXT' : { 'prefix' : 'IEA.SUMMARY', 'col_headers' : ['balitem', 'type', 'date', 'value'] },
    'STOCKDAT.TXT' : { 'prefix' : 'IEA.STOCKS', 'col_headers' : ['balitem', 'region', 'commod', 'date', 'value'] },
    'SUPPLY.TXT' : { 'prefix' : 'IEA.SUPPLY', 'col_headers' : ['region', 'commod', 'date', 'value'] },
}


def allmods(sdbstxt_zip_loc:str, raw:bool=False):
    z = zipfile.ZipFile(sdbstxt_zip_loc)
    res = []
    # filenames = ['PRODDAT.TXT']
    for filename in filenames:
        res.append(dask.delayed(read_filename(z, filename, raw)))

    res = dask.compute(*res)
    res = {x.attrs['filename']:x for x in res}
    return res


def to_excel(allmods, outp):
    for dataset in allmods:
        outpf = os.path.join(outp, '{}.xlsx'.format(dataset))
        allmods[dataset].to_excel(outpf)


def read_filename(z:str, filename:str, raw:bool=False) -> pd.DataFrame:
    col_headers = sdbstxt_mapping[filename]['col_headers']
    prefix = sdbstxt_mapping[filename]['prefix']
    df = pd.read_csv(z.open(filename), sep='\s+', header=None, names=col_headers, parse_dates=['date'])

    if raw:
        df.attrs['filename'] = filename
        return df

    if filename == 'NOECDDE.TXT':
        df['series'] = df.apply(lambda x: 'IEA.NOECDDE.{}'.format(x.region), 1)
    elif filename == 'OECDDE.TXT':
        df['series'] = df.apply(lambda x: 'IEA.OECDDE.{}.{}'.format(x.commod, x.region), 1)
    elif filename == 'STOCKDAT.TXT':
        df['series'] = df.apply(lambda x: 'IEA.STOCKS.{}.{}.{}'.format(x.balitem, x.commod, x.region,  ),1)
    elif filename == 'SUMMARY.TXT':
        df['series'] = df.apply(lambda x: 'IEA.SUMMARY.{}.{}'.format(x.balitem, x.type), 1)
    elif filename == 'SUPPLY.TXT':
        df['series'] = df.apply(lambda x: 'IEA.SUPPLY.{}.{}'.format(x.commod, x.region), 1)
    else:
        df['series'] = df.apply(lambda x: '%s.%s.%s.%s' % (prefix, x[col_headers[0]], x[col_headers[1]], x[col_headers[2]]), 1)
    df = df.groupby(['date', 'series']).mean().unstack()['value']
    df.attrs['filename'] = filename
    return df


def get_series(series_name: str, source_loc:str) -> pd.DataFrame:
    mods = allmods(source_loc)
    prefixmap = {sdbstxt_mapping[x]['prefix']:x for x in sdbstxt_mapping }
    prefix = '.'.join(series_name.split('.')[0:2])

    res = mods[prefixmap[prefix]][[series_name]]
    return res

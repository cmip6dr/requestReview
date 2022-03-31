import os
#
# analysing csv file doenloaded from ~http://esgf-ui.cmcc.it/esgf-dashboard-ui/cmip6.html
#


ifile = 'cmip6-variables_gb_20220331.csv'

ee = {}

class Scanner(object):
    def __init__(self,ifile):
        assert os.path.isfile(ifile), '%s not found' % ifile
        ii = [x.strip().split(',') for x in open(ifile).readlines()]
        print  (ii[0])
        for l in ii[1:]:
            if len(l) > 4:
                ee['%s.%s' % (l[0],l[-3]) ] = (float( l[-2] ), int(l[-1] ) )

        by_size = sorted( list(ee.keys()), key=lambda x:ee[x][0], reverse=True )
        by_count = sorted( list(ee.keys()), key=lambda x:ee[x][1], reverse=True )
        for k in by_size[:50]:
            print( '%s: %s' % (k,ee[k] ) )
        print ( '===============================\n\n' )
        for k in by_count[:50]:
            print( '%s: %s' % (k,ee[k] ) )


if __name__ == "__main__":
    s = Scanner(ifile)


import collections
from dreqPy import dreq
dq = dreq.loadDreq()

class DreqNS(object):
    def __init__(self, dq):
        self.dq=dq
        self.cc = collections.defaultdict( set )
        for i in self.dq.coll['CMORvar'].items:
            self.cc[(i.label,i.frequency)].add(i)

        self.ks = collections.defaultdict( set )

        for lf,i in self.cc.items():
            self.ks[len(i)].add(lf)

        for k in sorted( list( self.ks.keys() ) ):
            this = self.ks[k]
            print (k,len(this))
            tsets = collections.defaultdict( set )
            if k > 1:
              for lf in this:
                s1 = {i.mipTable for i in self.cc[lf]}
                tsets[ tuple( sorted( list( s1 ) ) ) ].add( lf[0] )
              print ( k,  len(tsets) )
              for k1,xx in tsets.items():
                  print (' --- ',k1,xx)

ns = DreqNS(dq)


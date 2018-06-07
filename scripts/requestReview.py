
import collections, xlsxwriter

from dreqPy import dreq


class rev01(object):
  def __init__(self):
    self.dq = dreq.loadDreq()

    self.exptix = dict()
    self.varix = dict()
    self.cmvix = dict()

    for i in self.dq.coll['experiment'].items:
      self.exptix[i.label] = i.uid

    for i in self.dq.coll['var'].items:
      self.varix[i.label] = i.uid

    for i in self.dq.coll['CMORvar'].items:
      self.cmvix[(i.mipTable,i.label)] = i.uid


  def reva(self):

# expand experiment label list to set of uid's of experiments + mips + experiment groups which, if referenced by esid attribute, imply a request from these experiments.
    eset = set( ['historical','piControl'] )
    esetx = set()
    for lab in eset:
      u = self.exptix[lab]
      esetx.add(u)
      i = self.dq.inx.uid[u]
      esetx.add(i.mip)
      esetx.add(i.egid)

## find set of ScenarioMIP experiments and experiment groups
    esetr = set(['ScenarioMIP',])
    i = self.dq.inx.uid['ScenarioMIP']
    for u in self.dq.inx.iref_by_sect[i.uid].a['experiment']:
      esetr.add(u)
      esetr.add(self.dq.inx.uid[u].egid)
    
## find requestLink items which link to an experiment in eset.
    rql01 = self._eset_to_rqlset(esetx)
    rqlScen = self._eset_to_rqlset(esetr)

## for each variable, collect names of mips requesting it
    cc = self._rqlset_to_varmipdict(rql01)
    self.ccScen = self._rqlset_to_varmipdict(rqlScen)

## rank variables
    ee = collections.defaultdict( set )
    for i in self.dq.coll['CMORvar'].items:
      t = (i.mipTable,i.label)
      ee[len(cc[t])].add( t )

    ks = sorted( ee.keys() )
    ks.reverse()

    kac = 0
    ll = len( self.dq.coll['CMORvar'].items )
    fac = 100./ll
    for k in ks:
      kac += len( ee[k] )
      print '%2s: %4s (%6.2f%%)' % (k, len(ee[k]), kac*fac)


    for k in ks:
      vv = collections.defaultdict( set )
      print '%s:: Request Rank %s' % (k,k)
      for t in ee[k]:
        vv[t[0]].add(t[1])
      for t in sorted( vv.keys() ):
        print '%s:: %s' % (t, ','.join( sorted( vv[t] ) ) )

    self.rankedList = ee
    self.dumpXlsx( 'test' )

  def dumpXlsx(self,fn):
    wb = xlsxwriter.Workbook('%s.xlsx' % fn)
    s = wb.add_worksheet(name='Info')
    s.write( 0,0, 'Review of Data Request: Summary' )
    s.write( 1,0, 'Rank' )
    s.write( 1,1, 'Number of MIPs requesting variable for either historical or piControl' )
    s.write( 1,0, "Count" )
    s.write( 1,1, "Number of variables" )
    s.write( 2,0, "Cumulative %" )
    s.write( 2,1, "Cumulative number of variables, as a percentage" )
    s.write( 3,0, "Count*" )
    s.write( 3,1, "Number of variables which in this set which are already requested for ScenarioMIP experiments by VIACSAB" )
    s.write( 4,0, "Rank" )
    s.write( 4,1, "Count" )
    s.write( 4,2, "Cumulative" )
    s.write( 4,3, "Cumulative %" )
    s.write( 4,4, "Count*" )
    s.write( 4,5, "Cumulative*" )


    ks = sorted( self.rankedList.keys() )
    ks.reverse()
    kac = 0
    kvac = 0
    ll = len( self.dq.coll['CMORvar'].items )
    fac = 100./ll
    ir = 4
    for k in ks:
      kac += len( self.rankedList[k] )
      kv = len( [t for t in self.rankedList[k] if 'VIACSAB' in self.ccScen[t] ] )
      kvac += kv
      ir += 1
      s.write( ir,0, k )
      s.write( ir,1, len( self.rankedList[k] ) )
      s.write( ir,2, kac )
      s.write( ir,3, (kac*fac) )
      s.write( ir,4, kv )
      s.write( ir,5, kvac )

    for k in ks:
      s = wb.add_worksheet(name='Rank %s' % k)
      s.write( 0,0, "Table" )
      s.write( 0,1, "Variable" )
      ##vv = collections.defaultdict( set )
      tt = sorted( list( self.rankedList[k] ) )
      ir = 0
      for t in tt:
        ir += 1
        s.write( ir,0, t[0] )
        s.write( ir,1, t[1] )
        s.write( ir,2, 'VIACSAB' in self.ccScen[t] )
        s.write( ir,3, self.dq.inx.uid[ self.cmvix[t] ].title )

    wb.close()

  def _eset_to_rqlset(self,eset):
    """Find requestLink items which link to an experiment in eset.
       Input:
         eset: set of uid's of experiments, experiment groups and mips;
       Output:
         rqlset: set of uid's of requestLink records.
    """
    rqlset = set()
    for i in self.dq.coll['requestLink'].items:
      for u in self.dq.inx.iref_by_sect[i.uid].a['requestItem']:
        this = self.dq.inx.uid[u]
        if this.esid in eset:
          rqlset.add( i.uid )
    return rqlset

  def _rqlset_to_varmipdict(self,rqlset):
    """For each variable, collect names of mips requesting it:
     Input:
       rqlset: set of uid's of requestLink records;
     Output:
       cc: dictionary (collections.defaultdict instance): keys: tuple of (mipTable, label), values: set of MIPS.
    """
    cc = collections.defaultdict( set )
    for i in rqlset:
      rvg = self.dq.inx.uid[i].refid
      mip = self.dq.inx.uid[i].mip
      for u in self.dq.inx.iref_by_sect[rvg].a['requestVar']:
        this = self.dq.inx.uid[u]
        cmv = self.dq.inx.uid[this.vid]
        cc[(cmv.mipTable,cmv.label)].add( mip )
    return cc


r = rev01()
r.reva()

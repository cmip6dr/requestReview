
from dreqPy import dreq
dq = dreq.loadDreq()

TS = False
ST = True

if TS:
  oo=open('time_slice.csv', 'w')
  for i in dq.coll['timeSlice'].items:
      oo.write( '\t'.join( [str(x) for x in [i.uid,i.label,i.title,i.description,i.type,i.start,i.end,i.nyears,i.step,i.sliceLen,i.startList,i.sliceLenUnit]] ) +'\n' )
  oo.close()


if ST:
  oo=open('structure.csv', 'w')
  for i in dq.coll['structure'].items:
      lts = dq.inx.uid[i.tmid].label
      lsp = dq.inx.uid[i.spid].label
      coords = ''
      print ( type(i.cids) )
      if type(i.cids) == type(()):
          coords = ' | '.join( [dq.inx.uid[x].label for x in i.cids] )

      odims = ''
      if type(i.odims) == type(''):
          odims = i.odims
      oo.write( '\t'.join( [str(x) for x in [i.uid,i.label,i.title,i.description,i.cell_measures,i.cell_methods,lts,lsp,odims,coords,i.procNote]] ) +'\n' )
  oo.close()

from dreqPy import dreq
dq = dreq.loadDreq()

tmp = dict( Amon='tas, pr, uas, vas, huss, rsut, rsdt, rlut, rsus, rsds, rlus, rlds, ps, ua, va, zg, cl, clt'.split(', '),
            AERmon='od550aer, abs550aer'.split(', '),
            Lmon='cVeg, cLitter, gpp, nbp, npp'.split(', '),
            Omon=['fgco2',],
            Emon=['cSoil',],
            day='tas, pr, tasmax, tasmin'.split(', ')
           )



s1 = {(i.mipTable, i.label):i for i in dq.coll['CMORvar'].items }

oo = open( 'CovidMIP_priority_analysis_variables.csv', 'w' )
headers = 'Table Label Title standard_name uid str_lab str_title' 
oo.write( '\t'.join( headers.split() ) + '\n' )
for k,tt in tmp.items():
    for v in tt:
        if (k,v) not in s1:
            print ( 'NOT FOUND',(k,v) )
        else:
            i = s1[(k,v)]
            var = dq.inx.uid[i.vid]
            st = dq.inx.uid[i.stid]
            print( k,v,i.title,var.sn )
            oo.write( '\t'.join( [k,v,i.title,var.sn,i.uid,st.label, st.title] ) + '\n'  )
oo.close()

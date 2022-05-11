

ar6 = open( 'AR6_priority_variables_02.csv', 'r' ).readlines()
rr = open( 'RankedCmipVariables.csv', 'r' ).readlines()

ppp = [x.split('\t')[2].strip()  for x in ar6 if len(x.split('\t')) > 2] 
print( ppp[0] )
ss = set(ppp )

assert '3hr.huss' in ss
oo = open( 'p1_out.csv', 'w' )
for l in rr[1:]:
    id = l.split(',')[0].strip()
    oo.write( '%s, %s\n' % (id, id in ss ) )
oo.close()

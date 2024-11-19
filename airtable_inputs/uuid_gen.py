


import uuid

oo = open( '/tmp/uuid.csv', 'w' )


l = 5000

for i in range(l):
    oo.write( str( uuid.uuid1() ) + ',\n' )


oo.close()

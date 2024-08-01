import os
from pyairtable import Api
key = open('/home/mjuckes/Repositories/airtable_at_20240528','r').readlines()[0].strip()
keyr = open('/home/mjuckes/Repositories/airtable_read_key','r').readlines()[0].strip()
api = Api(key)
base = 'appkxuavdXCURU4NX'
baseline_climate_variables = 'tblthY3kwi35fqBnE'

class AirTableInfo(object):
    def __init__(self,key,base_identifiers):
        self.key = key
        self.identifiers = base_identifiers


base_opportunities = 'apphsJQW7SLT3gynr'
base_variables = 'appas2EDANenGxXdp'
base_mip_variables = 'appqRFkdpwAitEZNY'
base_schema = 'appjtjehkUOlKbGct'
table = api.table(keyr, baseline_climate_variables )

info = AirTableInfo( keyr, [base_opportunities,])

def get_opp():
    return Api(keyr)


class Table(object):
    def __init__(self, base, tid):
        self.base = base
        self.baseid = tid.base
        self.name = tid.name
        self.id   = tid.id
        self.t = tid
    def load(self):
        self.records = self.base.table(self.id).all()


def tostr(x):
    if x == None:
        return ""
    else:
        return str(x)

class Base(object):
    def __init__(self,api,bkey):
        self.api = api
        self.ids = {x.name:x.id for x in api.bases()}
        self.name = map2[bkey]
        bid = self.ids[self.name]
        self.identifier = bid
        self.base = api.base(bid)
        self.schema = self.base.schema()
        self.names = {x.id:x.name for x in api.bases()}

    def __repr__(self):
        print( self.schema )

    def dump_schema_tables(self,file='x.csv'):
        oo = open(file,'w')
        oo.write( '\t'.join( ['Base','Table','Description','ID'] ) + '\n' )
        for t in self.schema.tables:
                rec = [tostr(x) for x in [self.name,t.name,t.description,t.id]]
                
                print( rec)
                try:
                   oo.write( '\t'.join( rec ) + '\n' )
                except:
                    print( 'Failed write' )
        oo.close()

    def dump_schema_csv(self,file='x.csv'):
        oo = open(file,'w')
        oo.write( '\t'.join( ['Base','Table','Record','Description','Id','Type'] ) + '\n' )
        for t in self.schema.tables:
            for f in t.fields:
                rec = [self.name, t.name,] + [tostr(f.__dict__[x]) for x in ['name','description','id','type']]
                oo.write( '\t'.join( rec ) + '\n' )
        oo.close()


    def load(self,verbose=False):
        self.tables = [Table(self.base,x) for x in self.base.tables()]
        if verbose:
            for t in self.tables:
                print ('%s <%s>' % (t.name,t.id))

l1 = ['Data Request Opportunities', 'Data Request Variables', 'Data Request Physical Parameters', 'CMIP6 Review', 'CMIP7 Data Request Schema Source', 'Data Request Opportunities (Public)', 'Data Request Physical Parameters (Public)', 'Data Request Variables (Public)']
l2 = ['opp','var','par','rev','sch','oppp','parp','varp']

map1 = {l1[x]:l2[x] for x in range(8)}
map2 = {l2[x]:l1[x] for x in range(8)}
    
class Request(object):
    def __init__(self,info):
        self.api = Api( info.key )
        self.bases = {map1[x.name]:(x.name,x.id) for x in self.api.bases()}

r = Request(info)

class GetBaseline(object):
    def __init__(self):
      self.tab=table.all()
      self.ee = dict( mon=['Amon','SImon', 'Omon','Lmon'], day=['SIday','Eday', 'Oday', 'CFday', 'day'],fixed=['fx','Ofx','Efx'], subdaily=['E1hr', '6hrPlev', '6hrPlevPt', 'E3hr', '3hr'],annual=['Oyr',])


    def list(self,key):
        s = set()
        for r in self.tab:
            x = r['fields']['Label']
            if x.split('.')[0] in self.ee[key]:
                s.add(x)
        return ', '.join(sorted(list(s)))


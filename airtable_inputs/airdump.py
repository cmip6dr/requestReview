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


class Base(object):
    def __init__(self,api,bid):
        self.identifier = bid
        self.base = api.base(bid)
    def load(self,verbose=False):
        self.tables = [Table(self.base,x) for x in self.base.tables()]
        if verbose:
            for t in self.tables:
                print ('%s <%s>' % (t.name,t.id))

    
class Request(object):
    def __init__(self,info):
        self.api = Api( info.key )
        self.bases = [Base(self.api,x) for x in info.identifiers]

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


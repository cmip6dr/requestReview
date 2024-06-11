import os
from pyairtable import Api
key = open('/home/mjuckes/Repositories/airtable_at_20240528','r').readlines()[0].strip()
api = Api(key)
base = 'appkxuavdXCURU4NX'
baseline_climate_variables = 'tblthY3kwi35fqBnE'
table = api.table(base, baseline_climate_variables )


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


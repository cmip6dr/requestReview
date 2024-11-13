"""loadcf
------
The loadcf module reads the cf standard name table into two dictionaries:
  names[<standard_name>] = (description, canonical units)
  alias[<alias>] = standard_name
"""

import xml, xlsxwriter, uuid
import xml.dom, xml.dom.minidom

class DR(object):
    def __init__(self,file="CF Standard Names-dr20241113.csv"):
        ii = open( file, 'r' )
        self.nn = {}
        for l in ii.readlines()[1:]:
            bits = l.strip().split(',')
            name,uid = bits[0],bits[-1]
            self.nn[name] = uid

class CF(object):
  def __init__(self,file="../ing02/inputs/cf-standard-name-table_v48.xml"):
    vocabs = xml.dom.minidom.parse( file )

    el = vocabs.getElementsByTagName( 'entry' )
    elv = vocabs.getElementsByTagName( 'version_number' )[0]
    self.version = elv.firstChild.data
    self.names = {}
    self.alias = {}

    for e in el:
      sn = e.getAttribute('id')
      unitsE = e.getElementsByTagName( 'canonical_units' )
      u = ''
      if len(unitsE) > 0 and type( unitsE[0] ) != type(None):
        c1 = unitsE[0].firstChild
        if type( c1 ) != type(None):
          u = c1.data
      descE = e.getElementsByTagName( 'description' )
      d = ''
      if len(descE) > 0 and type( descE[0] ) != type(None):
        c1 = descE[0].firstChild
        if type( c1 ) != type(None):
          d = c1.data
    
      self.names[sn] = (d,u )
    el = vocabs.getElementsByTagName( 'alias' )
    for e in el:
      sn = e.getAttribute('id')
      x = e.getElementsByTagName( 'entry_id' )
      self.alias[sn] = x[0].firstChild.data

  def xlsx_record(self,row,rec):
      col = 0
      for r in rec:
                  self.worksheet.write(row, col, r)
                  col +=1

  def review(self,dr):
      revised = dict()
      new_in_cf = dict()
      for name in dr.nn.keys():
          if name == "isotope_ratio_of_2H_to_1H_in_sea_water_excluding_solutes_and_solids" or name[0] == "_":
              pass
          else:
            assert (name in self.names) or (name in self.alias), "%s not found in new name list" % name
            if name in self.alias:
                new_name = self.alias[name]
                revised[dr.nn[name]] = (new_name,name)
      targets = set( [v for k,v in self.alias.items()] )
      oo = open( 'revised_names.csv', 'w')
      for k,v in revised.items():
          oo.write( ','.join( [k,v[0],v[1]] ) + '\n' )
      oo.close()
      for name in self.names:
          if (name not in targets) and (name not in dr.nn):
              new_in_cf[str(uuid.uuid1())] = name
      oo = open( 'new_names.csv', 'w')
      for k,v in new_in_cf.items():
          oo.write( ','.join( [k,v] ) + '\n' )
      oo.close()


  def xlsx(self,file):
      oo = open(file,'w')
      workbook = xlsxwriter.Workbook(file)
      self.worksheet = workbook.add_worksheet()
      self.xlsx_record( 0, ['name','description','canonical units','is alias','target'] )
      keys = self.names.keys()
      row = 0
      for k in sorted( list( keys) ):
         row += 1
         self.xlsx_record( row, [k,self.names[k][0],self.names[k][1],'False',''] )
      for k in self.alias.keys():
         row += 1
         self.xlsx_record( row, [k,'','','True',self.alias[k]] )
      workbook.close()




  def csv(self,file):
      oo = open(file,'w')
      oo.write( '\t'.join(['name','description','canonical units','is alias','target']) + '\n' )
      keys = self.names.keys()
      for k in sorted( list( keys) ):
         oo.write( '\t'.join( [k,self.names[k][0],self.names[k][1],'False',''] ) + '\n' )
      for k in self.alias.keys():
         oo.write( '\t'.join( [k,'','','True',self.alias[k]] ) + '\n' )
      oo.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 2:
        if sys.argv[1] == 'review':
          ifile = sys.argv[2]
          c = CF(ifile)
          dr = DR()
          c.review(dr)
    elif len(sys.argv) > 1:
        ifile = sys.argv[1]
        c = CF(ifile)
    else:
        c = CF()
    print( 'version = %s' % c.version )

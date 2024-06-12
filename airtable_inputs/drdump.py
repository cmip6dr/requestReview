import xlsxwriter
 

from dreqPy import dreq
dq = dreq.loadDreq()

## coll keys: ['__core__', '__main__', '__sect__', 'requestVarGroup', 'requestItem', 'exptgroup', 'miptable', 'CMORvar', 'objective', 'spatialShape', 'requestLink', 'tableSection', 'modelConfig', 'varChoiceLinkC', 'objectiveLink', 'remarks', 'experiment', 'requestVar', 'standardname', 'varChoiceLinkR', 'var', 'mip', 'varChoice', 'temporalShape', 'structure', 'grids', 'timeSlice', 'cellMethods', 'tags', 'varRelations', 'varRelLnk', 'qcranges', 'places', 'transfers', 'units']

class MIP_Variable(object):
    def __init__(self):
        self.data = dq.coll['var']

    def dump(self,fn):
        hh = ['label', 'title', 'uid', 'sn','units','procnote','procComment','prov']
        oo = open(fn,'w')
        oo.write( '\t'.join(hh) + '\n' )
        for i in self.data.items:
            this = [i.__dict__[x] for x in hh]
            this[5] = ', '.join(this[5])
            print(this)
            oo.write( '\t'.join(this) + '\n' )
        oo.close()

class MIP_Table(object):
    def __init__(self):
        self.data = dq.coll['miptable']

    def dump(self,fn):
        hh = ['label', 'title', 'uid', 'description','altLabel','comment','frequency']
        oo = open(fn,'w')
        oo.write( '\t'.join(hh) + '\n' )
        for i in self.data.items:
            this = [i.__dict__[x] for x in hh]
            print(this)
            oo.write( '\t'.join(this) + '\n' )
        oo.close()

class CMOR_Variable(object):
    def __init__(self):
        self.data = dq.coll['CMORvar']

    def dump(self,fn):
        hh = ['label', 'title', 'uid', 'description','stid','vid','type','modeling_realm','positive','mipTableSection','mipTable','prov','provNote','frequency']
        oo = open(fn,'w')
        workbook = xlsxwriter.Workbook(fn)
        worksheet = workbook.add_worksheet()
 
# Start from the first cell.
# Rows and columns are zero indexed.
        row = 0
 
        content = ['Name',] + hh
 
# iterating through content list
        for i in self.data.items:
            column = 0
            this = [str(i.__dict__.get(x,'')) for x in hh]
            dis = this[3]
            dis = dis.replace("'",'"')
            dis = dis.replace('\t',' ')
            dis = dis.replace('  ',' ')
            this[4] = dq.inx.uid[this[4]].label
            this[5] = dq.inx.uid[this[5]].label
            this = ['%s.%s' % (this[10],this[0]),] + this
            print(this)
            for x in this:
 
    # write operation perform
                  worksheet.write(row, column, x)
                  column += 1
 
    # incrementing the value of row by one
    # with each iterations.
            row += 1
     
        workbook.close()

class Temporal_Shape(object):
    def __init__(self):
        self.data = dq.coll['temporalShape']

    def dump(self,fn):
        hh = ['label', 'title', 'uid', 'dimensions']
        oo = open(fn,'w')
        oo.write( '\t'.join(hh) + '\n' )
        for i in self.data.items:
            this = [str(i.__dict__[x]) for x in hh]
            this[3] = ', '.join(this[3].split('|'))
            print(this)
            oo.write( '\t'.join(this) + '\n' )
        oo.close()

class Spatial_Shape(object):
    def __init__(self):
        self.data = dq.coll['spatialShape']

    def dump(self,fn):
        hh = ['label', 'title', 'uid', 'dimensions', 'levels', 'levelFlag']
        oo = open(fn,'w')
        oo.write( '\t'.join(hh) + '\n' )
        for i in self.data.items:
            this = [str(i.__dict__[x]) for x in hh]
            this[3] = ', '.join(this[3].split('|'))
            print(this)
            oo.write( '\t'.join(this) + '\n' )
        oo.close()


class Cell_Methods(object):
    def __init__(self):
        self.data = dq.coll['cellMethods']

    def dump(self,fn):
        hh = ['label', 'title', 'uid', 'cell_methods']
        oo = open(fn,'w')
        oo.write( '\t'.join(hh) + '\n' )
        for i in self.data.items:
            this = [str(i.__dict__[x]) for x in hh]
            print(this)
            oo.write( '\t'.join(this) + '\n' )
        oo.close()



class Structure(object):
    def __init__(self):
        map1_ii = open( 'Copy of MIP Variable Prioritization_20240523_v1.2 - Structures.tsv', 'r' ).readlines()
        self.ee = dict()
        for l in map1_ii[1:]:
            bits = [x.strip() for x in l.strip().split('\t')]
            self.ee[bits[1]] = bits[2]
        self.data = dq.coll['structure']

    def dump(self,fn):
        oo = open(fn,'w')
        hh = ['label','title','description','spid','tmid','coords','dids','cell_methods','cell_measures','procNote','prov']
        oo.write( '\t'.join( hh + ['new_title',] ) + '\n' )
        for i in self.data.items:
            this = [i.__dict__.get(x,'') for x in hh]
            this[3] = dq.inx.uid[this[3]].label
            this[4] = dq.inx.uid[this[4]].label
            this[6] = ', '.join( [ dq.inx.uid[y].label for y in this[6] ] )
            this.append( self.ee.get(this[1],'') )
            this = [ str(x) for x in this]
            print(this)

            oo.write( '\t'.join(this) + '\n' )
        oo.close()




st = Structure()
ts = Temporal_Shape()
ss = Spatial_Shape()
va = MIP_Variable()
mt = MIP_Table()
cm = CMOR_Variable()


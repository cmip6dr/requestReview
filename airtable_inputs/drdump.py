import xlsxwriter, collections
 

from dreqPy import dreq
dq = dreq.loadDreq()

## coll keys: ['__core__', '__main__', '__sect__', 'requestVarGroup', 'requestItem', 'exptgroup', 'miptable', 'CMORvar', 'objective', 'spatialShape', 'requestLink', 'tableSection', 'modelConfig', 'varChoiceLinkC', 'objectiveLink', 'remarks', 'experiment', 'requestVar', 'standardname', 'varChoiceLinkR', 'var', 'mip', 'varChoice', 'temporalShape', 'structure', 'grids', 'timeSlice', 'cellMethods', 'tags', 'varRelations', 'varRelLnk', 'qcranges', 'places', 'transfers', 'units']

class MIP_Variable(object):
    def __init__(self):
        self.data = dq.coll['var']

    def dump(self,fn):
        hh = ['label', 'description','title', 'uid', 'sn','units','procnote','procComment','prov']
        oo = open(fn,'w')
        workbook = xlsxwriter.Workbook(fn)
        worksheet = workbook.add_worksheet()
 
# Start from the first cell.
# Rows and columns are zero indexed.
        row = 0
        column = 0
 
        for x in hh:
                  worksheet.write(row, column, x)
                  column += 1
        row += 1
 
# iterating through content list
        for i in self.data.items:
            column = 0
            this = [i.__dict__[x] for x in hh]
            this[6] = ', '.join(this[6])
            print(this)
            for x in this:
    # write operation perform
                  worksheet.write(row, column, x)
                  column += 1
 
    # incrementing the value of row by one
    # with each iterations.
            row += 1
     
        workbook.close()

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

class Time_Slice(object):
    def __init__(self):
        self.data = dq.coll['timeSlice']

    def dump(self,fn):
        hh = ['label', 'title', 'uid', 'description','child','end','nyears','sliceLen','sliceLenUnit','start','startList','step','type']
        oo = open(fn,'w')
        oo.write( '\t'.join(hh) + '\n' )
        for i in self.data.items:
            this = [str(i.__dict__.get(x,'')) for x in hh]
            oo.write( '\t'.join(this) + '\n' )
        oo.close()

class CMOR_Variable(object):
    def __init__(self):
        self.data = dq.coll['CMORvar']

    def dump(self,fn):
        hh = ['label', 'title', 'uid', 'processing','description','stid','vid','type','modeling_realm','positive','mipTableSection','mipTable','prov','provNote','frequency','p1','p2','p3']
        oo = open(fn,'w')
        workbook = xlsxwriter.Workbook(fn)
        worksheet = workbook.add_worksheet()
 
# Start from the first cell.
# Rows and columns are zero indexed.
        row = 0
        column = 0
 
        content = ['Name',] + hh
        for x in content:
                  worksheet.write(row, column, x)
                  column += 1
        row += 1
 
# iterating through content list
        for i in self.data.items:
            column = 0
            this = [str(i.__dict__.get(x,'')) for x in hh[:-3]]
            p = collections.defaultdict( set )
            for rv in dq.inx.iref_by_sect[i.uid].a['requestVar']:
                x = dq.inx.uid[rv]
                p[x.priority].add( x.mip )
            for x in [1,2,3]:
                if len(p[x]) == 0:
                    p[x] = ''
            thisp = [', '.join( sorted(list( p[x] ) ) ) for x in [1,2,3] ]
            dis = this[4]
            dis = dis.replace("'",'"')
            dis = dis.replace('\t',' ')
            dis = dis.replace('  ',' ')
            this[5] = dq.inx.uid[this[5]].label
            this[6] = dq.inx.uid[this[6]].label
            this = ['%s.%s' % (this[11],this[0]),] + this + thisp
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
        self.map = dict()
        ii = open( 'branding vocabularies vert.csv', 'r' ).readlines()
        for i in ii:
            bits = [x.strip() for x in i.strip().split('\t')]
            if bits[0][0:2] == 'no':
                self.map['x'] = bits[1:4]
            else:
               bb = [x.strip() for x in bits[0].split(',')]
               for b in bb:
                   self.map[b] = bits[1:4]

    def dump(self,fn):
        hh = ['label', 'title', 'uid', 'dimensions', 'levels', 'levelFlag','brand']
        oo = open(fn,'w')
        oo.write( '\t'.join(hh) + '\n' )
        for i in self.data.items:
            this = [str(i.__dict__[x]) for x in hh[:-1]]
            dd = [x.strip() for x in this[3].split('|')]
            this[3] = ', '.join(dd)
            b = self.map['x'][0]
            for k in self.map.keys():
                if k in dd:
                    b = self.map[k][0]
            this.append(b)
            print(this)
            oo.write( '\t'.join(this) + '\n' )
        oo.close()


class Cell_Methods(object):
    def __init__(self):
        self.data = dq.coll['cellMethods']
        self.map = dict()
        ii = open( 'branding vocabularies cm.csv', 'r' ).readlines()
        for i in ii:
            bits = i.strip().split('\t')
            if bits[0][0] != '"':
                self.map[bits[0].split()[0]] = bits[1]

    def dump(self,fn):
        hh = ['label', 'title', 'uid', 'cell_methods','brand_label']
        oo = open(fn,'w')
        oo.write( '\t'.join(hh) + '\n' )
        for i in self.data.items:
            this = [str(i.__dict__[x]) for x in hh[:-1]]
            print(this)
            cm = this[3]
            cmw = cm.split()
            try:
                k = cmw.index('where')
                this.append(self.map[cmw[k+1]])
            except:
                this.append('x')
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
ce = Cell_Methods()
tt = Time_Slice()


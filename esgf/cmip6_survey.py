import json, urllib
import collections
import urllib.request
import shelve
from dreqPy import dreq

dq = dreq.loadDreq()

ss = set()
for i in dq.coll['CMORvar'].items:
    if i.mipTable == 'Amon' and i.defaultPriority == 1:
        ss.add( (i.mipTable,i.label) )

temp = 'https://esgf-index1.ceda.ac.uk/esg-search/search/?offset=0&limit=500&type=Dataset&replica=false&latest=true&project%%21=input4mips&activity_id=CMIP&table_id=%(table_label)s&mip_era=CMIP6&variable_id=%(variable_label)s&facets=mip_era%%2Cactivity_id%%2Cmodel_cohort%%2Cproduct%%2Csource_id%%2Cinstitution_id%%2Csource_type%%2Cnominal_resolution%%2Cexperiment_id%%2Csub_experiment_id%%2Cvariant_label%%2Cgrid_label%%2Ctable_id%%2Cfrequency%%2Crealm%%2Cvariable_id%%2Ccf_standard_name%%2Cdata_node&format=application%%2Fsolr%%2Bjson'

selection = "activity_id=CMIP&table_id=%(table_label)s&mip_era=CMIP6&variable_id=%(variable_label)s"
sdict = dict( table_id='Amon', experiment_id='historical', variable_id='tas' )

temp2 = 'https://esgf-index1.ceda.ac.uk/esg-search/search/?offset=0&limit=500&type=Dataset&replica=false&latest=true&project%%21=input4mips&%(selection)s&facets=mip_era%%2Cactivity_id%%2Cmodel_cohort%%2Cproduct%%2Csource_id%%2Cinstitution_id%%2Csource_type%%2Cnominal_resolution%%2Cexperiment_id%%2Csub_experiment_id%%2Cvariant_label%%2Cgrid_label%%2Ctable_id%%2Cfrequency%%2Crealm%%2Cvariable_id%%2Ccf_standard_name%%2Cdata_node&format=application%%2Fsolr%%2Bjson'



tmp = dict( Amon='tas, pr, uas, vas, huss, rsut, rsdt, rlut, rsus, rsds, rlus, rlds, ps, ua, va, zg, cl, clt'.split(', '),
            AERmon='od550aer, abs550aer'.split(', '),
            Lmon='cVeg, cLitter, gpp, nbp, npp'.split(', '),
            Omon=['fgco2',],
            Emon=['cSoil',],
            day='tas, pr, tasmax, tasmin'.split(', ')
           )


def survey1():
  sh = shelve.open( 'esgf_cmip6_survey' )
  for table,ll in tmp.items():
    table_label = table
    for variable_label in ll:
        u = temp % locals()
        obj = urllib.request.urlopen( u )
        ee = json.load( obj )
        model_list = ee['facet_counts']['facet_fields']['source_id']
        sh['%s.%s' % (table_label,variable_label) ] = model_list
        print (table_label,variable_label,len(model_list) )
  sh.close()

def survey2():
  sh = shelve.open( 'esgf_cmip6_survey' )
  for table_label,variable_label in ss:
      if variable_label not in tmp[table_label]:
        u = temp % locals()
        obj = urllib.request.urlopen( u )
        ee = json.load( obj )
        model_list = ee['facet_counts']['facet_fields']['source_id']
        sh['%s.%s' % (table_label,variable_label) ] = model_list
        print (table_label,variable_label,len(model_list) )
  sh.close()

def survey3():
  sv = collections.defaultdict( set )
  sh = shelve.open( 'esgf_cmip6_survey', 'r' )
  dd = {}
  for table_label,variable_label in ss:
    ml = sh[ '%s.%s' % (table_label,variable_label) ][::2]
    dd[variable_label] = set(ml)
    sv[ len( ml ) ].add(variable_label)
  sh.close()
  ks = sv.keys()
  for k in sorted(list(ks)):
      print (k, sorted(list(sv[k])), len( sv[k] ) )
  return dd
    

dd = survey3()

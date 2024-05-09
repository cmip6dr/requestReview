import json, urllib, os, collections
import collections
import urllib.request
import shelve
from dreqPy import dreq

##
## fields returned by default (fields=*) (Oct 2023):
## ['_timestamp', '_version_', 'access', 'activity_drs', 'activity_id', 'cf_standard_name', 'citation_url', 'data_node', 'data_specs_version', 'dataset_id_template_', 'datetime_start', 'datetime_stop', 'directory_format_template_', 'experiment_id', 'experiment_title', 'frequency', 'further_info_url', 'grid', 'grid_label', 'id', 'index_node', 'instance_id', 'institution_id', 'latest', 'master_id', 'member_id', 'mip_era', 'model_cohort', 'nominal_resolution', 'number_of_aggregations', 'number_of_files', 'pid', 'product', 'project', 'realm', 'replica', 'retracted', 'score', 'size', 'source_id', 'source_type', 'sub_experiment_id', 'table_id', 'title', 'type', 'url', 'variable', 'variable_id', 'variable_long_name', 'variable_units', 'variant_label', 'version', 'xlink']

e_id = json.load(open("CMIP6_CVs/CMIP6_experiment_id.json", "r"))["experiment_id"]


class UrlOpenError(Exception):
    "Custom exception for errors in url library"
    pass


dq = dreq.loadDreq()

dr_map = dict()
dr_map0 = dict()

l1 = ["Lmon.mrro", "Lmon.evspsblveg", "Lmon.evspsblsoi", "Amon.pr"]
l2 = ["day.pr", "day.tasmax", "day.sfcWindmax", "day.tasmin", "day.clt"]
dr_map_ex1 = set([tuple(s.split(".")) for s in l1 + l2])

temp_ex1 = "https://%(esgf_node)s/esg-search/search/?offset=0&limit=500&type=Dataset&replica=false&latest=true&project%%21=input4mips&table_id=%(table_label)s&mip_era=CMIP6&variable_id=%(variable_label)s&experiment_id=%(experiment_label)s&facets=mip_era%%2Cactivity_id%%2Cmodel_cohort%%2Cproduct%%2Csource_id%%2Cinstitution_id%%2Csource_type%%2Cnominal_resolution%%2Cexperiment_id%%2Csub_experiment_id%%2Cvariant_label%%2Cgrid_label%%2Ctable_id%%2Cfrequency%%2Crealm%%2Cvariable_id%%2Ccf_standard_name%%2Cdata_node&format=application%%2Fsolr%%2Bjson"

for i in dq.coll["CMORvar"].items:
    dr_map[(i.mipTable, i.label)] = i
    if (i.mipTable not in ["day", "Amon"]) and i.defaultPriority == 1:
        dr_map0[(i.mipTable, i.label)] = i

temp = "https://%(esgf_node)s/esg-search/search/?offset=0&limit=500&type=Dataset&replica=false&latest=true&project%%21=input4mips&activity_id=CMIP&table_id=%(table_label)s&mip_era=CMIP6&variable_id=%(variable_label)s&facets=mip_era%%2Cactivity_id%%2Cmodel_cohort%%2Cproduct%%2Csource_id%%2Cinstitution_id%%2Csource_type%%2Cnominal_resolution%%2Cexperiment_id%%2Csub_experiment_id%%2Cvariant_label%%2Cgrid_label%%2Ctable_id%%2Cfrequency%%2Crealm%%2Cvariable_id%%2Ccf_standard_name%%2Cdata_node&format=application%%2Fsolr%%2Bjson"

tempb = "https://%(esgf_node)s/esg-search/search/?offset=%(offset)s&limit=10000&type=Dataset&replica=false&latest=true&activity_id=CMIP%(other_constraints)s&fields=%(return_fields)s&format=application%%2Fsolr%%2Bjson"
temp_base = "https://%(esgf_node)s/esg-search/search/?"
tempb = "offset=%(offset)s&limit=10000&type=Dataset&replica=false&latest=true%(other_constraints)s&fields=%(return_fields)s"
_format = "&format=" + urllib.parse.quote_plus("application/solr+json")

## http://esgf-data.dkrz.de/esg-search/search/?offset=0&limit=10&type=Dataset&replica=false&latest=true&experiment_id=historical&mip_era=CMIP6&activity_id%21=input4MIPs&facets=mip_era%2Cactivity_id%2Cmodel_cohort%2Cproduct%2Csource_id%2Cinstitution_id%2Csource_type%2Cnominal_resolution%2Cexperiment_id%2Csub_experiment_id%2Cvariant_label%2Cgrid_label%2Ctable_id%2Cfrequency%2Crealm%2Cvariable_id%2Ccf_standard_name%2Cdata_node

selection = "activity_id=CMIP&table_id=%(table_label)s&mip_era=CMIP6&variable_id=%(variable_label)s"
sdict = dict(table_id="Amon", experiment_id="historical", variable_id="tas")

temp2 = "https://esgf-index1.ceda.ac.uk/esg-search/search/?offset=0&limit=500&type=Dataset&replica=false&latest=true&project%%21=input4mips&%(selection)s&facets=mip_era%%2Cactivity_id%%2Cmodel_cohort%%2Cproduct%%2Csource_id%%2Cinstitution_id%%2Csource_type%%2Cnominal_resolution%%2Cexperiment_id%%2Csub_experiment_id%%2Cvariant_label%%2Cgrid_label%%2Ctable_id%%2Cfrequency%%2Crealm%%2Cvariable_id%%2Ccf_standard_name%%2Cdata_node&format=application%%2Fsolr%%2Bjson"

tmp = dict(
    Amon="tas, pr, uas, vas, huss, rsut, rsdt, rlut, rsus, rsds, rlus, rlds, ps, ua, va, zg, cl, clt".split(
        ", "
    ),
    AERmon="od550aer, abs550aer".split(", "),
    Lmon="cVeg, cLitter, gpp, nbp, npp".split(", "),
    Omon=[
        "fgco2",
    ],
    Emon=[
        "cSoil",
    ],
    day="tas, pr, tasmax, tasmin".split(", "),
)

report_year = 2024
ifile = "../esgf_dashboard/cmip6-variables_gb_20220331.csv"
ifile = "../esgf_dashboard/cmip6-variables_04_09_2023.csv"
ifile = "../esgf_dashboard/revised/cmip6-variables_31_03_2022.csv"
if report_year == 2024:
    ifile = "../esgf_dashboard/revised/cmip6-variables_20_03_2024.csv"

ee = {}


class Scanner(object):
    def __init__(self, ifile, silent=False):
        assert os.path.isfile(ifile), "%s not found" % ifile
        ii = [x.strip().split(",") for x in open(ifile).readlines()]
        for l in ii[1:]:
            if len(l) > 4:
                k = "%s.%s" % (l[-3], l[0])
                if k in ee:
                    ee[k] = (float(l[-2]) + ee[k][0], int(l[-1]) + ee[k][1])
                else:
                    ee[k] = (float(l[-2]), int(l[-1]))

        ## WARNING : possible case issues not fully dealt with`

        self.by_size = sorted(list(ee.keys()), key=lambda x: ee[x][0], reverse=True)
        self.by_count = sorted(list(ee.keys()), key=lambda x: ee[x][1], reverse=True)
        if not silent:
            for k in self.by_size[:50]:
                print("%s: %s" % (k, ee[k]))
            print("===============================\n\n")
            for k in self.by_count[:50]:
                print("%s: %s" % (k, ee[k]))
        self.ee = ee
        self.ranks = {}
        for k in self.ee.keys():
            self.ranks[k] = (self.by_size.index(k) + 1, self.by_count.index(k) + 1)

    def get_c3s(self, ifile="c3s34g_variables.json"):
        ee = json.load(open(ifile, "r"))
        self.c3svars = []
        for k, l in ee["requested"].items():
            self.c3svars += ["%s.%s" % (k, x) for x in l]


esgf_node = "esgf-index1.ceda.ac.uk"
esgf_node = "esgf-data.dkrz.de"


def survey1(idict=locals()):
    sh = shelve.open("esgf_cmip6_survey_02")
    for table, ll in tmp.items():
        table_label = table
        idict["table_label"] = table_label
        for variable_label in ll:
            idict["variable_label"] = variable_label
            u = temp % idict
            obj = urllib.request.urlopen(u)
            ee = json.load(obj)
            model_list = ee["facet_counts"]["facet_fields"]["source_id"]
            sh["%s.%s" % (table_label, variable_label)] = model_list
            print(table_label, variable_label, len(model_list))
    sh.close()
    return ee


def survey2b(
    shname, redo=False, template=tempb, experiment_labels=["piControl"], return_after=-1
):
    esgf_node = "esgf-index1.ceda.ac.uk"
    esgf_node = "esgf-data.dkrz.de"
    sh = shelve.open(shname)
    _sh = shelve.open("_" + shname)
    experiment_label = "historical"
    return_fields = "*"
    return_fields = "size%2Cid%2Cnumber_of_files"
    _return_fields = "size,id,number_of_files"
    return_fields = urllib.parse.quote_plus(_return_fields)
    kk = 0
    for experiment_label in experiment_labels:
        this_key = experiment_label
        other_constraints = (
            "&experiment_id=%(experiment_label)s&mip_era=CMIP6&variant_label=r1i1p1f1"
            % locals()
        )
        other_constraints = (
            "&experiment_id=%(experiment_label)s&mip_era=CMIP6" % locals()
        )

        if redo or (this_key not in _sh.keys()):
            ##if variable_label in tmp[table_label]:
            try:
                complete = False
                offset = 0
                ntot = 0
                sz = 0
                nf = 0
                kk = 0
                while not complete:
                    kk += 1
                    x = temp_base % locals()
                    y = template % locals()
                    u = x + y + _format
                    obj = urllib.request.urlopen(u)
                    ee = json.load(obj)
                    for i in ee["response"]["docs"]:
                        sz += i["size"]
                        nf += i["number_of_files"]
                        sh[i["id"]] = (i["size"], i["number_of_files"])
                    ntot += len(ee["response"]["docs"])
                    if ntot >= ee["response"]["numFound"]:
                        complete = True
                    else:
                        offset += len(ee["response"]["docs"])
                print(experiment_label, int(sz * 1.0e-9), nf, kk, ntot)
                _sh[this_key] = (experiment_label, int(sz * 1.0e-9), nf, kk, ntot)

            except KeyboardInterrupt as e:
                ## if interrupted while reading, abondon this loop
                print("Caught keyboard interrupt [1]. Canceling tasks...")
                break
            ##except:
            ## if interrupted while reading, abondon this loop
            ####print("Exception raised while opening URL")
            ##raise UrlOpenError

        kk += 1
        if return_after > 0 and kk >= return_after:
            break

    sh.close()
    _sh.close()
    return ee


def survey2(
    shname,
    redo=False,
    this_map=dr_map,
    template=temp,
    experiment_label="",
    return_after=-1,
):
    esgf_node = "esgf-index1.ceda.ac.uk"
    esgf_node = "esgf-data.dkrz.de"
    sh = shelve.open(shname)
    kk = 0
    for table_label, variable_label in this_map:
        if experiment_label != "":
            this_key = "%s.%s.%s" % (table_label, variable_label, experiment_label)
        else:
            this_key = "%s.%s" % (table_label, variable_label)

        if redo or (this_key not in sh.keys()):
            ##if variable_label in tmp[table_label]:
            try:
                u = template % locals()
                obj = urllib.request.urlopen(u)
                ee = json.load(obj)
                print(ee)
            except KeyboardInterrupt as e:
                ## if interrupted while reading, abondon this loop
                print("Caught keyboard interrupt [1]. Canceling tasks...")
                break
            except:
                ## if interrupted while reading, abondon this loop
                print("Exception raised while opening URL")
                raise UrlOpenError

            try:
                model_list = ee["facet_counts"]["facet_fields"]["source_id"]
                sh[this_key] = model_list
                print(table_label, variable_label, len(model_list))
            except KeyboardInterrupt as e:
                ## if interrupted after reading, complete this loop.
                print("Caught keyboard interrupt [2]. Canceling tasks...")
                model_list = ee["facet_counts"]["facet_fields"]["source_id"]
                sh[this_key] = model_list
                print(table_label, variable_label, experiment_label, len(model_list))
                break
        kk += 1
        if return_after > 0 and kk >= return_after:
            break

    sh.close()
    return ee


def survey2c(insh, outsh):
    ## to take per-dataset results and aggregate to per variable
    sh = shelve.open(insh, "r")
    cc = collections.defaultdict(lambda: collections.defaultdict(int))

    for k in sh.keys():
        model, expt, variant, tab, var = tuple(k.split("."))[3:8]
        cc[(tab, var)][model] += 1
    sh.close()
    sho = shelve.open(outsh)
    for k, d in cc.items():
        ll = []
        for m in sorted(list(d.keys())):
            ll.append(m)
            ll.append(d[m])
        sho["%s.%s" % k] = ll
    sho.close()


def survey3():
    sv = collections.defaultdict(set)
    sh = shelve.open("esgf_cmip6_survey_dkrz", "r")
    dd = {}
    for table_label, variable_label in dr_map:
        ml = sh["%s.%s" % (table_label, variable_label)][::2]
        dd[variable_label] = set(ml)
        sv[len(ml)].add(variable_label)
    sh.close()
    ks = sv.keys()
    for k in sorted(list(ks)):
        print(k, sorted(list(sv[k])), len(sv[k]))
    return dd


def frank(l):
    r = l[-1]
    if l[-2] != None:
        r += 1.0e-6 * l[-2]
    else:
        r += 0.999
    return r


collector_ll = []
collector_cc = collections.defaultdict(set)
pdict = {}


def survey4(shfile="esgf_cmip6_survey_dkrz_20230822", tag="20230822"):
    sv = collections.defaultdict(set)
    sh = shelve.open(shfile, "r")
    for k in sh.keys():
        try:
            pdict[k] = dr_map[k].defaultPriority
        except:
            print("DEFAULT PRIORITY NOT FOUND FOR %s" % k)
            pdict[k] = 3
        for m in sh[k][::2]:
            collector_cc[m].add(k)

    # oo = open('cmip6_survey.ris','w' )
    # for k,l in collector_cc.items():
    #     oo.write( 'TY  - %s\n' % k )
    #     oo.write( 'AU  - %s\n' % k )
    #     oo.write( 'TI  - %s\n' % k )
    #     oo.write( 'AB  - %s\n' % ' '.join([x.replace('.','_') for x in list(l)]) )
    #     oo.write( 'ER  -\n\n' )
    # oo.close()
    dd = {}
    for k, l in sh.items():
        ml = l[::2]
        dd[k] = set(ml)
        sv[len(ml)].add(k)
    sh.close()
    ks = sv.keys()
    ltot = 0
    ##i = None
    ##print( 'No month found: %s' % this)
    ee = {}
    rank = 1
    sc = Scanner(ifile, silent=True)
    sc.get_c3s()
    lsc = len(sc.ee)
    #
    # k is the count of models
    #

    for k in sorted(list(ks), reverse=True):
        pk = k / 66.0
        prank = rank / 1206.0
        collector_ll.append((prank, pk))
        ltot += len(sv[k])
        print(ltot, k, sorted(list(sv[k])), len(sv[k]))
        for v in sv[k]:
            if v in sc.ee:
                ee[v] = (
                    k,
                    rank,
                    sc.ee[v][0],
                    sc.ranks[v][0],
                    sc.ee[v][1],
                    sc.ranks[v][1],
                    min([rank, sc.ranks[v][0], sc.ranks[v][1]]),
                )
            else:
                vbits = v.split(".")
                vl = "%s.%s" % (vbits[0], vbits[1].lower())
                if vl in sc.ee:
                    ee[v] = (
                        k,
                        rank,
                        sc.ee[vl][0],
                        sc.ranks[vl][0],
                        sc.ee[vl][1],
                        sc.ranks[vl][1],
                        min([rank, sc.ranks[vl][0], sc.ranks[vl][1]]),
                    )
                else:
                    ee[v] = (k, rank, None, lsc + 1, None, lsc + 1, rank)
        rank += len(sv[k])

    oo = open("RankedCmipVariables_%s.csv" % tag, "w")
    oo.write(
        "ID,Model Count,Rank,Download Volume,Rank,Download Count,Rank,Min Rank,In C3S,\n"
    )
    struc_rank = collections.defaultdict(lambda: 9999)
    var_rank = collections.defaultdict(lambda: 9999)
    krank = 0
    for k in sorted(list(ee.keys()), key=lambda x: frank(ee[x])):
        krank += 1
        tab, var = k.split(".")
        fr = frank(ee[k])
        if (tab, var) not in dr_map:
            oo.write(
                ",".join(
                    [
                        k,
                    ]
                    + [str(x) for x in ee[k]]
                )
                + (",%s," % (k in sc.c3svars))
                + (",".join(["na", "na"]))
                + "\n"
            )
        else:
            cid = dr_map[tuple(k.split("."))]
            struc = cid.stid
            struc_rank[cid.stid] = min([krank, struc_rank[cid.stid]])
            var_rank[cid.vid] = min([krank, var_rank[cid.vid]])
            v = dq.inx.uid[cid.vid]
            struc = dq.inx.uid[cid.stid]

            oo.write(
                ",".join(
                    [
                        k,
                    ]
                    + [str(x) for x in ee[k]]
                )
                + (",%s," % (k in sc.c3svars))
                + (",".join([v.sn, '"%s"' % v.title, '"%s"' % struc.title]))
                + "\n"
            )
    oo.close()
    oo = open("RankedCmipStructures_%s.csv" % tag, "w")
    for k in sorted(list(struc_rank.keys()), key=lambda x: struc_rank[x]):
        st = dq.inx.uid[k]
        oo.write(
            ",".join(
                [
                    '"%s"' % x
                    for x in [str(struc_rank[k]), st.label, st.title, st.cell_methods]
                ]
            )
            + "\n"
        )
    oo.close()

    oo = open("RankedCmipModels.csv", "w")
    for k in sorted(list(collector_cc.keys()), key=lambda x: len(collector_cc[x])):
        oo.write(
            ",".join(
                [
                    '"%s"' % x
                    for x in [
                        k,
                        len(collector_cc[k]),
                        len([x for x in collector_cc[k] if pdict[x] == 1]),
                    ]
                ]
            )
            + "\n"
        )
    oo.close()

    return dd, sv, collector_cc


if __name__ == "__main__":
    ##dd = survey3()
    op = 3
    op = 2.3
    if op == 2:
        for x in range(5):
            try:
                dd = survey2("esgf_cmip6_survey_dkrz_tmp")
                #
            except UrlOpenError:
                print("AT ATTEMPT %s" % x)

    elif op == 2.1:
        dd = survey2(
            "esgf_cmip6_survey_dkrz_20230822_ex1",
            this_map=dr_map_ex1,
            template=temp_ex1,
            experiment_label="historical",
        )
        dd = survey2(
            "esgf_cmip6_survey_dkrz_20230822_ex1",
            this_map=dr_map_ex1,
            template=temp_ex1,
            experiment_label="ssp245",
        )
        dd = survey2(
            "esgf_cmip6_survey_dkrz_20230822_ex1",
            this_map=dr_map_ex1,
            template=temp_ex1,
            experiment_label="ssp126",
        )
    elif op == 2.2:
        dd = survey2b(
            "esgf_cmip6_survey2b_dkrz_20240422",
            experiment_labels=sorted(list(e_id.keys())),
        )
    elif op == 2.3:
        dd = survey2c(
            "esgf_cmip6_survey2b_dkrz_20240422", "esgf_cmip6_survey_dkrz_20240422"
        )
    elif op == 3:
        # dd, sv, sm = survey4(shfile='esgf_cmip6_survey_dkrz_20240525')

        shf, t = "esgf_cmip6_survey_dkrz", "202203_v2"
        if report_year == 2024:
            shf, t = "esgf_cmip6_survey_dkrz_20240325", "202403"

        dd, sv, sm = survey4(shfile=shf, tag=t)

        this = sorted(collector_ll, key=lambda x: x[0])
        for x in this:
            print(x)

        cc = collector_cc
        ll = sorted(list(cc.keys()), key=lambda x: len(cc[x]))
        ll.reverse()
        print(ll[0], len(cc[ll[0]]))
        print(ll[1], len(cc[ll[1]]))
        print(len(cc[ll[0]].intersection(cc[ll[1]])))

        A, B, C = [cc[ll[x]] for x in [0, 1, 2]]

        print([len(x) for x in [A, B, C]])
        print([len(x.intersection(y)) for x, y in [(A, B), (B, C), (C, A)]])
        print(len(A.intersection(B.intersection(C))))

        print(ll[0:3])
        C = cc[ll[0]]
        print("1,", len(C), ",", len(C), ",")
        oo = open("intersections.csv", "w")
        for k in range(len(ll)):
            x = ll[k]
            C = C.intersection(cc[x])
            oo.write(
                ",".join([str(xx) for xx in [x, k + 1, len(cc[x]), len(C)]]) + ",\n"
            )
        oo.close()

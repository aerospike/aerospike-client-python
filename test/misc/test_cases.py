# import the module
from __future__ import print_function
import aerospike
from aerospike import exception as ex
import time
import psutil
import os
import gc
from concurrent.futures import ThreadPoolExecutor
from aerospike_helpers import cdt_ctx
import ctypes
import datetime
from aerospike_helpers.operations import hll_operations

list_index = "list_index"
list_rank = "list_rank"
list_value = "list_value"
map_index = "map_index"
map_key = "map_key"
map_rank = "map_rank"
map_value = "map_value"

ctx_ops = {
    list_index: cdt_ctx.cdt_ctx_list_index,
    list_rank: cdt_ctx.cdt_ctx_list_rank,
    list_value: cdt_ctx.cdt_ctx_list_value,
    map_index: cdt_ctx.cdt_ctx_map_index,
    map_key: cdt_ctx.cdt_ctx_map_key,
    map_rank: cdt_ctx.cdt_ctx_map_rank,
    map_value: cdt_ctx.cdt_ctx_map_value,
}


def malloc_trim():
    ctypes.CDLL("libc.so.6").malloc_trim(0)


def create_idx(client_a, namespace, setname, binname, indexname, ctx):
    assert client_a.index_cdt_create(namespace, setname, binname, 0, 1, indexname, {"ctx": ctx}) == 0


def drop_idx(client_a, ns, setname, indexname, policy=None):
    if policy is None:
        policy = {"timeout": 3000}
    r = []
    r.append("sindex-delete:")
    if ns:
        r.append("ns=%s" % (ns))
    if setname:
        r.append(";set=%s" % (setname))
    if indexname or indexname == "":
        r.append(";indexname=%s" % (indexname))
    req = "".join(r)
    client_a.info_all(req, policy=policy)


def test_sindex(aeros, namespace, setname):
    indexname = "idx1"
    binname = "bin1"
    ctx_ops = {
        "BY_LIST_INDEX": cdt_ctx.cdt_ctx_list_index,
        "BY_MAP_KEY": cdt_ctx.cdt_ctx_map_key,
    }

    context = [("BY_LIST_INDEX", 3), ("BY_MAP_KEY", 29931), ("BY_MAP_KEY", 28880), ("BY_MAP_KEY", 11346)]
    ctx = []
    for api, idx_or_key in context:
        ctx.append(ctx_ops[api](idx_or_key))
    threads_status = {}
    with ThreadPoolExecutor(max_workers=20) as executor:
        for i in range(10):
            threads_status["create" + str(i)] = executor.submit(
                create_idx, aeros, namespace, setname, binname=binname, indexname=indexname, ctx=ctx
            )
        for i in range(10):
            threads_status["drop" + str(i)] = executor.submit(
                drop_idx, aeros, ns=namespace, setname=setname, indexname=indexname
            )
    for thread in threads_status.values():
        thread.result()


first_ref_count = 0
last_ref_count = 0

process = psutil.Process(os.getpid())
initial_rss_usage = process.memory_info().rss
initial_vms_usage = process.memory_info().vms


def add_ctx_op(ctx_type, value):
    ctx_func = ctx_ops[ctx_type]
    return ctx_func(value)


ctx_list_index = []
ctx_list_index.append(add_ctx_op(list_index, 0))


def gccallback(phase, info):
    if phase == "start":
        print("starting garbage collection....")
    else:
        print("Finished garbage collection.... \n{}".format("".join(["{}: {}\n".format(*tup) for tup in info.items()])))

        print("Unreachable objects: \n{}".format("\n".join([str(garb) for garb in gc.garbage])))
        print()
        print(
            f"gc DONE current rss = {process.memory_info().rss}, \
                memory increase bytes = {process.memory_info().rss - initial_rss_usage}"
        )
        print(
            f"gc DONE current vms = {process.memory_info().vms}, \
                memory increase bytes = {process.memory_info().vms - initial_vms_usage}"
        )
        print()


def connect_to_cluster(aeros):
    client = aeros.connect("admin", "admin")
    client.close()
    # gc.collect()


def test_memleak(aeros, namespace, setname):
    # first_ref_count = sys.gettotalrefcount()
    # last_ref_count = first_ref_count

    # print(f'first_ref_count = {first_ref_count}')
    count = 1
    i = 0
    print()
    print(f"init count = {count}, rss:{initial_rss_usage} vms:{initial_vms_usage}")
    print()
    # client = aeros.connect('generic_client', 'generic_client')
    ns = "test"
    set_name = "demo"
    aeros.truncate(ns, set_name, 0)
    while i < count:
        i = i + 1
        connect_to_cluster(aeros)
        time.sleep(1)
        # malloc_trim()
        # k = (namespace, setname, 0)
        # val = [1,2,3,3,3,3,3,3,3,3,3,3,3]
        # assert client.put(k, {'bin1': val}) == 0
        # assert client.remove(k) == 0
        print(
            f"run:{i} rss:{process.memory_info().rss} rss_change:{process.memory_info().rss - initial_rss_usage} \
                vms:{process.memory_info().vms} vms_change: {process.memory_info().vms - initial_vms_usage}"
        )
        # n = gc.collect()
        # print("Number of unreachable objects collected by GC:", n)
        # print("Uncollectable garbage:", gc.garbage)
        # last_ref_count = sys.gettotalrefcount()
        # print(f'outstandingref = {last_ref_count-first_ref_count}')

    # client.close()
    print(
        f"test DONE rss:{process.memory_info().rss} rss_change:{process.memory_info().rss - initial_rss_usage} \
            vms:{process.memory_info().vms} vms_change: {process.memory_info().vms - initial_vms_usage}"
    )


def test_hllop(aeros, namespace, setname):
    client = aeros.connect("generic_client", "generic_client")
    # assert client.put(('test', 'demo', 0), {'': 3}) == 0
    # assert client.remove(('test', 'demo', 0), {'': 3}) == 0
    # bs_b4_cdt = client.get_cdtctx_base64(ctx_list_index)
    # print(bs_b4_cdt)
    list_items = list("item%s" % str(i) for i in range(4))
    ops = [
        hll_operations.hll_add("bin1", list_items, 4, 4),
    ]
    key = (namespace, setname, "send-key-test")
    client.operate(key, ops)
    t1, t2, res = client.get(key)
    print("res is ==========={}", t1, t2, res)
    # > {'bin1': b'\x00\x04\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00
    # \x00%\x00\x00\x00\x00:\x00\x00\x00\x00\x00'}
    client.close()


def test_cdtctx_info(aeros, namespace, setname):
    client_a = aeros.connect("generic_client", "generic_client")
    indexname = "test_index"
    indextype = cdt_ctx.index_type_string(aerospike.INDEX_TYPE_LIST)
    binname = "bin1"
    bintype = cdt_ctx.index_datatype_string(aerospike.INDEX_STRING)
    numbins = 1
    priority = 1
    # ctx = [cdt_ctx.cdt_ctx_list_index(1), cdt_ctx.cdt_ctx_list_index(2)]
    bs_b4_cdt = client_a.get_cdtctx_base64(ctx_list_index)
    r = []
    r.append("sindex-create:")
    r.append("ns=%s" % (namespace))
    r.append(";set=%s" % (setname))
    r.append(";indexname=%s" % (indexname))
    r.append(";indextype=%s" % (indextype))
    r.append(";numbins=%d" % (numbins))
    r.append(";indexdata=%s,%s" % (binname, bintype))
    r.append(";priority=%d" % (priority))
    r.append(";context=%s" % (bs_b4_cdt))
    req = "".join(r)
    print("req is ==========={}", req)
    res = client_a.info_all(req, policy=None)
    print("res is ==========={}", res)
    client_a.close()


def test_50levelcdt(aeros, namespace, setname):
    val = {
        2816: 2816,
        671425: {
            3237: 3237,
            9519: 9519,
            346: 346,
            4692233: {
                4596203: {
                    4942: 4942,
                    2814: 2814,
                    2719613: [
                        "acdae",
                        [
                            {
                                4057: [590493566920838883, 2889561087904236858, 5572498139349771130],
                                601: 3642,
                                3417: {
                                    "1": 3286882184514064098,
                                    "'+nlC?c';RTy": 6888747680410831808,
                                    "FJ=XK86SmY": 8287492002251011878,
                                },
                                "&<KLw": 4003,
                                "OubZL": [13868506621886207, 1811070964111467906, 2635136454044971015],
                                1087: 1814,
                                3673: [6422767578136824690, 2312135633032219741, 6860485504107103533],
                                "Z_Y-a": "o}}9[",
                                866: "NXw_f",
                                "r2i[<": [6595822541341408389, 8017978258977891515, 2067418307568156408],
                                "g7WzP": 2938,
                                973: [7542366039781928526, 6955590977909145841, 1439108476557539035],
                            },
                            [
                                1950,
                                {
                                    "g%pkrB02W": 2407660725694543420,
                                    "AXh": 7826952493485362139,
                                    "*DX]@;q": 8039642851365424526,
                                    "o@5": 888273446792979787,
                                    "1rlY4.uvHH]M&": 8924428933460853695,
                                },
                                "edebc",
                            ],
                            1333,
                            {
                                "Ah']6": 1597,
                                ")xajt": "VFV9n",
                                "-+SZX": {
                                    "?w_&(LD'`m": 3516550445970078097,
                                    "Rak8_G|": 5112801998488992581,
                                    "9)I h": 4679343890043674099,
                                },
                                3312: [6344799939414826069, 1052983503997987056, 5427291048021309954],
                                2430: [5356481335881007783, 6734682758742175692, 3753444826878850294],
                                "(1qLV": 1412,
                                3630: 583,
                                324: [8338119424922781244, 2306681387819472262, 2813005015956471911],
                                "NvCq.": {
                                    "gv2=o3FeO{": 811511699181190845,
                                    "|": 7875930961809289980,
                                    "D,bg": 3691444306930117305,
                                },
                                3744: [1045701296117046449, 1266822545165597865, 6340943785083360883],
                                2903: {
                                    "ZM#3S": 4733494279710667800,
                                    "2#%!si": 1384950902148382553,
                                    "l7oP ;?": 8302014523550172530,
                                },
                                "R!qAs": 3099,
                            },
                            2445,
                            [
                                [3163, 3831, 661],
                                [
                                    ["cceda", "abbea", "dacee"],
                                    ["badea", "beeae", "bcbae"],
                                    ["eecdd", "bdaed", "decad"],
                                    {
                                        "aL#]j": [2085613002616207403, 879063663570614885, 3385668391729842253],
                                        "'{Ws&": 2374,
                                        "[X*w!": "\\y$|4",
                                        162: "?B[?j",
                                        "D-`PN": {
                                            "u]:": 8203002090577247733,
                                            '>\\?EVG""-X&-<': 2903803540527296018,
                                            "ryW;@": 1062030783036823163,
                                        },
                                        3738: [4604688926343214390, 2178415870303591447, 7547473572638601623],
                                        2622: [2295997213922831216, 5083963970483388652, 3970734129593498841],
                                        "&f wj": {
                                            "zax@": 1192725755944006730,
                                            "o-z]/p": 1376274633851378839,
                                            ")>U>+f>GQ": 5818074212413897431,
                                        },
                                        702: "ye%ti",
                                        1110: "%F!uf",
                                        'KDP"?': [1332236913595279846, 8208722476076636614, 4249792637844900212],
                                        3790: 3945,
                                    },
                                    {
                                        1125: [2258620693460166182, 4818633126498422514, 197673709328729376],
                                        "S.Ga_": {
                                            "yh_k.Co": 3888826914302974858,
                                            "wSYc\\{w{D6Pz": 2176082544985955056,
                                            "m": 1999381806960498151,
                                        },
                                        "K)[lG": 202,
                                        "=ac(C": [1666105201749454109, 6297243955437477278, 6617242070644334060],
                                        18: "p$)<$",
                                        "R<G|#": 2956,
                                        1756: 3111,
                                        '"VR$X': {
                                            ")+|wGa9H": 3473386646937173861,
                                            "q-hMHk,;&&": 3129033289950556894,
                                            "RErQSi*": 8620281386393307257,
                                        },
                                        "E(ED-": "^&Xy+",
                                        "sW$I/": 1551,
                                        ",8phF": [7309015312428161247, 5116273424931759533, 4140772272394814367],
                                        1867: {
                                            "Oc%z#\\D": 1423601833017109385,
                                            ")pN/*!": 5469237635489154514,
                                            "SEU&=w7gY]": 8099576400738943625,
                                        },
                                    },
                                    "bdcda",
                                    2193,
                                    {
                                        3768: 1671,
                                        "%aB!m": "[B]H9",
                                        "(#).=": "gG4D&",
                                        "z:x.v": [6184558226581815411, 6934781531566496155, 3640877490823354141],
                                        "U6Grq": [3832847009912486745, 912959491684441184, 133513932873335039],
                                        "QN,KK": "8WMbT",
                                        1317: "YkLl,",
                                        "^LK7Q": [3979356224329979203, 283187564561724848, 245875322764163051],
                                        "gur==": [3124825022476825105, 3088390696690034651, 3597014862597900123],
                                        "a%auB": 1784,
                                        "-j)9V": [5765106871511876223, 778215160303866058, 1723298462969836340],
                                        "U[/5>": [2890023042171654308, 8123686514671922679, 8520495847488602417],
                                    },
                                    {
                                        5752083: {
                                            2770: 2770,
                                            3059: 3059,
                                            7777: 7777,
                                            3570: 3570,
                                            7376477: [
                                                {
                                                    3989: 3989,
                                                    9448: 9448,
                                                    1131: 1131,
                                                    7860: 7860,
                                                    2316: 2316,
                                                    6099: 6099,
                                                    6621: 6621,
                                                    3887: 3887,
                                                    964361: {
                                                        1440: 1440,
                                                        9996: 9996,
                                                        5541: 5541,
                                                        2810: 2810,
                                                        747293: [
                                                            {
                                                                2027: 394,
                                                                3596: [
                                                                    2463010183639166700,
                                                                    4827808215612039182,
                                                                    4043864144366475339,
                                                                ],
                                                                2985: "uZ^>^",
                                                                "PK*xN": [
                                                                    848717836773981534,
                                                                    6239274084777124153,
                                                                    4981642700193221809,
                                                                ],
                                                                "i )s`": "RG=fy",
                                                                ".Bo[S": {
                                                                    "=!Mp%j4": 2564610091407173771,
                                                                    "Bqgs": 3111426243815068299,
                                                                    "-/|&Ki.&#63Ck": 8142168873523112206,
                                                                },
                                                                "?-9C8": 1194,
                                                                279: [
                                                                    1326631294173250612,
                                                                    849260668155614101,
                                                                    3520325726550959148,
                                                                ],
                                                                "1Hz7,": "fZwV#",
                                                                "m|N!c": 2470,
                                                                "j;m8`": {
                                                                    "Y4nw4": 8886265102849866553,
                                                                    "{CN|vtZu@UxU": 7955960771880304639,
                                                                    "j?h ,N:( $0>n": 6977399364680029501,
                                                                },
                                                                "fWIDp": "uF&MV",
                                                            },
                                                            2982,
                                                            {
                                                                "AF|*A": 2005,
                                                                758: {
                                                                    "hIKd}Yhd7": 1551281315901946460,
                                                                    "&IK+-Zf": 9176813277065999591,
                                                                    "mW": 8651028812980915798,
                                                                },
                                                                "b-<o`": [
                                                                    1724020611841458403,
                                                                    3053515631056495339,
                                                                    9143523156739557342,
                                                                ],
                                                                1264: "CuhdS",
                                                                ",IMe ": [
                                                                    3762255609680949465,
                                                                    7265786085946512892,
                                                                    3072110543628209222,
                                                                ],
                                                                "e4Au,": 607,
                                                                1801: {
                                                                    "MqYqnk}Wh": 3067809804619220754,
                                                                    "=qXPv3(<(": 3448661332367999319,
                                                                    "^cRl": 4013406947957305192,
                                                                },
                                                                30: [
                                                                    7466911968193955303,
                                                                    7948450597700797993,
                                                                    5444886302028496328,
                                                                ],
                                                                2137: [
                                                                    5554165874553971666,
                                                                    1344653534269255827,
                                                                    5538226087460848205,
                                                                ],
                                                                "U`M*w": [
                                                                    5838865217692676759,
                                                                    5863966837687017001,
                                                                    6159602170729732223,
                                                                ],
                                                                "uI}y?": {
                                                                    "U?7pSP>1XD": 743356241923447617,
                                                                    "nS!=)4\\B;)`": 4284240182559573579,
                                                                    "W": 1323296074644479192,
                                                                },
                                                                "@nkYD": {
                                                                    "W5ElJ": 424266987327178816,
                                                                    'mE!`>bn&)"Q': 3559973129035862605,
                                                                    "P": 3395454499869956156,
                                                                },
                                                            },
                                                            {
                                                                "jJylA": 2253,
                                                                2560: {
                                                                    "0<f%Xc@_?A:!4": 1449027845592591043,
                                                                    "#391%wnrWZ%": 2468654852582399446,
                                                                    "_|9y": 8743903038928174043,
                                                                },
                                                                1211: "E{5#Z",
                                                                2018: 3203,
                                                                "SJ_rN": "!s0tB",
                                                                1578: 3487,
                                                                "zl^`d": ".b)G]",
                                                                "^!r'6": {
                                                                    "{2Zh!m5nM%`-": 5159985773958993944,
                                                                    "1r5w^G6v!": 7741318237873869227,
                                                                    "6?nR&e=p": 1617427126784111747,
                                                                },
                                                                "mXwsg": {
                                                                    "bXkT7(4RvMj": 8882327807572626757,
                                                                    ")um/e": 3819319662129572073,
                                                                    "QGbiOP/0JX=ym": 6524023637815889240,
                                                                },
                                                                2823: {
                                                                    "l(49C": 4193004424873230286,
                                                                    "$56(ti7B8G^": 5339665307853260565,
                                                                    '>#J7v@"4N}wC=': 6052840610072839241,
                                                                },
                                                                "[B2$,": 851,
                                                                ";6vRI": [
                                                                    6764882414318602794,
                                                                    1628520468065504284,
                                                                    6125452125849181162,
                                                                ],
                                                            },
                                                            {
                                                                "OJaH&": "'\\Wn=",
                                                                "EcAn$": "8ee<|",
                                                                1548: [
                                                                    4070476138575018672,
                                                                    3343948461392337735,
                                                                    8825229423718797861,
                                                                ],
                                                                668: "x=V![",
                                                                40: [
                                                                    2818842976772160830,
                                                                    1485666503349229314,
                                                                    4747651886874025316,
                                                                ],
                                                                "G&vb`": "47{1w",
                                                                417: 3518,
                                                                ">VU.^": 3408,
                                                                3249: {
                                                                    "cyrmli^8b=": 7519821360035600110,
                                                                    "pW6 :UGD\\": 4885547559104044913,
                                                                    "2*J/vE": 8873540696851386837,
                                                                },
                                                                "rL}l1": 1746,
                                                                "nS@IP": 848,
                                                                3865: {
                                                                    "1eR37Ad}": 6776552411059158395,
                                                                    "*8@K9O7": 1478459735808846945,
                                                                    ".|B@Ie?e O6q": 2110582812466738389,
                                                                },
                                                            },
                                                            317,
                                                            {
                                                                2630215: [369, 1503, 1475],
                                                                4243: 4243,
                                                                7794: 7794,
                                                                9679: 9679,
                                                                8973: 8973,
                                                                8109: 8109,
                                                                1548: 1548,
                                                                1260: 1260,
                                                                9870: 9870,
                                                                3241: 3241,
                                                            },
                                                            4050,
                                                            "aaecd",
                                                            [
                                                                [
                                                                    853442487881750152,
                                                                    126171576947105448,
                                                                    7450059654182306366,
                                                                ],
                                                                {
                                                                    "Y[f]": 7336114561704774509,
                                                                    "&$CdV',|cpr&[": 7692817014171639556,
                                                                    "AB1F;sO2v<(4D": 5433629429453764571,
                                                                    ")w(gaFw^*ebMA": 5642312740207363870,
                                                                    "v": 4298439949089730627,
                                                                },
                                                                {
                                                                    "SK|8e": 7136335999959122517,
                                                                    "lI57].B?": 8164838381546159623,
                                                                    "R]^+#t|RL%Q": 5912821041872549830,
                                                                    "5#4BS`3caH": 7602070183758184113,
                                                                    "zAO8": 3032935071670655002,
                                                                },
                                                            ],
                                                        ],
                                                        2127: 2127,
                                                        2827: 2827,
                                                        9134: 9134,
                                                        937: 937,
                                                        5366: 5366,
                                                    },
                                                    7292: 7292,
                                                },
                                                {
                                                    1393: 109,
                                                    3112: {
                                                        "LdZ\\/6f^A": 5577716653152903163,
                                                        ",`iSdNp/W1:": 253795283617170175,
                                                        "t3'<-": 2167476403216322540,
                                                    },
                                                    ";\\SOx": {
                                                        " =s_": 186590748812957088,
                                                        "[Y$/8j": 5233125218607880821,
                                                        "t7& HLA-7": 3264910993059369936,
                                                    },
                                                    "t Snc": {
                                                        "J`V{E#": 9020946831205671666,
                                                        "ilJEz": 5481729984669397119,
                                                        "+v!P^9-zBi(": 5226652903605259318,
                                                    },
                                                    "$:vGf": [
                                                        8009554248919103838,
                                                        2355591054648389318,
                                                        6805002444398522550,
                                                    ],
                                                    3952: 187,
                                                    884: [
                                                        2176183113985878018,
                                                        5284263743058300892,
                                                        1487331613872679427,
                                                    ],
                                                    1288: [8783897222559681420, 63718272962736148, 2749435958775532386],
                                                    "x;:O9": [
                                                        6015276470118693451,
                                                        4896906552918297103,
                                                        2114003376691583175,
                                                    ],
                                                    "bJ0*Q": [
                                                        851345212218174821,
                                                        6425566914823636120,
                                                        4798240692366202837,
                                                    ],
                                                    2585: {
                                                        ",1ma.<": 3966571508219656568,
                                                        "}\\": 8556156910631593352,
                                                        "+`[,T n": 1091015094557000987,
                                                    },
                                                    "VZ&`P": ":B oX",
                                                },
                                                [
                                                    {
                                                        "QSKx-+": 3236916673757490314,
                                                        'NG"dU': 7522621400616520498,
                                                        "`2o'{Hr=;2+H`": 5955395711367752028,
                                                        "UVq;-}1": 4715765558826656130,
                                                        "=t[V!n": 988869454548330631,
                                                    },
                                                    "bccce",
                                                    {
                                                        "Ne?": 6011304735515408111,
                                                        "V]5": 1400904644520331651,
                                                        "NSgR": 5701118380696214300,
                                                        "DM`E(!*?t)": 392540412880230131,
                                                        "f": 7162138198433621048,
                                                    },
                                                ],
                                                ["abbac", "aeddb", "bbace"],
                                                {
                                                    ":Zr,)": {
                                                        "vLL": 5595039227619193009,
                                                        "lH}AUB": 6419153205348736620,
                                                        "d>(Dm_": 1929975271257110366,
                                                    },
                                                    "6f@&i": {
                                                        "# 2/o-xB(&&": 7331628997618803729,
                                                        ")": 2971404189184101499,
                                                        '" 2|1WMJcgp': 1478418811300837871,
                                                    },
                                                    524: 2254,
                                                    2565: {
                                                        "7MS P6#{DdSvQ": 4841441705447230717,
                                                        "30l": 5725450007076816073,
                                                        ".fMeFFuP:": 7179493933275497374,
                                                    },
                                                    "UlIMs": 1030,
                                                    2449: {
                                                        "6:'Zs \\z`4'": 293490135622983900,
                                                        "Z5:v": 6141135214591181449,
                                                        ">": 4880372173283240632,
                                                    },
                                                    "tk@zB": 3136,
                                                    'P"p%I': [
                                                        4582081444604219274,
                                                        586260088412629021,
                                                        1676243346444597439,
                                                    ],
                                                    ",_CL\\": {
                                                        "CJl": 484975339481239030,
                                                        "UJ$Qu9BBr": 2838277582618526991,
                                                        "kj(6NoFG<cZd)": 4224231604232546221,
                                                    },
                                                    "5J;E)": [
                                                        6492937526926164700,
                                                        8326942268264913375,
                                                        9081709862124306228,
                                                    ],
                                                    "}AL)o": 1596,
                                                    659: 793,
                                                },
                                                {
                                                    3054: "M27=i",
                                                    "]#:=[": "WhPde",
                                                    480: {
                                                        "]Y0gTm{J": 9008730467373588282,
                                                        "|?e-B8!,H": 7344985153594091090,
                                                        "CDIp.!,": 2869711357023551566,
                                                    },
                                                    ";A1D?": "+Gurs",
                                                    2069: {
                                                        "pi8&q&": 996183412951382552,
                                                        "''": 5453710601911841434,
                                                        "*wtAC52BOz": 6281138719839032588,
                                                    },
                                                    1023: "Ej3+c",
                                                    2965: " n't9",
                                                    1869: "Hl.uV",
                                                    994: {
                                                        "1YH&(Jvw": 4136426127742316820,
                                                        "ns'Zl": 7385853162364454595,
                                                        "D.=%t4": 5174573752102651699,
                                                    },
                                                    2942: [
                                                        7330508256615544217,
                                                        7997673218639489919,
                                                        1643304528856637502,
                                                    ],
                                                    340: {
                                                        "M4": 4380434322844674338,
                                                        "v=_": 1699193794382495275,
                                                        "Zf#l$2C#K": 8949910293318952844,
                                                    },
                                                    ',=A"q': "[bv/o",
                                                },
                                                {
                                                    3620: 637,
                                                    "U|z(0": "/2Y^g",
                                                    "v@N0w": {
                                                        ">'Q\\!x_(P_;zo": 6928940127706333863,
                                                        "$l4dfImsc{/": 8277869772306091784,
                                                        "yKDNj\\ hs\\7": 2710354223418346327,
                                                    },
                                                    171: {
                                                        "!3I\\<MdjO,Y'": 6882715928893922599,
                                                        "/": 4915416033223895550,
                                                        ";L,OQMisd|c": 4252887791381074817,
                                                    },
                                                    "t:$N<": [
                                                        8206838907755229469,
                                                        277940573941647944,
                                                        4398572320328845486,
                                                    ],
                                                    "%c<fO": 4071,
                                                    "m;ev-": 3587,
                                                    415: [198720664400913147, 3710381027789790560, 2582747289397733741],
                                                    4091: {
                                                        "BaVv,8q": 6615154009434816388,
                                                        ":$xD4VXQoy-": 6976403941840839493,
                                                        "^Ou.bs[yO": 2579063218446292516,
                                                    },
                                                    "OP9,r": [
                                                        1795921550935700458,
                                                        7193761707223019475,
                                                        4881286042775226465,
                                                    ],
                                                    ")08?N": [
                                                        4533823493193789274,
                                                        788555898962233507,
                                                        1225030485168913702,
                                                    ],
                                                    "J8wEN": [
                                                        11654038397849624,
                                                        5310427225686210275,
                                                        2880300065622492686,
                                                    ],
                                                },
                                                {
                                                    "}(NA!": [
                                                        1369486789251020772,
                                                        6198398293933211824,
                                                        4644127257921163986,
                                                    ],
                                                    "a>u[p": {
                                                        "3V": 6940356534833746900,
                                                        '"jsqvSO(Vd': 3226190586979807270,
                                                        "#}3Zk h": 4501943060626910480,
                                                    },
                                                    "c3%Za": 3285,
                                                    4037: 3880,
                                                    "1\\1.L": [
                                                        7830193743088826958,
                                                        2077896270110090810,
                                                        6952357052514428515,
                                                    ],
                                                    3253: {
                                                        "+<|'li": 4746633954658959312,
                                                        "Ohm\\B1#": 6675076437738110455,
                                                        "I:OIQe": 5424354111699135965,
                                                    },
                                                    359: "_73@e",
                                                    "s+js_": [
                                                        4935783077476286835,
                                                        3718872378182874233,
                                                        7345618427961070042,
                                                    ],
                                                    "fD{.)": "\\pBo)",
                                                    3542: [171585675843715, 8924034453706826905, 4477370601942615952],
                                                    2682: [
                                                        4812942837520499192,
                                                        2984172991742510973,
                                                        5484992455790464180,
                                                    ],
                                                    746: {
                                                        "I5$zj;p": 5793815295421900678,
                                                        "3MoSB^jBxH8a": 5975474916082669762,
                                                        "r''cHV'al@": 4768717089363506137,
                                                    },
                                                },
                                                [
                                                    1292,
                                                    {
                                                        "b#u0oX": 5794436652271677790,
                                                        "p 4M*pyk": 1726916543793342640,
                                                        "hsW0|[?L{&X0": 1189265131072066635,
                                                        "PQTHJJiO==[P": 1234556814580221589,
                                                        "sBfP{_haJ5": 580274964330547897,
                                                    },
                                                    1385,
                                                ],
                                                874,
                                            ],
                                            5473: 5473,
                                            9731: 9731,
                                            60: 60,
                                            5969: 5969,
                                            6008: 6008,
                                        },
                                        2035: 2035,
                                        170: 170,
                                        4140: 4140,
                                        2265: 2265,
                                        9329: 9329,
                                        8411: 8411,
                                        2320: 2320,
                                        5315: 5315,
                                        1527: 1527,
                                    },
                                    [
                                        "bccab",
                                        {
                                            "Rc(/cNP@}+54@": 8867211792506789334,
                                            "u12t_Fh67UV": 9089032353315645081,
                                            "bg$h)^": 8059751342671581867,
                                            "0c|k": 1512922316533870031,
                                            "lHQ": 8103945943278369774,
                                        },
                                        {
                                            "Q@ic@F": 6777432795076022694,
                                            "37}-!}y|<a&": 1474897271406368964,
                                            "A*G+:": 4764648748061839627,
                                            "Kzc": 296338348230449552,
                                            '2:!e%8g"iq2': 5537746975974794093,
                                        },
                                    ],
                                ],
                                [3699, 363, 1056],
                                {
                                    2689: {
                                        "el4i}=": 7898063814061605708,
                                        "YHQ": 7632885987837498628,
                                        "b/$Sxg=>-u^L": 3942042652524562264,
                                    },
                                    "g,6bT": {
                                        'Z?Ck&Z|Q4\'H""': 8238489863228508105,
                                        "@e9Y?7": 7092050975134655828,
                                        "yCGd5[": 4839880660274753620,
                                    },
                                    1891: 2830,
                                    131: "g6UCt",
                                    "dLAnX": [820035869346323201, 6939170416599955582, 7639418322001234019],
                                    "mrM2]": [5709017635494087037, 7580234105968907209, 2112660284700003281],
                                    "_c1;c": [2730559641754431480, 705852861800377935, 4133459124448770172],
                                    1699: {
                                        "r7K)": 8830865434963042603,
                                        "n.eBB*c?": 4856895942102738082,
                                        "6TqK'=Sr:s": 1356653983369852547,
                                    },
                                    "T=\\8t": {
                                        "RYV": 2538223414488006587,
                                        "0<R6}[llp": 256833017697384542,
                                        "<.L=HGf>": 8769880023818298751,
                                    },
                                    "_+(B0": "8tt3B",
                                    "[mT#H": {
                                        "`ol": 305341142819160421,
                                        "QL": 3790456137066296625,
                                        "}\\T4Hhu4-F'lm": 5923138333664602400,
                                    },
                                    746: [98370100846008647, 2943421159043572304, 6347605179300798607],
                                },
                                [
                                    {
                                        ",xS-9": 3699388621298159754,
                                        "w#0|v": 9194617906518410490,
                                        "SM$!YA+^m]@S": 883163160478344249,
                                        '-DN".E[BZZ7*': 4138351368928621436,
                                        "V+&A": 4865899177221311630,
                                    },
                                    [7186595294859557019, 4020162902642804405, 2911214146193266134],
                                    {
                                        '"&Eb-bO]I{': 1870833637063689953,
                                        "PN": 6304495870709899086,
                                        "\\U%\"v('": 2518461447259817325,
                                        "->": 5977697440784052771,
                                        "-PP-GBm": 4301179066347453857,
                                    },
                                ],
                                {
                                    2398: {
                                        ")Lz1<YxY; gm": 8784801304969310903,
                                        "=w": 5398409997084602542,
                                        "f.JJVzC8EI`": 3781817411756781446,
                                    },
                                    2401: "WH7*C",
                                    "<ow#l": "F:$S/",
                                    2716: {
                                        "kvG+xYy&": 834384450486382502,
                                        "9M": 8123030546428511072,
                                        "tI[/U@!hpal": 6818866686793164244,
                                    },
                                    450: [7529783080525961355, 4812656955093450457, 9067225399099680601],
                                    3210: {
                                        "z}<!d)+Y-9&": 5928029917317993411,
                                        "b/)L": 2685871273121645714,
                                        '_+;Z"ew}x': 4528826367519680248,
                                    },
                                    "My2C8": [2629903304982347581, 7085328020545580558, 5962049687263773196],
                                    2930: 558,
                                    2589: 320,
                                    3873: 2145,
                                    2427: {
                                        "e]U'0AI1": 5124223921881703880,
                                        ")Q8cs6Y-q": 2306193454976240708,
                                        "RLj ": 5532562044417375269,
                                    },
                                    "3+@\\H": "Ha.fa",
                                },
                                ["eeddc", "acdde", "dbabe"],
                                {
                                    "ir|cv": "%rVaP",
                                    "lQ.wx": 2007,
                                    "}|DOv": [23833155448905026, 1941338589292228423, 8229736983837949389],
                                    114: {
                                        "=q={!\\!;": 7489328726231041066,
                                        "h^Rx": 882512292988500749,
                                        "kKf(%F_I*:1Z": 4775366996318847054,
                                    },
                                    "}l.:s": "fdl7{",
                                    1379: "b2#y9",
                                    "m4`c+": [7137055101704362220, 3305581963482709514, 7336893505026136955],
                                    2137: "&]mMV",
                                    81: [5052253576572489867, 6042858838242851669, 4350333925553214268],
                                    2635: 3055,
                                    ":LZ'h": [595987539435464299, 1296195135511462962, 1170816308378411730],
                                    "qzF=$": ";}U[n",
                                },
                                3848,
                                [
                                    [5929264317667981504, 3230715833475089562, 1984762642145356200],
                                    "ecaae",
                                    {
                                        "%h$*1k*3j_Z": 7409681051042462381,
                                        "VoLmHXm>!Gs*P": 2596725860844678428,
                                        "O]lYw55": 2420290214915017907,
                                        "Z9W<+\\": 2091984955680402369,
                                        "0(": 4076740638127535921,
                                    },
                                ],
                            ],
                            {
                                "6DIDW": {
                                    "x-Mh.0mi?At": 7929986287159167186,
                                    "^SP(dzBK": 5585321310954707374,
                                    "/": 4846641775267719851,
                                },
                                2867: [5289023964426153550, 3897067676998841688, 7624497976067715466],
                                3619: 936,
                                2163: "<]aoU",
                                "kHt-N": {
                                    "a5-#DIV|.p ": 3704294061292277684,
                                    "!": 7616906959287278134,
                                    "8PK]c@!(L": 3556100206623030885,
                                },
                                ":C=io": [2884679329244455093, 3472314146465850443, 113311003849287548],
                                "O!|,U": 3710,
                                4054: 1645,
                                3996: 3296,
                                3434: [3740454812684224153, 1772524556638378465, 4979208099297491635],
                                2137: 3314,
                                285: 370,
                            },
                            {
                                858: 2099,
                                3213: "pnlU8",
                                101: 2822,
                                3102: "F'wQG",
                                3797: {
                                    "x)M(+-Al1C": 6285530819096356695,
                                    ')zfMF!Pib>34"': 7511346800043960200,
                                    "02S%\\,]y:I=!6": 1940323471712546811,
                                },
                                "QI;F6": [1539186530100543866, 5163976612472968774, 4280406586081031613],
                                "]s'kc": 4037,
                                "|cWYM": "IlW;B",
                                ".)27&": [9131495783512433728, 2389274184832376213, 3908833132702372428],
                                "m@uK,": {
                                    '%":': 7796770948431171679,
                                    "AaDH`7On|": 1291169901387844561,
                                    "Y`m;": 8340472187938635051,
                                },
                                784: {
                                    "U;/V;LogpAsx)": 8360076081615269680,
                                    "^T]PJ>`?,": 4805782719361081147,
                                    "dVwApq:xF|": 4964790101583609536,
                                },
                                1262: {
                                    "^gG|F/)u": 7885131421306397050,
                                    "!]wFHx": 2240780882873109160,
                                    "$CvGrt>a-Xt(": 7276244971445321701,
                                },
                            },
                            [
                                {
                                    "j\\Y": 2355604870937000073,
                                    "t<'0e7V?xH": 8510351704859051707,
                                    ":ot_QIe?}YC\\": 8749487052052962058,
                                    "YdwZ`DjQsbY": 8901686992783530293,
                                    "zp+5S8,l": 4942166851032597905,
                                },
                                [7933560436373562602, 1755776693911435531, 4134698244546362306],
                                "edbee",
                            ],
                            3835,
                        ],
                        {
                            "a7!+D": 1476,
                            652: 1300,
                            "o2n[`": {
                                "^@kmTHLk rY": 5373612247593519891,
                                "R:Y$p": 979146317806373008,
                                "dGx.l8)kp+If": 4624203697439771384,
                            },
                            "0pkbb": [8319679702072731314, 3925460750788406677, 1185953130603455516],
                            1884: 1069,
                            1093: 3429,
                            "--&TT": 985,
                            858: {
                                "ui.c%s2v8v": 8177582129866345300,
                                "IWWokP>aIM": 729667612529206287,
                                "Y 7": 8140907436373383623,
                            },
                            917: [6408162673894661691, 6883032844087786887, 5732343454361937337],
                            2054: [6258093984718556193, 5229680872016365769, 6653742241093949321],
                            "P_1N\\": "v+>x[",
                            437: "+)LG6",
                        },
                        1767,
                        "acaeb",
                        ["badcc", "edbba", "bddbd"],
                        ["bbade", "cdced", "beedb"],
                        {
                            104: " Z%9e",
                            "_Eh%9": {
                                "M&zPE^F": 8331297352739858833,
                                "|N2022-08-10T04:42:28.309831405Z OLd": 1726458278774597495,
                                "DM8@QV8b}SL(": 4506479360942954155,
                            },
                            2295: {
                                "gIL-hA": 2030165687758471106,
                                "%,K": 1341311707233903130,
                                "Q>tiGY%$D%\\": 5701944301346970973,
                            },
                            2639: {
                                "PM>@": 5340547330742563316,
                                "4fr1VqE&": 7097415008185343415,
                                "0*by1": 9049821280850369704,
                            },
                            2627: 2444,
                            1091: [2522870108927085530, 4539899532612328635, 2519238489946561895],
                            4037: 687,
                            176: [748863759689891716, 4523036602654400853, 373418779686850345],
                            " #25|": [9215079402980587578, 1967695157491523129, 1725875725339314635],
                            'd"!\\O': [1444698429073639132, 5357352183950232293, 5685982586196833444],
                            4095: 480,
                            2242: "i-hX)",
                        },
                        [1167, 1526, 786],
                        "cabbc",
                    ],
                    4572: 4572,
                    5582: 5582,
                    2992: 2992,
                    852: 852,
                    3662: 3662,
                    4071: 4071,
                    9979: 9979,
                },
                9534: 9534,
                5053: 5053,
                7480: 7480,
                1555: 1555,
                9639: 9639,
                4728: 4728,
                7040: 7040,
                6485: 6485,
                4033: 4033,
            },
            3754: 3754,
            4162: 4162,
            7524: 7524,
            3313: 3313,
            4081: 4081,
            80: 80,
        },
        6788: 6788,
        7709: 7709,
        7314: 7314,
        7782: 7782,
        8858: 8858,
        8041: 8041,
        1326: 1326,
        7590: 7590,
    }
    k = (namespace, setname, 0)
    client = aeros.connect("generic_client", "generic_client")
    assert client.put(k, {"bin1": val}) == 0
    client.close()
    print("test_50levelcdt passed \n")


def test_send_key(aeros, namespace, setname):

    client = aeros.connect("generic_client", "generic_client")

    # Records are addressable via a tuple of (namespace, set, primary key)
    key = (namespace, setname, "send-key-test")

    try:
        # Write a record
        client.put(key, {"name": "John Doe", "age": 32})
    except ex.RecordError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))

    # Read a record
    (k, meta, record) = client.get(key)

    print(k)

    (k, meta, record) = client.get(key, policy={"key": aerospike.POLICY_KEY_SEND})

    print(k)

    client.remove(key)

    client.close()


def test_create_user(aeros, namespace, setname):
    policy = {"timeout": 1000}
    user = "generic_client"
    password = "generic_client"
    # roles = ["data-admin", "user-admin", "sys-admin",
    # "read-write", "read-write-udf", "quota-4k-2k-role",
    # "sindex-admin", "truncate"]
    roles = ["data-admin", "user-admin", "sys-admin", "read-write", "read-write-udf", "sindex-admin", "truncate"]

    aeros.connect("admin", "admin")

    # usr_sys_admin_privs = [{"code": aerospike.PRIV_USER_ADMIN}, {"code": aerospike.PRIV_SYS_ADMIN}]
    try:
        aeros.admin_drop_role("quota-4k-2k-role")
        time.sleep(2)
    except Exception:
        pass
    # aeros.admin_create_role(
    #        "quota-4k-2k-role", usr_sys_admin_privs, read_quota=40000, write_quota=20000)
    # time.sleep(1)

    try:
        aeros.admin_drop_user(user, policy)
        time.sleep(2)
    except Exception:
        pass

    aeros.admin_create_user(user, password, roles, policy)


# def test_large_put(aeros, namespace, setname):

#     client = aeros.connect("generic_client", "generic_client")

#     # Records are addressable via a tuple of (namespace, set, primary key)
#     key = (namespace, setname, "send-key-test")

#     f = open("/home/randersen/NYTZ131.json")
#     data = json.load(f)
#     f.close()

#     # Test to make sure the doc was properly loaded
#     print(data["TimeZoneName"])

#     key = (namespace, setname, "America/New_York")

#     try:
#         client.put(key, {"tzzone": data})
#     except Exception as e:
#         print(e)


def get_aerospike():
    # tls_name = 'bob-cluster-a'
    tls_name = "172.31.1.163"

    endpoints = [("172.28.0.1", 3000)]

    hosts = [(address[0], address[1], tls_name) for address in endpoints]

    #    print(f'Connecting to {endpoints}')

    config = {
        "hosts": hosts,
        "policies": {"auth_mode": aerospike.AUTH_INTERNAL},
        # 'tls': {
        #     'enable': True,
        #     'cafile': "/Users/ramarajpandian/code/src/aerospike/enterprise/as-dev-infra/certs/Platinum/cacert.pem",
        #     'certfile': "/Users/ramarajpandian/code/src/aerospike/enterprise/as-dev-infra/certs/Client-Chainless/cert.pem",  # noqa: E501
        #     'keyfile': "/Users/ramarajpandian/code/src/aerospike/enterprise/as-dev-infra/certs/Client-Chainless/key.pem",  # noqa: E501
        #     'for_login_only': True,
        # }
        "user": "admin",
        "password": "admin",
    }
    # Optionally set policies for various method types
    write_policies = {"total_timeout": 2000, "max_retries": 0, "key": aerospike.POLICY_KEY_SEND}
    read_policies = {"total_timeout": 1500, "max_retries": 1, "key": aerospike.POLICY_KEY_SEND}
    policies = {"write": write_policies, "read": read_policies}

    config["policies"] = policies

    return aerospike.client(config)


def run():

    # aerospike.set_log_level(aerospike.LOG_LEVEL_INFO)

    aeros = get_aerospike()

    # config = {"hosts": [("bad_addr", 3000)]}
    # bad_client = aerospike.client(config)

    # Connect once to establish a memory usage baseline.
    connect_to_cluster(aeros)
    test_create_user(aeros, "test", "demo")
    # test_memleak(aeros, "test", "demo")
    # test_50levelcdt(aeros, "test", "demo")
    # test_sindex(aeros, "test", "demo")
    # test_send_key(aeros, "test", "demo")
    # test_cdtctx_info(aeros, "test", "demo")
    # test_hllop(aeros, "test", "demo")


if __name__ == "__main__":
    print("Current date and time: ", datetime.datetime.now())
    # gc.set_debug(gc.DEBUG_UNCOLLECTABLE | gc.DEBUG_STATS)
    # gc.callbacks.append(gccallback)
    run()
    print()
    # time.sleep(5)
    print(
        f"main DONE rss:{process.memory_info().rss} rss_change:{process.memory_info().rss - initial_rss_usage} \
            vms:{process.memory_info().vms} vms_change: {process.memory_info().vms - initial_vms_usage}"
    )
    print("Current date and time: ", datetime.datetime.now())

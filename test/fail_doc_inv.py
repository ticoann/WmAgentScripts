import json
from pprint import pprint
from json_encode_fail import fail_doc

en = json.JSONEncoder()

for i in fail_doc['docs']:
    try:
        en.encode(i['fwjr'])
    except:
        step = i['fwjr']['steps']
        for key in step:
            try:
                en.encode(step[key]["errors"])
            except:
                for i in step[key]["errors"]:
                    try:
                        en.encode(i["details"])
                    except:
                        a = str(unicode(i["details"], errors='ignore'))
                        pprint(i["details"])
                        pprint(a)
                        en.encode(a)

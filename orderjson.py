import json


def read(filename: str) -> dict[str, dict]:
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)


FILE = "fix/event.json"
fix_event = read(FILE)

obj1: dict[str, dict] = {}
obj2: dict[str, dict] = {}
for k, v in fix_event.items():
    ks = set(v.keys())
    for if_in, del_k in {
        "filmaffinity": ("imdb", "category", ),
        "imdb": ("category", ),
    }.items():
        if if_in in ks:
            for x in del_k:
                if x in v:
                    del v[x]
    if len(v.keys()) == 1:
        obj1[k] = v
    else:
        obj2[k] = v

obj1 = dict(sorted(obj1.items(), key=lambda x: x[0]))
obj2 = dict(sorted(obj2.items(), key=lambda x: x[0]))

with open(FILE+".json", "w", encoding="utf-8") as f:
    f.write("{\n")
    for k, v in obj1.items():
        f.write(f'  "{k}": {json.dumps(v, ensure_ascii=False)},\n')
    f.write(json.dumps(obj2, indent=2, ensure_ascii=False)[2:])

read(FILE+".json")  # check that it is valid JSON
print(FILE+".json")

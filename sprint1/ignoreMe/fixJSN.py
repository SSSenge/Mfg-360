import json


old = json.load(open('C:/Users/samso/Downloads/1_device_master.json'))
new = []
for k,v in old.items():
    new.append(v)

open('temp.json', 'w').write(str(new).replace("'", '"'))

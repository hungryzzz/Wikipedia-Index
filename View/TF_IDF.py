import json
import math

allList = []

def custom_key(val):
    v = val.split("#")
    return float(v[1])

def get_tfidf(MyList):
    sum = len(MyList) * 1.0

    for item in MyList:
        tfidfDict = {}
        for key, value in item.items():
            tfidfList = []
            for val in value:
                resorce = val.split("#")
                df = int(resorce[0]) * 1.0
                tf = float(resorce[1])
                tf_idf = tf * math.log(sum / df)
                tfidfList.append(resorce[2] + "#" + str(tf_idf))
            tfidfList.sort(key = custom_key, reverse = True)
            tfidfDict.update({key: tfidfList})
        allList.append(tfidfDict)        

if __name__ == "__main__":
    with open("df.json", 'r') as f:
        loadList = json.load(f)
    get_tfidf(loadList)
    with open("tf_idf.json", 'w') as f:
        json.dump(allList, f)


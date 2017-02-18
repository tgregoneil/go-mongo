### go-mongo 

Simple mongoDb operations:

    P.init = (pIn)
    P.createCollection = (collectionName)
    P.deleteOne = (ob, cb)
    P.deleteMany = (ob, cb)
    P.doReportErr = (reportErr)
    P.find = (queryOb, projectionOb, cb)
    P.findOne = (queryOb, projectionOb, cb)
    P.getPrimaryKeys = (cb)
    P.getSecondaryKeys = (keypath, cb)
    P.insert = (ob, cb)
    P.listCollections = (cb)
    P.listDatabases = (cb)
    P.listUsers = (cb)
    P.listUriCollection = ()
    P.remove = (ob, cb)
    P.setCbs = (cb, errCb)
    P.setCollection = (collectionName)
    P.setDatabase = (dbName)
    P.setHost = (host)
    P.setPort = (port)
    P.setUserPwd = (user, pwd)
    P.updateOne = (queryOb, updateOb, cb)
    P.updateMany = (queryOb, updateOb, cb)
    P.upsertOne = (queryOb, upsertOb, cb)
    P.upsertMany = (queryOb, upsertOb, cb)
    P.updateUri = (doCallInfoCb)

### Installation
```shell
$ npm install go-mongo
```

### Example (test.js)

```js

var mo = new require ('go-mongo') ();
var pretty = require ('js-beautify').js_beautify;

function pr (jstr) {
    return pretty (JSON.stringify (jstr));
}

mo.init ({dbName: 'test', collectionName: 'col'});

var doc = {key0: 'val1'};
console.log ('doc to be inserted: ' + pr (doc) + '\n');

mo.insert ({key0: 'val1'}, function (res) {
    console.log ('insert.res: ' + pr (res) + '\n');

    mo.find ({}, {}, function (res) {
        console.log ('find.res: ' + pr (res) + '\n');
        mo.remove ({key0:'val1'}, function (res) {
            console.log ('remove.res: ' + pr (res) + '\n');
            
            mo.find ({}, {}, function (res) {
                console.log ('Verifying empty collection after removing document (empty array)-- find.res\n' + pr (res) + '\n');
            });
        });
        
    });
});

```

### Result
```js
doc to be inserted: {
    "key0": "val1"
}

insert.res: {
    "result": {
        "ok": 1,
        "n": 1
    },
    "ops": [{
        "key0": "val1",
        "_id": "58a8bc2d21ba96a745ff7748"
    }],
    "insertedCount": 1,
    "insertedIds": ["58a8bc2d21ba96a745ff7748"]
}

find.res: [{
    "_id": "58a8bc2d21ba96a745ff7748",
    "key0": "val1"
}]

remove.res: {
    "n": 1,
    "ok": 1
}

Verifying empty collection after removing document (empty array)-- find.res
[]
```

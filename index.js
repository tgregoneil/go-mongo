// go-mongo/index.js


module.exports = function (p) {

// PRIVATE Properties/Methods
var v = {

    mongoClient: require ('mongodb').MongoClient,
    objectId: require ('mongodb').ObjectID,
    ut: require ('go-util'),

    uri: null,
    collectionName: "",

    cb: console.log,

    errCb: console.error,
    infoCb: console.log,
    reportErr: true,

    lastInited: null,
    savedP: null,
    keypath: null,

    initDefaults: {
        host: 'localhost',
        port: 27017,
        user: "",
        pwd: "",
        dbName: 'test',
        collectionName: 'test',
        cb: console.log,
        errCb: console.error,
        infoCb: console.log,
    },

    dbName: "",

}; // end PRIVATE properties
var f={};

//---------------------
f.init = () => {
    
    v.lastInited = v.initDefaults;

    var pIn = v.ut.isOb (p) ? p : v.lastInited;

    P.init (pIn);

}; // end f.init

//---------------------
f.connect = (cb) => {
    

    //v.mongoClient.connect (v.uri, function (err, dbOb) {
    v.mongoClient.connect (v.uri, function (err, client) {

        if (err) {

            if (v.reportErr) {

                v.errCb (err);

            } else {

                cb (null);

            } // end if (v.reportErr)
            

        } else {

            //cb (dbOb);
            const db = client.db (v.dbName);
            cb (db);

        } // end if (err)
        
    });

}; // end f.connect 


//---------------------
f.connectCollection = (cb) => {
    
    f.connect (function (dbOb) {
        
        if (dbOb === null) {

            cb (null);

        } else {
            
            if (v.collectionName === "") {

                v.errCb ('No collection name specified');

            } else {
                
                var collectionOb = dbOb.collection (v.collectionName);
                cb (collectionOb);

            } // end if (v.collectionName === "")
            

        } // end if (dbOb === null)
        
    });
        
}; // end f.connectCollection 


//---------------------
f.doCallback = (err, res, cb) => {
    
    cb = !cb ? v.cb : cb;
        // providing cb is optional: if not passed, use v.cb as defined in P.init

    if (err) {

        v.errCb (err);
        cb (err);

    } else {

        if (cb === console.log && typeof res !== 'string') {

            res = JSON.stringify (res);

        } // end if (cb === console.log && typeof res !== 'string')
        
        cb (res);

    } // end if (err)
    

}; // end f.doCallback 

//---------------------
f.restoreP = () => {
    
    v.uri = v.savedP.uri;
    v.collectionName = v.savedP.collectionName;
    v.cb = v.savedP.cb;
    v.errCb = v.savedP.errCb;
    v.lastInited = v.savedP.lastInited;

    return;

}; // end f.restoreP



//---------------------
f.saveP = () => {

    v.savedP = {
        uri: v.uri,
        collectionName: v.collectionName,
        cb: v.cb,
        errCb: v.errCb,
        lastInited: v.ut.cloneOb (v.lastInited),
    };
        
    return;

}; // end f.saveP


// PUBLIC Properties/Methods
var P = {};

//---------------------
P.init = (pIn) => {
    
    var p = v.ut.pCheck (pIn, v.lastInited);
    v.lastInited = p;

    v.collectionName = p.collectionName;

    P.updateUri (false);

    v.cb = p.cb;
    v.errCb = p.errCb;
    v.infoCb = p.infoCb;

}; // end P.init 


//---------------------
P.createCollection = (collectionName) => {
    
    f.connect (function (dbOb) {
        
        dbOb.createCollection (collectionName, function (err, collection) {
            
            if (err) {

                v.errCb ("mongoOps.createCollection: Couldn't create collection " + collectionName);

            } else {

                v.collectionName = collectionName;

                P.listUriCollection ();

            } // end if (err)
            
        });
    });

}; // end P.createCollection 


//---------------------
P.deleteOne = (ob, cb) => {
    
    v.reportErr = true;

    f.connectCollection (function (collectionOb) {
     
        collectionOb.deleteOne (ob, function (err, res) {

            err = err ? 'P.insert: err = ' + err : null ;
            f.doCallback (err, res, cb);

        });
        
    });

}; // end P.Insert 


//---------------------
P.deleteMany = (ob, cb) => {
    
    v.reportErr = true;

    f.connectCollection (function (collectionOb) {
     
        collectionOb.deleteMany (ob, function (err, res) {

            err = err ? 'P.insert: err = ' + err : null ;
            f.doCallback (err, res, cb);

        });
        
    });

}; // end P.Insert 


//---------------------
P.doReportErr = (reportErr) => {
    
    v.reportErr = reportErr;

}; // end P.doReportErr 


//---------------------
P.explain = (queryOb, projectionOb, cb) => {
    
    v.reportErr = true;

    f.connectCollection (function (collectionOb) {
        
        collectionOb.find (queryOb, projectionOb)
        .explain (function (err, explanation) {

            err = err ?  'P.explain failed:  query ' + JSON.stringify (queryOb) + '\n' + 
                'projection ' + JSON.stringify (projectionOb) : null;

            f.doCallback (err, explanation, cb);
            
        });
        
    });


}; // end P.explain 


//---------------------
P.aggregate = (pipeline, options, cb) => {
    
    v.reportErr = true;

    f.connectCollection (function (collectionOb) {
        
        //.toArray (function (err, items) {
        collectionOb.aggregate (pipeline, options, function (err, itemsCursor) {

            if (err) {

                err = err ?  'P.aggregate failed:  pipeline ' + JSON.stringify (pipeline) + '\n' + 
                    'options ' + JSON.stringify (options) : null;

                f.doCallback (err, null, cb);
            
            } else {

                itemsCursor.toArray (function (err, items) {

                    if (items !== null) {
        
                        v.ut.dollarDotSubUnicodeRestore (items);
        
                    } // end if (items != null)

                    f.doCallback (err, items, cb);

                });
    

            } // end if (err)
            

        })
            
        
    });


}; // end P.aggregate 


//---------------------
P.find = (queryOb, projectionOb, cb) => {
    
    v.reportErr = true;

    f.connectCollection (function (collectionOb) {
        
        collectionOb.find (queryOb, projectionOb)
        .toArray (function (err, items) {

            err = err ?  'P.find failed:  query ' + JSON.stringify (queryOb) + '\n' + 
                'projection ' + JSON.stringify (projectionOb) : null;

            if (items !== null) {

                v.ut.dollarDotSubUnicodeRestore (items);

            } // end if (items != null)

            f.doCallback (err, items, cb);
            
        });
            
        
    });


}; // end P.find 



//---------------------
P.findOne = (queryOb, projectionOb, cb) => {
    
    v.reportErr = true;

    f.connectCollection (function (collectionOb) {
        
        collectionOb.findOne (queryOb, projectionOb, function (err, item) {

            err = err ? 'P.findOne failed:  query ' + JSON.stringify (queryOb) + '\n' + 
                'projection ' + JSON.stringify (projectionOb) : null;

            if (item !== null) {

                v.ut.dollarDotSubUnicodeRestore (item);

            } // end if (item !== null)

            f.doCallback (err, item, cb);
            
        });
            
    });


}; // end P.find 



//---------------------
P.genObjectId = (hexStr) => {
    
    var oid = new v.objectId (hexStr);
    return oid;

}; // end P.genObjectId


//---------------------
P.getPrimaryKeys = (cb) => {
    
    v.reportErr = true;

    f.connectCollection (function (collectionOb) {

        var map = function () {
            for (var key in this) { emit(key, null); }
        };

        var reduce = function () {
            return null;
        };

        //collectionOb.mapReduce (map, reduce, {out: v.collectionName + '_keys'}, function (err, col) {
        collectionOb.mapReduce (map, reduce, {out: 'zkeys'}, function (err, col) {
            
            if (err) {

                v.errCb ('P.getPrimaryKeys, mapReduce failed: ' + err);
                return;

            } // end if (err)

            col.find().toArray (function (err, arr) {

                var resA;
                if (err) {

                    err = 'P.remove failed:  query ' + JSON.stringify (ob);

                } else {
                    
                    var res = {};
                    for (var i = 0; i < arr.length; i++) {
    
                        var key = arr [i]._id;
                        res [key] = 1;
    
                    } // end for (var i = 0; i < arr.length; i++)
    
                    resA = Object.keys (res);

                } // end if (err)

                f.doCallback (err, resA, cb);
            });

        });
    });
    

}; // end P.getPrimaryKeys 


//---------------------
f.walkPath = (ob, path) => {
    
    var matching = true;
    while (matching) {
        
        var matched = path.match (/(.*?)\.(.*)/);
        if (matched) {

            ob = ob [matched [1]];
            path = matched [2];

        } else {

            ob = ob [path];
            matching = false;

        } // end if (matched)


    } // end while (matching)
    
    return ob;


}; // end f.walkPath 


//---------------------
P.getSecondaryKeys = (keypath, cb) => {
    
    var q = {};
    q [keypath] = {$exists: 1};
    //v.keypath = keypath;
    //var p = {_id: 0};
    var p = {};
    p [keypath] = 1;
    P.find (q, p, function (items) {

        //var kp = v.keypath;
        var res = {};
        var maxLength = 0;
        var values = {};
        var j;
        var keys;
        for (var i = 0; i < items.length; i++) {

            var item = f.walkPath (items [i], keypath);
            if (v.ut.isOb (item)) {

                keys = Object.keys (item);
                for (j = 0; j < keys.length; j++) {

                    res [keys [j]] = 1;

                } // end for (var j = 0; j < keys.length; j++)
                

            } else if (Array.isArray (item)) {

                maxLength = item.length > maxLength ? item.length : maxLength;
                for (j = 0; j < item.length; j++) {

                    var aItem = item [j];
                    if (v.ut.isOb (aItem)) {

                        keys = Object.keys (aItem);
                        for (var k = 0; k < keys.length; k++) {

                            res [keys [k]] = 1;

                        } // end for (var k = 0; k < keys.length; k++)
                

                    } // end if (v.ut.isOb (aItem))
                    

                } // end for (var j = 0; j < item.length; j++)
                

            } else {

                item = typeof item === 'string' ? item : JSON.stringify (item);
                if (values.hasOwnProperty (keypath)) {

                    values [keypath].push (item);

                } else {

                    values [keypath] = [item];

                } // end if (values.hasOwnProperty (keypath))
                
            } // end if (v.ut.isOb (item))
            
        } // end for (var i = 0; i < items.length; i++)

        var resKeys = Object.keys (res);
        if (maxLength > 0) {

            resKeys.push (maxLength);

        } // end if (maxLength > 0)

        if (Object.keys (values).length > 0) {

            resKeys.push (values);

        } // end if (values.length > 0)
        
        
        cb (resKeys);
        
    });

}; // end P.getSecondaryKeys 


//---------------------
P.insert = (ob, options, cb) => {
    
    ob = v.ut.dollarDotSubUnicode (ob);

    v.reportErr = true;

    f.connectCollection (function (collectionOb) {
     
        collectionOb.insert (ob, options ? options : null, function (err, res) {

            err = err ? 'P.insert: err = ' + err : null ;
            f.doCallback (err, res, cb);

        });
        
    });

}; // end P.Insert 


//---------------------
P.listCollections = (cb) => {
    
    f.connect (function (dbOb) {
        
        if (dbOb === null) {

            cb (null);

        } else {
            
            dbOb.listCollections ().toArray (function (err, collections) {
                
                if (err) {
    
                    v.errCb (err);
    
                } else {
    
                    var res = [];
                    for (var i = 0; i < collections.length; i++) {
    
                        var colName = collections [i].name;
                        res.push (colName);
    
                    } // end for (var i = 0; i < collections.length; i++)
                    
                    cb (res);
                        //console.log ('collections: ' + JSON.stringify (collections) + '\n');
    
                } // end if (err)
                
            });

        } // end if (dbOb === null)
        
    });

}; // end P.listCollections 


//---------------------
P.listDatabases = (cb) => {
    
    f.connect (function (dbOb) {
        
        if (dbOb === null) {

            cb (null);

        } else {
            
            var adminDb = dbOb.admin ();
            adminDb.listDatabases (function (err, dbs) {
    
                if (err) {
    
                    v.errCb (err);
    
                } else {
    
                    var res = [];
    
                    var dbA = dbs.databases;
    
                    for (var i = 0; i < dbA.length; i++) {
    
                        var dbName = dbA [i].name;
                        res.push (dbName);
    
                    } // end for (var i = 0; i < dbA.length; i++)
                    
                    cb (res);
                        //console.log ('dbs: ' + JSON.stringify (dbs) + '\n');
    
                } // end if (err)
                
            });

        } // end if (dbOb === null)
        
    });

}; // end P.listDatabases 


//---------------------
P.listUsers = (cb) => {
    // if P.init not yet executed (dbName and all others are null), then use v.initDefaults
    // in any case, when finished, restore init parameters to whatever they were beforehand
    
    f.saveP ();

    var p = v.ut.cloneOb (v.lastInited);

    p.dbName = 'admin';
    p.collectionName = 'system.users';

    P.init (p);

    v.reportErr = true;
    P.find ({}, {user:1, db:1, roles: 1}, function (items) {

        cb (items);
        f.restoreP ();
        
    });

}; // end P.listUsers 


//---------------------
P.listUriCollection = () => {
    
    v.infoCb (v.uri + ' ' + v.collectionName);

}; // end P.listUriCollection


//---------------------
P.remove = (ob, cb) => {
    
    v.reportErr = true;

    f.connectCollection (function (collectionOb) {
        collectionOb.remove (ob, function (err, res) {
            
            err = err ?  'P.remove failed:  query ' + JSON.stringify (ob) : null;
            f.doCallback (err, res, cb);

        });
    });

}; // end P.remove 


//---------------------
P.setCbs = (cb, errCb) => {
    
    v.lastInited.cb = cb;
    v.lastInited.errCb = errCb;
        P.setCollection = (collectionName) => {

            v.lastInited.collectionName = collectionName;
            v.collectionName = collectionName;
            P.updateUri (true);

        }; // end P.setCollection


//---------------------

    v.cb = cb;
    v.errCb = errCb;

}; // end P.setCbs 


//---------------------
P.setDatabase = (dbName) => {
    
    v.lastInited.dbName = dbName;
    v.collectionName = "";
    P.updateUri (false);

}; // end P.setDatabase


//---------------------
P.setHost = (host) => {
    
    v.lastInited.host = host;

    P.updateUri (true);

}; // end P.setHostPort 


//---------------------
P.setPort = (port) => {
    
    v.lastInited.port = port;

    P.updateUri (true);

}; // end P.setHostPort 


//---------------------
P.setUserPwd = (user, pwd) => {
    
    v.lastInited.user = user;
    v.lastInited.pwd = pwd;

    P.updateUri (true);

}; // end P.setUserPwd 


// end PUBLIC section

//---------------------
P.update = (queryOb, updateOb, cb) => {
    
    v.reportErr = true;

    f.connectCollection (function (collectionOb) {
     
        collectionOb.update (queryOb, updateOb, function (err, res) {

            err = err ? 'P.insert: err = ' + err : null ;
            f.doCallback (err, res, cb);

        });
        
    });

}; // end P.Insert 


//---------------------
P.updateOne = (queryOb, updateOb, cb) => {
    
    v.reportErr = true;

    f.connectCollection (function (collectionOb) {
     
        collectionOb.updateOne (queryOb, updateOb, function (err, res) {

            err = err ? 'P.insert: err = ' + err : null ;
            f.doCallback (err, res, cb);

        });
        
    });

}; // end P.Insert 


//---------------------
P.updateMany = (queryOb, updateOb, cb) => {
    
    v.reportErr = true;

    f.connectCollection (function (collectionOb) {
     
        collectionOb.updateMany (queryOb, updateOb, function (err, res) {

            err = err ? 'P.insert: err = ' + err : null ;
            f.doCallback (err, res, cb);

        });
        
    });

}; // end P.Insert 


//---------------------
P.upsert = (queryOb, upsertOb, cb) => {
    
    v.reportErr = true;

    f.connectCollection (function (collectionOb) {
     
        collectionOb.update (queryOb, upsertOb, {upsert: true}, function (err, res) {

            err = err ? 'P.insert: err = ' + err : null ;
            f.doCallback (err, res, cb);

        });
        
    });

}; // end P.Insert 


//---------------------
P.upsertOne = (queryOb, upsertOb, cb) => {
    
    v.reportErr = true;

    f.connectCollection (function (collectionOb) {
     
        collectionOb.updateOne (queryOb, upsertOb, {upsert: true}, function (err, res) {

            err = err ? 'P.insert: err = ' + err : null ;
            f.doCallback (err, res, cb);

        });
        
    });

}; // end P.Insert 


//---------------------
P.upsertMany = (queryOb, upsertOb, cb) => {
    
    v.reportErr = true;

    f.connectCollection (function (collectionOb) {
     
        collectionOb.updateMany (queryOb, upsertOb, {upsert: true}, function (err, res) {

            err = err ? 'P.insert: err = ' + err : null ;
            f.doCallback (err, res, cb);

        });
        
    });

}; // end P.Insert 


//---------------------
P.updateUri = (doCallInfoCb) => {
    
    var p = v.lastInited;

    var upwd = p.user !== "" ? p.user + ':' + p.pwd + '@' : "";
    //v.uri = 'mongodb://' + upwd +  p.host + ':' + p.port + '/' + p.dbName;
    v.dbName = p.dbName;
    v.uri = 'mongodb://' + upwd +  p.host + ':' + p.port;

    if (doCallInfoCb) {

        v.infoCb (v.uri + '/' + v.dbName + ' ' + v.collectionName);

    } // end if (doCallInfoCb)
    

}; // end P.updateUri


f.init ();

return P;

};



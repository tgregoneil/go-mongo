// go-mongo/index.js


module.exports = function (p) {

// PRIVATE Properties/Methods
var _ = {

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

}; // end PRIVATE properties

//---------------------
_.init = () => {
    
    _.lastInited = _.initDefaults;

    var pIn = _.ut.isOb (p) ? p : _.lastInited;

    P.init (pIn);

}; // end _.init

//---------------------
_.connect = (cb) => {
    

    _.mongoClient.connect (_.uri, function (err, dbOb) {

        if (err) {

            if (_.reportErr) {

                _.errCb (err);

            } else {

                cb (null);

            } // end if (_.reportErr)
            

        } else {

            cb (dbOb);

        } // end if (err)
        
    });

}; // end _.connect 


//---------------------
_.connectCollection = (cb) => {
    
    _.connect (function (dbOb) {
        
        if (dbOb === null) {

            cb (null);

        } else {
            
            if (_.collectionName === "") {

                _.errCb ('No collection name specified');

            } else {
                
                var collectionOb = dbOb.collection (_.collectionName);
                cb (collectionOb);

            } // end if (_.collectionName === "")
            

        } // end if (dbOb === null)
        
    });
        
}; // end _.connectCollection 


//---------------------
_.doCallback = (err, res, cb) => {
    
    cb = !cb ? _.cb : cb;
        // providing cb is optional: if not passed, use _.cb as defined in P.init

    if (err) {

        _.errCb (err);

    } else {

        if (cb === console.log && typeof res !== 'string') {

            res = JSON.stringify (res);

        } // end if (cb === console.log && typeof res !== 'string')
        
        cb (res);

    } // end if (err)
    

}; // end _.doCallback 

//---------------------
_.restoreP = () => {
    
    _.uri = _.savedP.uri;
    _.collectionName = _.savedP.collectionName;
    _.cb = _.savedP.cb;
    _.errCb = _.savedP.errCb;
    _.lastInited = _.savedP.lastInited;

    return;

}; // end _.restoreP



//---------------------
_.saveP = () => {

    _.savedP = {
        uri: _.uri,
        collectionName: _.collectionName,
        cb: _.cb,
        errCb: _.errCb,
        lastInited: _.ut.cloneOb (_.lastInited),
    };
        
    return;

}; // end _.saveP


// PUBLIC Properties/Methods
var P = {};

//---------------------
P.init = (pIn) => {
    
    var p = _.ut.pCheck (pIn, _.lastInited);
    _.lastInited = p;

    _.collectionName = p.collectionName;

    P.updateUri (false);

    _.cb = p.cb;
    _.errCb = p.errCb;
    _.infoCb = p.infoCb;

}; // end P.init 


//---------------------
P.createCollection = (collectionName) => {
    
    _.connect (function (dbOb) {
        
        dbOb.createCollection (collectionName, function (err, collection) {
            
            if (err) {

                _.errCb ("mongoOps.createCollection: Couldn't create collection " + collectionName);

            } else {

                _.collectionName = collectionName;

                P.listUriCollection ();

            } // end if (err)
            
        });
    });

}; // end P.createCollection 


//---------------------
P.deleteOne = (ob, cb) => {
    
    _.reportErr = true;

    _.connectCollection (function (collectionOb) {
     
        collectionOb.deleteOne (ob, function (err, res) {

            err = err ? 'P.insert: err = ' + err : null ;
            _.doCallback (err, res, cb);

        });
        
    });

}; // end P.Insert 


//---------------------
P.deleteMany = (ob, cb) => {
    
    _.reportErr = true;

    _.connectCollection (function (collectionOb) {
     
        collectionOb.deleteMany (ob, function (err, res) {

            err = err ? 'P.insert: err = ' + err : null ;
            _.doCallback (err, res, cb);

        });
        
    });

}; // end P.Insert 


//---------------------
P.doReportErr = (reportErr) => {
    
    _.reportErr = reportErr;

}; // end P.doReportErr 


//---------------------
P.find = (queryOb, projectionOb, cb) => {
    
    _.reportErr = true;

    _.connectCollection (function (collectionOb) {
        
        collectionOb.find (queryOb, projectionOb)
        .toArray (function (err, items) {

            err = err ?  'P.find failed:  query ' + JSON.stringify (queryOb) + '\n' + 
                'projection ' + JSON.stringify (projectionOb) : null;

            if (items !== null) {

                _.ut.dollarDotSubUnicodeRestore (items);

            } // end if (items != null)

            _.doCallback (err, items, cb);
            
        });
            
        
    });


}; // end P.find 



//---------------------
P.findOne = (queryOb, projectionOb, cb) => {
    
    _.reportErr = true;

    _.connectCollection (function (collectionOb) {
        
        collectionOb.findOne (queryOb, projectionOb)
        .toArray (function (err, items) {

            err = err ? 'P.findOne failed:  query ' + JSON.stringify (queryOb) + '\n' + 
                'projection ' + JSON.stringify (projectionOb) : null;

            if (items !== null) {

                _.ut.dollarDotSubUnicodeRestore (items);

            } // end if (items !== null)

            _.doCallback (err, items [0], cb);
            
        });
            
    });


}; // end P.find 



//---------------------
P.genObjectId = (hexStr) => {
    
    var oid = new _.objectId (hexStr);
    return oid;

}; // end P.genObjectId


//---------------------
P.getPrimaryKeys = (cb) => {
    
    _.reportErr = true;

    _.connectCollection (function (collectionOb) {

        var map = function () {
            for (var key in this) { emit(key, null); }
        };

        var reduce = function () {
            return null;
        };

        //collectionOb.mapReduce (map, reduce, {out: _.collectionName + '_keys'}, function (err, col) {
        collectionOb.mapReduce (map, reduce, {out: 'zkeys'}, function (err, col) {
            
            if (err) {

                _.errCb ('P.getAllKeys, mapReduce failed: ' + err);
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

                _.doCallback (err, resA, cb);
            });

        });
    });
    

}; // end P.getAllKeys 


//---------------------
_.walkPath = (ob, path) => {
    
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


}; // end _.walkPath 


//---------------------
P.getSecondaryKeys = (keypath, cb) => {
    
    var q = {};
    q [keypath] = {$exists: 1};
    //_.keypath = keypath;
    //var p = {_id: 0};
    var p = {};
    p [keypath] = 1;
    P.find (q, p, function (items) {

        //var kp = _.keypath;
        var res = {};
        var maxLength = 0;
        var values = {};
        for (var i = 0; i < items.length; i++) {

            var item = _.walkPath (items [i], keypath);
            if (_.ut.isOb (item)) {

                var keys = Object.keys (item);
                for (var j = 0; j < keys.length; j++) {

                    res [keys [j]] = 1;

                } // end for (var j = 0; j < keys.length; j++)
                

            } else if (Array.isArray (item)) {

                maxLength = item.length > maxLength ? item.length : maxLength;
                for (var j = 0; j < item.length; j++) {

                    var aItem = item [j];
                    if (_.ut.isOb (aItem)) {

                        var keys = Object.keys (aItem);
                        for (var k = 0; k < keys.length; k++) {

                            res [keys [k]] = 1;

                        } // end for (var k = 0; k < keys.length; k++)
                

                    } // end if (_.ut.isOb (aItem))
                    

                } // end for (var j = 0; j < item.length; j++)
                

            } else {

                item = typeof item === 'string' ? item : JSON.stringify (item);
                if (values.hasOwnProperty (keypath)) {

                    values [keypath].push (item);

                } else {

                    values [keypath] = [item];

                } // end if (values.hasOwnProperty (keypath))
                
            } // end if (_.ut.isOb (item))
            
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
    
    ob = _.ut.dollarDotSubUnicode (ob);

    _.reportErr = true;

    _.connectCollection (function (collectionOb) {
     
        collectionOb.insert (ob, options ? options : null, function (err, res) {

            err = err ? 'P.insert: err = ' + err : null ;
            _.doCallback (err, res, cb);

        });
        
    });

}; // end P.Insert 


//---------------------
P.listCollections = (cb) => {
    
    _.connect (function (dbOb) {
        
        if (dbOb === null) {

            cb (null);

        } else {
            
            dbOb.listCollections ().toArray (function (err, collections) {
                
                if (err) {
    
                    _.errCb (err);
    
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
    
    _.connect (function (dbOb) {
        
        if (dbOb === null) {

            cb (null);

        } else {
            
            var adminDb = dbOb.admin ();
            adminDb.listDatabases (function (err, dbs) {
    
                if (err) {
    
                    _.errCb (err);
    
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
    // if P.init not yet executed (dbName and all others are null), then use _.initDefaults
    // in any case, when finished, restore init parameters to whatever they were beforehand
    
    _.saveP ();

    var p = _.ut.cloneOb (_.lastInited);

    p.dbName = 'admin';
    p.collectionName = 'system.users';

    P.init (p);

    _.reportErr = true;
    P.find ({}, {user:1, db:1, roles: 1}, function (items) {

        cb (items);
        _.restoreP ();
        
    });

}; // end P.listUsers 


//---------------------
P.listUriCollection = () => {
    
    _.infoCb (_.uri + ' ' + _.collectionName);

}; // end P.listUriCollection


//---------------------
P.remove = (ob, cb) => {
    
    _.reportErr = true;

    _.connectCollection (function (collectionOb) {
        collectionOb.remove (ob, function (err, res) {
            
            err = err ?  'P.remove failed:  query ' + JSON.stringify (ob) : null;
            _.doCallback (err, res, cb);

        });
    });

}; // end P.remove 


//---------------------
P.setCbs = (cb, errCb) => {
    
    _.lastInited.cb = cb;
    _.lastInited.errCb = errCb;

    _.cb = cb;
    _.errCb = errCb;

}; // end P.setCbs 


//---------------------
P.setCollection = (collectionName) => {
    
    _.lastInited.collectionName = collectionName;
    _.collectionName = collectionName;
    P.updateUri (true);

}; // end P.setCollection 


//---------------------
P.setDatabase = (dbName) => {
    
    _.lastInited.dbName = dbName;
    _.collectionName = "";
    P.updateUri (false);

}; // end P.setDatabase


//---------------------
P.setHost = (host) => {
    
    _.lastInited.host = host;

    P.updateUri (true);

}; // end P.setHostPort 


//---------------------
P.setPort = (port) => {
    
    _.lastInited.port = port;

    P.updateUri (true);

}; // end P.setHostPort 


//---------------------
P.setUserPwd = (user, pwd) => {
    
    _.lastInited.user = user;
    _.lastInited.pwd = pwd;

    P.updateUri (true);

}; // end P.setUserPwd 


// end PUBLIC section

//---------------------
P.updateOne = (queryOb, updateOb, cb) => {
    
    _.reportErr = true;

    _.connectCollection (function (collectionOb) {
     
        collectionOb.updateOne (queryOb, updateOb, function (err, res) {

            err = err ? 'P.insert: err = ' + err : null ;
            _.doCallback (err, res, cb);

        });
        
    });

}; // end P.Insert 


//---------------------
P.updateMany = (queryOb, updateOb, cb) => {
    
    _.reportErr = true;

    _.connectCollection (function (collectionOb) {
     
        collectionOb.updateMany (queryOb, updateOb, function (err, res) {

            err = err ? 'P.insert: err = ' + err : null ;
            _.doCallback (err, res, cb);

        });
        
    });

}; // end P.Insert 


//---------------------
P.upsertOne = (queryOb, upsertOb, cb) => {
    
    _.reportErr = true;

    _.connectCollection (function (collectionOb) {
     
        collectionOb.updateOne (queryOb, upsertOb, {upsert: true}, function (err, res) {

            err = err ? 'P.insert: err = ' + err : null ;
            _.doCallback (err, res, cb);

        });
        
    });

}; // end P.Insert 


//---------------------
P.upsertMany = (queryOb, upsertOb, cb) => {
    
    _.reportErr = true;

    _.connectCollection (function (collectionOb) {
     
        collectionOb.updateMany (queryOb, upsertOb, {upsert: true}, function (err, res) {

            err = err ? 'P.insert: err = ' + err : null ;
            _.doCallback (err, res, cb);

        });
        
    });

}; // end P.Insert 


//---------------------
P.updateUri = (doCallInfoCb) => {
    
    var p = _.lastInited;

    var upwd = p.user !== "" ? p.user + ':' + p.pwd + '@' : "";
    _.uri = 'mongodb://' + upwd +  p.host + ':' + p.port + '/' + p.dbName;

    if (doCallInfoCb) {

        _.infoCb (_.uri + ' ' + _.collectionName);

    } // end if (doCallInfoCb)
    

}; // end P.updateUri


_.init ();

return P;

};



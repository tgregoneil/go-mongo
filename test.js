#!/usr/bin/node
// test.js

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


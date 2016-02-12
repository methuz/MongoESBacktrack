(function() {
    var async, cursor, col, config, db, eol, missingDocs, errorFile, ElasticSearch, es, finishCheck, noop, retry, fs, yaml, MongoClient, q;

    eol = require('os').EOL;
    noop = function() {};

    ElasticSearch = require('elasticsearch');
    MongoClient = require('mongodb').MongoClient;

    async = require('async');
    fs = require('fs');
    retry = require("retry");
    yaml = require('js-yaml');

    errorFile = fs.createWriteStream('missing_docs.txt');

    try {
        config = yaml.safeLoad(fs.readFileSync('config.yml', 'utf8'));
    } catch (e) {
        console.error('Missing config.yml')
    }

    es = new ElasticSearch.Client({
        hosts: [{
            "host": config.es.url,
            "port": config.es.port,
            "auth": config.es.auth
        }],
        requestTimeout: 60000,
        suggestCompression: true,
        maxSockets: 100
    })

    // Wait until connected
    var i = 0;
    var setUpLoop = setInterval(function() {
        process.stdout.clearLine();
        process.stdout.cursorTo(0);
        i = (i + 1) % 4;
        var dots = new Array(i + 1).join(".");
        process.stdout.write("Connecting to Mongodb" + dots); // write text

        if (db) {
            console.log(eol + "Connected to Mongodb");
            clearInterval(setUpLoop);
        }
    }, 300);

    var connectionString = config.mongo.connection_string;
    MongoClient.connect(connectionString, function(err, _db) {
        if (err)
            throw err
        db = _db
        col = db.collection(config.mongo.collection);
        console.log("Connected to collection", config.mongo.collection);
        start();
    })

    es.ping({
        'index': config.es.index,
        'type': config.es.type
    }, function(error) {
        if (error) {
            console.trace('elasticsearch cluster is down!');
            return
        }
        console.log('Connected to ES');
    });

    function findById(id, callback) {
        var operation = retry.operation();

        var get = {
            'index': config.es.index,
            'type': config.es.type,
            'body': {
                'query': {
                    'match': {
                        '_id': id
                    }
                }
            },
            'routing': '126'
        };

        operation.attempt(function(currentAttemp) {
            es.search(get, function(error, data) {
                if (operation.retry(error)) {
                    console.log('Search Error : Retrying with retry.js');
                    return
                }

                if (error) {
                    callback(operation.mainError());
                    return;
                }

                if (data.hits.total > 0) {
                    callback(null, true);
                    return;
                }

                console.dir(data);
                callback(null, false);
            });

        })

    }

    //Start
    function start() {
        q = async.queue(function(doc, callback) {
            findById(doc._id, function(err, found) {
                if (err) {
                    callback(err);
                    return;
                }
                if (!found) {
                    missingDocs.push(doc._id);
                    errorFile.write(doc._id + '\n');
                }
                callback();
            })
        }, 1000)

        missingDocs = [];
        cursor = col.find({});

        var docCount = 0;
        var totalDoc = Infinity;

        cursor.count(function(err, count) {
            if (err)
                throw err
            totalDoc = count;
            console.log('total docs', totalDoc);
        })

        cursor.on('data', function(doc) {
            docCount++;
            q.push(doc, function(err) {
                if (err)
                    throw err
            })
        })

        finishCheck = setInterval(function() {
            console.log('Doc Count', docCount, 'Total Docs', totalDoc, 'Percentage', 100 * (docCount / totalDoc), 'Total Missing', missingDocs.length, 'Queue Size', q.length())
            if (docCount >= totalDoc) {
                console.log('Finished', 'Total error:', missingDocs.length);
                clearInterval(finishCheck);
                process.exit(0);
            }
        }, 2000);
    }
}).call(this);

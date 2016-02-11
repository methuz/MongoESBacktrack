(function() {
    var config, db, eol, ElasticSearch, es, noop, fs, yaml, MongoClient;

    eol = require('os').EOL;
    noop = function() {};

    yaml = require('js-yaml');
    fs = require('fs');
    MongoClient = require('mongodb').MongoClient;
    ElasticSearch = require('elasticsearch');

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
        requestTimeout: 10000,
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
}).call(this);

(function() {
    var config, noop, fs, yaml;
    noop = function() {};

    yaml = require('js-yaml');
    fs = require('fs');

    try {
        config = yaml.safeLoad(fs.readFileSync('config.yml', 'utf8'));
    } catch (e) {
        console.error('Missing config.yml')
    }

    console.dir(config);
}).call(this);

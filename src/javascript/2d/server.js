var express = require('express'),
    http = require('http'),
    browserify = require('browserify'),
    url = require("url"),
    qs = require("querystring"),
    bodyParser = require("body-parser"),
    app = express();

app.use(bodyParser.json());
app.use(bodyParser.json());

var ITEMS_BACKLOG = 20;

app.get('/', function(req, res) {
    res.send('<html><head><title>Lucene GeoView</title><link rel="stylesheet" href="http://cdn.leafletjs.com/leaflet-0.6.4/leaflet.css"/></head><body><script src="https://ajax.googleapis.com/ajax/libs/jquery/2.1.3/jquery.min.js"></script><script src="bundle.js"></script></html>');
});

app.get('/bundle.js', function(req, res) {
    var b = browserify();
    b.add('./client.js');
    b.bundle().pipe(res);
});

app.get('/real_time_feed', function(req, res) {
    var since = parseInt(qs.parse(url.parse(req.url).query).since, 10);
    feed.query(since, function (data) {
        res.send(data);
    });
});

app.post('/send_feed_item', function (req, res) {
    feed.appendMessage(req.body);
    res.send("SUCCESS");
});

var feed = new function() {
    var real_time_items = [], callbacks = [];

    this.appendMessage = function (json) {
        real_time_items.push( json );
        console.log(new Date() + ": " + JSON.stringify(json) + " pushed");
        while (callbacks.length > 0)
            callbacks.shift().callback([json]);
        while (real_time_items.length > ITEMS_BACKLOG)
            real_time_items.shift();
    };

    this.query = function (since, callback) {
        var matching = [];
	for (var i = 0; i<real_time_items.length; ++i) {
	    var real_time_item = real_time_items[i];
	    if (real_time_item.timestamp > since)
		matching.push(real_time_item);
	}

	if (matching.length != 0) {
	    callback(matching);
	} else {
	    callbacks.push({ timestamp: new Date(), callback: callback });
        }
    };
};

var NOT_FOUND = "Not Found\n";

function notFound(req, res) {
    res.sendwriteHeadHeader(404, [ ["Content-Type", "text/plain"], ["Content-Length", NOT_FOUND.length]]);
    res.write(NOT_FOUND);
    res.end();
}

app.listen(3000);
console.log('Server running at http://localhost:3000/');

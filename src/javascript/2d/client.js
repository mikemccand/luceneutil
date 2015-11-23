var L = require('leaflet');

L.Icon.Default.imagePath = 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.5/images/';

var map;

$(document).ready( function() {
    var div = document.body.appendChild(document.createElement('div'));
    div.style.cssText = 'height:1100px;';
    map = L.map(div).setView([39.6582, -96.5795], 5);
    L.tileLayer('http://a.tile.openstreetmap.org/{z}/{x}/{y}.png').addTo(map);
    longPoll_feed();
});

function longPoll_feed() {
    console.log("longPoll called");
    // make another request
    $.ajax({
        cache: false,
        dataType: 'json',
        type: "GET",
        url: "/real_time_feed",
        error: function() {
            // don't flood the servers on error
            setTimeout(longPoll_feed, 10*1000);
        },
        success: function (json) {
            var j = JSON.stringify(json);
            j = j.substring(1, j.length-1);
	    console.log("adding to map " + j);
            L.geoJson(json).addTo(map);   
	    longPoll_feed();
        }
    });
}


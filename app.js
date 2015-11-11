var mongoose = require('mongoose');
//var config = require('./config');
var fs = require('fs');
var amqp = require('amqplib');


mongoose.connect('mongodb://username:password@serveraddress/twitter_soton');
var db = mongoose.connection;
db.on('error', console.error.bind(console, 'connection error:'));
db.once('open', function (callback) {
    console.log("connected to database");
});



var tweetDoc = new mongoose.Schema({
  source: String,
  status: String,
});



var Message = mongoose.model('Message', tweetDoc); 



// io.on('connection', function (socket) {
//     socket.emit("filter", filter); // emit the current state to this client
//     // receive a filter update, combine it and send to ALL clients
//     socket.on('define_data', function (data) {
//         console.log("Got Website Response:", data.define_data);
//         _.extend(filter, data);
//         //console.log("emitting filter:", filter); 
//         updateDatabaseFromWebsite(data)
        
//     });


//      socket.on('load_data', function (data) {
//         console.log("Loading New Application User");
//         //console.log("emitting filter:", filter); 
//         loadDatabaseData(socket);        
//     });


// });




function showErr (e) {
    console.error(e, e.stack);
}





// function loadDatabaseData(socket){
//     var response = [];
//     Message.find(function (err, responses) {
//     if (err) return console.error(err);
//      //console.log(responses);
//      try{
//      socket.emit("historic_data", responses.slice((responses.length-1000), (responses.length-1)));
//         }catch(e){

//      socket.emit("historic_data", responses);


//         }
//     })

// }







function updateDatabaseWithTweets(data_rec, outName){

    try{
        var data = data_rec.content;

        //console.log("Data: "+data);

  		var doc = new Message({
  			source: outName,
	    	status: data,
       
            });

                doc.save(function(err, doc) {
                if (err) return console.error(err);
                //console.dir(thor);
                });



                // console.log("-----")
                // console.log(data_rec.statuses[status].text)
                // console.log(data_rec.statuses[status].created_at)
                // console.log("-----")
            
            
            //console.log("Added New Items: "+data);
			//emit to real-time
			
        }catch(e){

        }
}




///to connect to the amqp queues

var connectQueueTwo = function (queueName, outName) {
    return amqp.connect("amqp://wsi-h1.soton.ac.uk").then(function(conn) {

        process.once('SIGINT', function() { conn.close(); });
        return conn.createChannel().then(function(ch) {
            var ok = ch.assertExchange(queueName, 'fanout', {durable: false});

            ok = ok.then(function() { return ch.assertQueue('', {exclusive: true}); });

            ok = ok.then(function(qok) {
                return ch.bindQueue(qok.queue, queueName, '').then(function() {
                    return qok.queue;
                });
            });

            ok = ok.then(function(queue) {
                return ch.consume(queue, function (msg) { updateDatabaseWithTweets(msg, outName); }, {noAck: true});
            });

            return ok;
        });
    });
};

var connect = connectQueueTwo("twitter_uk_southampton", "twitter_uk_southampton");

//connect = connect.then(function() { return connectQueueTwo("wikipedia_hose", "wikipedia_revisions"); }, showErr);

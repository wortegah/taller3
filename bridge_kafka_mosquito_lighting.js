var mqtt    = require('mqtt');
var client  = mqtt.connect('mqtt://localhost');
var Kafka   = require('no-kafka');
var producer = new Kafka.Producer();
var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://localhost:27017/";
var deviceCollection = null;

console.log('start service bridge');
// Connection to topic numbers in mosquitto
client.on('connect', function () {
    client.subscribe('lighting');
    console.log('connected to mosquitto');
    initProcess();
});

function initProcess(){
  console.log('initProcess()');  
  MongoClient.connect(url, function(err, db){
    if(err){
      console.log('Error connectiong to mongodb ' + err);
      throw err;
    }
    var _deviceDb = db.db('deviceDb');
    deviceCollection = _deviceDb.collection('deviceCollection');
    startReceiverMosquitto();
  });
}

function startReceiverMosquitto(){
    console.log('startReceiverMosquitto()');
    client.on('message', function (topic, message) {
        console.log(topic +": "+message.toString());
        try {
           validateAndPublishMessage(topic, message);
        }
        catch(err) {
           console.log(err)
           console.log("Error sending message to kafka")
        }
    });
}

function validateAndPublishMessage(topic, message){
  console.log('validateAndPublishMessage('+topic+','+message+')');
  if(topic && message){
    var jsonCall = JSON.parse(message.toString());
    if(jsonCall.id){
      deviceCollection.find(
        {'device_id': jsonCall.id},
        { device_type: 1, device_status: 1, _id: 0}
      ).limit(1).toArray(function (err, docs){
        if(err){
          console.log('Error queriyng deviceCollection for ' + jsonCall.id);
          throw err;
        }
        if(docs && docs.length > 0){
          if(docs[0].device_status.toString().trim() === 'V'){
            publishKafka(topic, message.toString());
          }
        }
      });
    };
  }else{
    console.log('Error with data');
  }
}

function publishKafka(topic, msg){
    producer.init().then(function(){
        console.log('Publish to kafka '+msg+' in topic '+topic);
        return producer.send({
            connectionString: '127.0.0.1:9092',
            topic: topic,
            partition: 0,
            message: {
                value: msg
            }
        });
    });
}

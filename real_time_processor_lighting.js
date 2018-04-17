var Kafka   = require('no-kafka');
var consumer = new Kafka.SimpleConsumer();
var redis = require("redis"),
pub = redis.createClient();
var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://localhost:27017/dataDb";
var dataCollection = null;

var MAX_FACTOR = 9.0;
var MAX_POSSIBLE = 100.0;
var MAX_WARN = 7.0;
var MIN_WARN = 0.0;

console.log('start consumer service');

var dataHandler = function (messageSet, topic, partition) {
  messageSet.forEach(function (m) {
    console.log("Publish to redis: " + m.message.value.toString('utf8') + " from topic " + topic);
    var jsonObject = JSON.parse(m.message.value.toString('utf8'));
    var convertedValue = convertMedition(jsonObject.data.medition);
    validateBusinessRule(convertedValue);
    jsonObject.data.medition = convertedValue;
    pub.publish(topic, JSON.stringify(jsonObject.data));
    saveRecord(jsonObject);
  });
};

function saveRecord(record){
  MongoClient.connect(url, function(err, db){
    if(err){
      console.log('Error connectiong to mongodb ' + err);
      throw err;
    }
    var _deviceDb = db.db('dataDb');
    dataCollection = _deviceDb.collection('dataCollection');
    dataCollection.insertOne(record, function(err, res){
      if(err) throw err;
      console.log("record inserted");
      db.close();
    });
  });
}

function validateBusinessRule(medition){
  if(medition > MAX_WARN || medition < MIN_WARN){
    // TODO Generar Alerta y Guardar en MongoDB
  }
}

function convertMedition(medition){
  return ( parseFloat(medition) * MAX_FACTOR ) / MAX_POSSIBLE;
}

return consumer.init().then(function () {
  return consumer.subscribe('lighting', dataHandler);
});

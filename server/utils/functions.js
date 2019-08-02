const  MongoClient = require('mongodb').MongoClient,
        axios = require('axios'),
        kafka = require('kafka-node'),
        fs = require('fs');

  async function uploadToHdfs(invoice, fileName) {
    try {
      const response = await axios.put(
          `http://192.168.99.100:50075/webhdfs/v1/invoices/${fileName}?op=CREATE&namenoderpcaddress=localhost:8020&createflag=&createparent=true&overwrite=false&user.name=root`,
          invoice
        );
        if (response) {
          console.log("Invoice upload to hdfs");
        }
    } catch (error) {
      console.error(`${fileName} already exists in hdfs`);
    }
  }

  function uploadToMongo(fileName) {
    const dbUrl = 'mongodb://127.0.0.1:27017/';
    var file = fs.readFileSync(`src/server/public/files/${fileName}`);
    let invoice = JSON.parse(file);

    MongoClient.connect(dbUrl,{ useNewUrlParser: true }, function(err, db) {
      if (err) throw err;
      var dbo = db.db("BigData");
      dbo.collection("Invoices").insertOne(invoice, function(err, res) {
        if (err) throw err;
        console.log(`${fileName} insert to invoices collection`);
        db.close();
      });
    });
  }

  async function sendToKafka(fileName) {
    console.log("publish to kafka consumer...");
    var file = fs.readFileSync(`src/server/public/files/${fileName}`);
    let invoice = JSON.parse(file);

    let body = {
      records:[
        {value: invoice}
      ]
    }

    try {
      const options = {
        method: "POST",
        url: 'http://35.208.177.111:8082/topics/bigdata',
        data: JSON.stringify(body),
        headers: {
          'Content-Type': 'application/vnd.kafka.json.v2+json'
        }
      }
      const response = await axios(options);
      if (response) {
        console.log("invoice published to kafka!");
      }
    } catch (error) {
      console.log(error);
      console.error("something went wrong sending to kafka..");
    }
  }

module.exports = {sendToKafka, uploadToMongo, uploadToHdfs}

/* insert new invoices to MongoDB
 if the invoice provider is in the db it will update is invoices array, else will create new provider */

const MongoClient = require('mongodb').MongoClient,
  assert = require('assert'),
  fs = require('fs')

const dbUrl = 'mongodb://127.0.0.1:27017/';

function mongoVendorCollection(fileName) {
  var file = fs.readFileSync(`src/server/public/files/${fileName}`);
  let invoice = JSON.parse(file);

  MongoClient.connect(dbUrl, { useNewUrlParser: true }, function(err, client) {
    assert.equal(null, err);
    console.log("Connected correctly to server");
    const db = client.db("BigData");
    insertDocument(db, invoice, function() {
      client.close();
     });
  });
}

function insertDocument(db, invoice, callback) {
   // first try to update; if a document could be updated, we're done
   console.log("Processing for "+ invoice.provider);
   updateProviderInvoices( db, invoice, function (results) {
       if (!results || results.result.n == 0) {
         let newProvider = {
           provider: invoice.provider,
           lastModified: false,
           invoices: [invoice],
           totalOrders: 1,
           totalSum: invoice.total
         }
          // the document was not updated so presumably it does not exist; let's insert it
          db.collection("vendors").insertOne(
            newProvider , function(err, res) {
              if (err) throw err;
              console.log("created new provider: "+invoice.provider);
              callback();
            });
       } else {
         callback();
       }
     });
  };

var updateProviderInvoices = function(db, invoice , callback) {
  console.log(invoice.provider);
   db.collection("vendors").updateOne(
      { "provider" : invoice.provider },
      {
        $push: { invoices:  invoice },
        $inc: { totalOrders: 1, totalSum: parseFloat(invoice.total) },
        $currentDate: { "lastModified": true }
      }).then((obj) => {
            console.log('Updated - ' + obj);
            callback(obj)
        }).catch((err) => {
           console.log('Error: ' + err);
      });
};

module.exports = {mongoVendorCollection};

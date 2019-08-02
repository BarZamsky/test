const MongoClient = require('mongodb').MongoClient,
assert = require('assert');


const dbUrl = 'mongodb://127.0.0.1:27017/';

let db;

const vendors = ["Rami Levy - Hashikma Marketing", "Mega", "Shufersal", "Hazi-Hinam", "Osher ad",
          "Victory", "Tiv-Taam", "AM:PM"];

const months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];

  function getVolume(productName, start, end, callBack) {
  MongoClient.connect(dbUrl, { useNewUrlParser: true }, function(err, client) {
    assert.equal(null, err);
    console.log("Connected correctly to server");
    db = client.db("BigData");

  let vendorProductCount = [];
    let res = db.collection("vendors").find({
      invoices: {
          $elemMatch: {
            date: {$gte: new Date(start).toISOString(),
                  $lt: new Date(end).toISOString()}}},
    }).toArray(function(err, result) {
        if (err) throw err;

        for (res of result) {
          let elem = {
            vendor: res["provider"],
            volume: 0
          }
          for (invoice of res["invoices"]) {
            for (item of invoice["items"]) {
              if (item.id == productName) {
                elem['volume']++;
              }
            }
          }
          vendorProductCount.push(elem)
        }
        for (const vendor of vendors) {
          if (!vendorProductCount.some(item => item.vendor === vendor)) {
            let newVendor = {
              vendor: vendor,
              volume: 0
            }
            vendorProductCount.push(newVendor);
          }
        }
        return callBack(vendorProductCount);
      });
    });
}

  function getPriceChange(productName, start, end, callBack) {
    MongoClient.connect(dbUrl, { useNewUrlParser: true }, function(err, client) {
      assert.equal(null, err);
      console.log("Connected correctly to server");
      db = client.db("BigData");

    let productPrices = [];
      let res = db.collection("vendors").find({
        invoices: {
            $elemMatch: {
              date: {$gte: new Date(start).toISOString(),
                    $lt: new Date(end).toISOString()}}},
      }).toArray(function(err, result) {
          if (err) throw err;

          for (res of result) {
            let elem = {
              vendor: res["provider"],
              prices: 0
            }
            for (invoice of res["invoices"]) {
              for (item of invoice["items"]) {
                if (item.id == productName) {
                  elem['prices'] = item.price;
                }
              }
            }
            productPrices.push(elem)
          }
          for (const vendor of vendors) {
            if (!productPrices.some(item => item.vendor === vendor)) {
              let newVendor = {
                vendor: vendor,
                prices: 0
              }
              productPrices.push(newVendor);
            }
          }
          return callBack(productPrices);
        });
      });
  }

  function getVendorsData(callBack) {
    MongoClient.connect(dbUrl, { useNewUrlParser: true }, function(err, client) {
      assert.equal(null, err);
      console.log("Connected correctly to server");
      db = client.db("BigData");

      let vendorsData = [];

      db.collection("vendors").find({}).toArray(function(err, result) {
        if (err) throw err;
        console.log(result);

        for (res of result) {
          let vendor = {
            name: res.provider,
            orders: res.totalOrders,
            sum: res.totalSum
          }
          vendorsData.push(vendor);
        }

        for (const vendor of vendors) {
          if (!vendorsData.some(item => item.name === vendor)) {
            let newVendor = {
              name: vendor,
              orders: 0,
              sum: 0
            }
            vendorsData.push(newVendor);
          }
        }

        return callBack(vendorsData);
      });
    })
  }

  function getVendorInvoicesSummary(vendor, callBack) {
    MongoClient.connect(dbUrl, { useNewUrlParser: true }, function(err, client) {
      assert.equal(null, err);
      console.log("Connected correctly to server");
      db = client.db("BigData");


      let res = db.collection("vendors").find({
        provider: vendor
      }).toArray(function(err, result) {
          if (err) throw err;
          console.log(result);
          let invoices = [];
          if (result.length == 0) {
            return callBack(result);
          }

          for (const m of months) {
            let month = {
              month: m,
              count: 0
            }
            invoices.push(month);
          }

          for (const invoice of result[0]['invoices']) {
            let date = new Date(invoice['date']);
            let m = months[date.getMonth()];
            let index = invoices.findIndex(x => x.month === m);
            invoices[index].count++;
          }

          return callBack(invoices);
      });
    });
  };

module.exports = {getVolume, getPriceChange, getVendorsData, getVendorInvoicesSummary};

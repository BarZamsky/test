const express = require('express'),
 bodyParser = require('body-parser'),
 os = require('os'),
 path = require('path')
 cors = require('cors'),
 fileUpload = require('express-fileupload'),
 {uploadToHdfs, uploadToMongo, sendToKafka} = require('./utils/functions'),
 {mongoVendorCollection} = require('./mongo/mongo'),
 {getVolume, getPriceChange, getVendorsData, getVendorInvoicesSummary} = require('./mongo/queries')

 const app = express();

app.use(cors())
app.use(bodyParser.urlencoded({ extended: false }))
app.use(bodyParser.json())
app.use(express.static('dist'));
app.use(fileUpload());
app.use(express.static(__dirname));
app.use(express.static(path.join(__dirname, 'build')));

app.get('/*', function (req, res) {
  res.sendFile(path.join(__dirname, 'build', 'index.html'));
});

app.post('/api/upload', (req, res, next) => {
  let uploadFile = req.files.file
  const fileName = req.files.file.name
  uploadFile.mv(
    `${__dirname}/public/files/${fileName}`, function (err) {
      if (err) {
        res.status(200).send("error occoured!")
      } else {
        try {
          sendToKafka(fileName)
          uploadToHdfs(uploadFile.data, fileName);
          uploadToMongo(fileName);
          mongoVendorCollection(fileName);
          next();
        } catch(e) {
          console.log(e);
        }
      }});
  });

  app.get('/api/best-seller', (req, res) => {
    res.send('test')
  })

  app.post('/api/product-volume', async (req, res) => {
    if (!req['body']) {
      res.send("no body")
    }
    console.log(req.body);
    let productName = req.body['productName'];
    let start = req.body['start'];
    let end = req.body['end'];

    getVolume(productName, start, end, function(result) {
      res.status(200).send(result);
    });
  })

  app.post('/api/product-price', async (req, res) => {
    if (!req['body']) {
      res.send("no body")
    }
    console.log(req.body);
    let productName = req.body['productName'];
    let start = req.body['start'];
    let end = req.body['end'];

    getPriceChange(productName, start, end, function(result) {
      res.status(200).send(result);
    });
  })

  app.get('/api/vendors', async (req, res) => {
    getVendorsData(function(result) {
      res.status(200).send(result);
    });
  })

  app.get('/api/:vendorId', async (req, res) => {
    if (!req.params['vendorId']) {
      res.status(404).send();
    } else {
      let vendor = req.params['vendorId'];
      getVendorInvoicesSummary(vendor, function(result) {
        res.status(200).send(result);
      });
    }
  });

app.listen(process.env.PORT || 8080, () => console.log(`server running on port ${process.env.PORT || 8080}!`));

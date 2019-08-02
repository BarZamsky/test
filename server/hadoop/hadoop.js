const express = require('express'),
  fs = require('fs'),
  WebHDFS = require('webhdfs');

  let hdfs = WebHDFS.createClient({
      user: "root",
      host: "192.168.99.100",
      port: 50070, //change here if you are using different port
      path: "webhdfs/v1/"
  });

  let localFileStream = fs.createReadStream('./../public/files/invoice7.json');
  // Initialize writable stream to HDFS target
  let remoteFileStream = hdfs.createWriteStream('/tmp/hadoop-root/dfs/name');
  // Pipe data to HDFS
  localFileStream.pipe(remoteFileStream);
  // Handle errors
  remoteFileStream.on('error', function onError (err) {
    console.log("error uploading given file...", err);
  });
  // Handle finish event
  remoteFileStream.on('finish', function onFinish () {
    console.log("file was upload to hdfs!");
  });

(function () {
    "use strict";
    const express = require("express");
    const fs = require("fs");
    const csvTransform = require("csv-to-json-stream");
    const stream = require("stream");
    const Transform = stream.Transform;
    const transformStream = new Transform({
        transform(chunk, encoding, callback) {
            callback(null, chunk);
        }
    });
    const app = express();
    const server = require("http").createServer(app);
    const appPort = 6009;
    const bulkSize = 10000;
    const csv = {
        "delimiter": ",",
        "skipHeader": true,
        "map": {
            "id": 0,
            "day": 1,
            "wday": 2,
            "time": 3,
            "lat": 4,
            "lng": 5,
            "state": 6
        }
    };

    let length = 0;
    let blocks = 0;
    let documents = [];
    let document = {};
    let stayPoints = {};
    let keys = [];

    transformStream.on("data", function(chunk){
        document = JSON.parse(chunk.toString());
        //length = Object.keys(stayPoints).length;
        if(document.state === "SP" && documents.length < bulkSize){
            documents.push(document);
            stayPoints[document.id] = (stayPoints[document.id]) ? stayPoints[document.id] + 1 : 1;
            //stayPoints[document.id] = document;
            //console.log(documents.length);
        }
        if (documents.length === bulkSize) {
            blocks++;
            keys = Object.keys(stayPoints);
            console.log("block "+blocks+" documents: "+documents.length);
            console.log("users "+keys.length);
            console.log(stayPoints);
            this.pause();

            documents = [];
        }
    }).on("end", () => {
        console.log("finish reading");
    });


    fs.createReadStream("/Users/john/Downloads/bigsample.csv")
        .pipe(csvTransform(csv))
        .pipe(transformStream);
    // http://experiance-jam-4.s3-sa-east-1.amazonaws.com/

    //let s3 = new AWS.S3({apiVersion: '2006-03-01'});
    //let params = {Bucket: 'myBucket', Key: 'myImageFile.jpg'};



    require("./server/routes/index")(app);


    server.listen(appPort, function () {
        process.stdout.write(["Server running on port:", appPort].join(" "));
    });

}());
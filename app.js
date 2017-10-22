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
    const bulkSize = 1000000;
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
    const daysOfWeek = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"];

    let blocks = 0;
    let documents = [];
    let document = {};
    let stayPoints = {};
    let userFrequency = {};
    let key = "";
    let keys = [];
    let dayOfweek = 0;

    function compareTime(a,b) {
        if (new Date("01/01/90 "+a.time).getTime() < new Date("01/01/90 "+b.time))
            return -1;
        if (new Date("01/01/90 "+a.time).getTime() > new Date("01/01/90 "+b.time))
            return 1;
        return 0;
    }

    transformStream.on("data", function(chunk){
        document = JSON.parse(chunk.toString());
        //length = Object.keys(stayPoints).length;
        if(document.state === "SP" && documents.length < bulkSize){
            documents.push(document);
            dayOfweek = daysOfWeek.indexOf(document.wday.toLowerCase());
            if(!stayPoints[document.id]){
                stayPoints[document.id] = {};
                stayPoints[document.id]["state"] = document.state;
                stayPoints[document.id]["daysOfWeek"] = {   "0": {"frequency": 0, "coordinates": []},
                                                            "1": {"frequency": 0, "coordinates": []},
                                                            "2": {"frequency": 0, "coordinates": []},
                                                            "3": {"frequency": 0, "coordinates": []},
                                                            "4": {"frequency": 0, "coordinates": []},
                                                            "5": {"frequency": 0, "coordinates": []},
                                                            "6": {"frequency": 0, "coordinates": []}};
            }

            stayPoints[document.id]["daysOfWeek"][dayOfweek].frequency = stayPoints[document.id]["daysOfWeek"][dayOfweek].frequency + 1;
            stayPoints[document.id]["daysOfWeek"][dayOfweek].coordinates.push({"lat": document.lat, "lng": document.lng, "time": document.time});
            userFrequency[document.id] = (stayPoints[document.id]) ? stayPoints[document.id] + 1 : 1;
            //stayPoints[document.id] = document;
            //console.log(documents.length);
        }
        if (documents.length === bulkSize) {
            blocks++;
            keys = Object.keys(stayPoints);
            console.log("\nBlock "+blocks+" documents: "+documents.length);
            console.log("Total users "+keys.length);
            key = keys.indexOf("6dc43b2edaf144f7c0a209c2ce6288258f9aefdde8da93b1236310f8351c4c77f6719a0388ec99ecf0c206306a487119");

            console.log("Users "+key);
            for(let i in stayPoints[keys[key]]["daysOfWeek"]){
                stayPoints[keys[key]]["daysOfWeek"][i].coordinates.sort(compareTime)
            }
            //console.log(keys[406]);

            console.log(JSON.stringify(stayPoints["6dc43b2edaf144f7c0a209c2ce6288258f9aefdde8da93b1236310f8351c4c77f6719a0388ec99ecf0c206306a487119"], undefined, 2));
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
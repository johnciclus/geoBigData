(function () {
    "use strict";
    const express = require("express");
    const app = express();
    const server = require("http").createServer(app);
    const appPort = 6009;

    require("./server/routes/index")(app);


    server.listen(appPort, function () {
        process.stdout.write(["Server running on port:", appPort].join(" "));
    });

}());
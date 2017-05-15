var express             =   require("express");
var app                 =   express();
var bodyParser          =   require("body-parser");
var path                =   require("path");
var router              =   express.Router();
var queryModel          =   require("./models/query");
var analytics_4         =   require("./models/analytics_4");

router.get("/",function(req,res){
    res.sendFile(path.join(__dirname + '/index.html'));
});

router.get("/index.html",function(req,res){
    res.sendFile(path.join(__dirname + '/index.html'));
});

router.get("/state-prediction.html",function(req,res){
    res.sendFile(path.join(__dirname + '/state-prediction.html'));
});

router.get("/analytics.html",function(req,res){
    //res.json({"error" : false,"message" : "Hello World"});
    res.sendFile(path.join(__dirname + '/analytics.html'));
});

router.get("/file4/state/:state/start_date/:start_date/end_date/:end_date/start_time/:start_time/end_time/:end_time/dma/:dma",function(req,res){
    console.log("file4 request")
    queryModel.file_4(req, function(err, data){
        if(err){
            //res.json({"error" : true,"message" : "Error processing the query."});
            res.status(500).send("Error processing the query.");
        }else{
            res.status(200).json(data);
        }
    });

});

router.get("/api/state/:state/ad_id/:ad_id",function(req,res){
    var time_0 = Date.now()/1000
    console.log(time_0)
    queryModel.dbQuery(req, function(err, data){
        if(err){
            //res.json({"error" : true,"message" : "Error processing the query."});
            res.status(500).send("Error processing the query.");
        }else{
            //res.status(200).json(data);
            var time_1 = Date.now()/1000
            var seconds = time_1 - time_0
            console.log("Request was processed in " + seconds + " seconds.")
            res.status(200).json(data);
        }
    });

});


app.use('/',router);

var port = process.env.PORT || 5000;
var server = app.listen(port);
console.log("Listening to PORT "+ port);

var http = require ('http');

server.timeout = 500000;  


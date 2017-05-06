var express             =   require("express");
var app                 =   express();
var bodyParser          =   require("body-parser");
var router              =   express.Router();
var dbconnection        =   require('./dbconnection.js');

dbconnection.connect(function(err){
 if(!err) {
     console.log("Database is connected ...");  
 } else {
     console.log("Error connecting database ... \n\n");  
 }
 });

router.get("/",function(req,res){
    res.json({"error" : false,"message" : "Hello World"});
});

router.get("/api",function(req,res){
    dbconnection.query('SELECT * from events LIMIT 2', function(err, rows, fields) {
        dbconnection.end();
        if (!err){
            res.json(rows)
            console.log('The solution is: ', JSON.stringify(rows));
        }else{
            res.json({"error" : true,"message" : "Error processing the query."});
        }
    });

});


app.use('/',router);

var port = process.env.PORT || 5000;
app.listen(port);
console.log("Listening to PORT "+ port);

var http = require ('http');


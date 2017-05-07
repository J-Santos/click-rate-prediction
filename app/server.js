var express             =   require("express");
var app                 =   express();
var bodyParser          =   require("body-parser");
var router              =   express.Router();
var queryModel          =   require("./models/query");

router.get("/",function(req,res){
    res.json({"error" : false,"message" : "Hello World"});
});

// router.get("/api",function(req,res){
//     dbconnection.query('SELECT count(*) as count from events where country = "US"', function(err, rows, fields) {
//         dbconnection.end();
//         if (!err){
//             res.json(rows)
//             //console.log('The solution is: ', JSON.stringify(rows));
//         }else{
//             res.json({"error" : true,"message" : "Error processing the query."});
//         }
//     });

// });

router.get("/api/ad_id/:ad_id",function(req,res){
    // dbconnection.query('SELECT count(*) as count from events where country = "US"', function(err, rows, fields) {
    //     dbconnection.end();
    //     if (!err){
    //         res.json(rows)
    //         //console.log('The solution is: ', JSON.stringify(rows));
    //     }else{
    //         res.json({"error" : true,"message" : "Error processing the query."});
    //     }
    // });
    queryModel.dbQuery(req, function(err, data){
        if(err){
            //res.json({"error" : true,"message" : "Error processing the query."});
            res.status(500).send("Error processing the query.");
        }else{
            res.status(200).json(data);
        }
    });

});


app.use('/',router);

var port = process.env.PORT || 5000;
app.listen(port);
console.log("Listening to PORT "+ port);

var http = require ('http');


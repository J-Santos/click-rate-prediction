var mysql=require('mysql');

// var connection=mysql.createConnection({
//  host:'localhost',
//  user:'root',
//  password:'',
//  database:'cmpe239projectdb'
 
// });

var connection=mysql.createConnection({
 host:'jesdbinstance.cimw8kngg4r8.us-west-2.rds.amazonaws.com',
 user:'dbuser',
 password:'239dbuser',
 database:'cmpe239projectdb'
 
});
module.exports=connection;
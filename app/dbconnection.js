var mysql=require('mysql');

var connection=mysql.createConnection({
 host:'localhost',
 user:'root',
 password:'',
 database:'cmpe239projectdb'
 
});
module.exports=connection;
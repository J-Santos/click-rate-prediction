var async             =   require("async");
var dbconnection      =   require('../dbconnection.js');
var stateVariables    =   require("../state_variables");




dbconnection.connect(function(err){
 if(!err) {
     console.log("Database is connected ...");  
 } else {
     console.log("Error connecting database ... \n\n");  
 }
 });

exports.dbQuery = function (req, func){
	//selectQuery();
	console.log("Ad id: ",req.params.ad_id);
	// User.findOne({'email': req.params.user_id}).exec(function(err, user) {
 //        callback(err,user);
 //    });

 	selectQuery(func)
 	// console.log("RESULTS: ")
 	// console.log(results)
 	// if(resultserr){
 	// 	err = results.err;
 	// }else{
 	// 	err = null
 	// }
 	//callback(results[0], results[1])

    // dbconnection.query('SELECT count(*) as count from events where country = "US"', function(err, rows, fields) {
    //     //dbconnection.end();
    //     callback(err, rows)
    //     // if (!err){
    //     //     res.json(rows)
    //     //     //console.log('The solution is: ', JSON.stringify(rows));
    //     // }else{
    //     //     res.json({"error" : true,"message" : "Error processing the query."});
    //     // }
    // });

}

function selectQuery(func){
	console.log("hello world");
	//func(null, {'ad_id': '1234'})
	// var func_arr = []
	// for(state in stateVariables.state_map){
	// 	// var func_name = "functionState"+ state
	// 	// console.log("functionState"+state);
	// 	// //console.log(state_map[state]);
	// 	// var "functionState"+ state = "hello"
	// 	// console.log("functionState"+ state)
	// 	//console.log(state_name)
	// 	var func = function(callback){
	// 		//console.log(state_name)
	// 		var stateObject = new State(state)
	// 		selectQueryByState(stateObject, callback)
	// 	}
	// 	func_arr.push(func)
	// }
	// console.log(func_arr)
	// var functionOne = function(callback){
	// 	selectQueryByState('CA', callback)
	// }

	// var functionTwo = function(callback){
	// 	selectQueryByState('TX', callback)
	// }
	async.parallel(stateVariables.state_func, function (err, result) {
		     //This code will be executed after all previous queries are done (the order doesn't matter).
		     //For example you can do another query that depends of the result of all the previous queries.
		//console.log("Async")
		func(err,result)
	});
	//selectQueryByState('CA')

	// var data = {}
	// dbconnection.query('SELECT count(*) as count from events where country = "US"', function(err, rows, fields) {
 //        printData(err, rows)
 //    });
    //return data;
}

results_data = {}
function printData(state, err, rows) {
	var parser = JSON.parse(JSON.stringify(rows))
	console.log(state)
	console.log(parser[0].count)
	//console.log(JSON.stringify(rows)[0].count)
	//results_data.push(rows)
	// body...
}

function selectQueryByState(state, callback){
	//var queryStatement = 'SELECT count(*) as count from events where country = "US" and state = "' + state + '"';
	var queryStatement = 'SELECT count(*) as count from '+ state +'_events';// where country = "US" and state = "' + state + '"';
	dbconnection.query(queryStatement, function(err, rows, fields) {
		var parser = JSON.parse(JSON.stringify(rows))
		var data = {'state': state, 'count': parser[0].count}
		console.log("Finished query of select for US and "+ state)
        callback(err, data)
    });
}


function State (name) {
    this.name = name;
}

exports.selectQueryByState = selectQueryByState
// async.parallel([
//     firstQueryFunction,
//     secondQueryFunction,
//     thirdQueryFunction,
//     fourthQueryFunction
// ], function (err, result) {
//      //This code will be executed after all previous queries are done (the order doesn't matter).
//      //For example you can do another query that depends of the result of all the previous queries.
// });
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

exports.dbQuery = function (req, callback){
 	var state = req.params.state
 	var adId = req.params.ad_id
 	var queryStateGivenClickedAd = 'select count(*) as count from ' + 
		'(select t1.display_id from (select display_id from clicked_ads where clicked_ad_id = '+ adId +')t1) as temp1 ' +
		'inner join ' +
		'(select * from '+ state +'_events) as temp2 ' +
		'on temp1.display_id = temp2.display_id'

	var queryState = 'select count(*) as count from '+ state +'_events'
	var queryUS = 'select count(*) as count from US_events'
	var queryClicks = 'select count(*) as count from clicked_ads where clicked_ad_id = '+ adId
	var queryCtr = 'select ctr as count from click_rate where ad_id = '+ adId

	var functionGivenClicked = function(callback1){
    	performQueryCount(queryStateGivenClickedAd, 'clicked_ad', callback1)
	}

	var functionState = function(callback2){
    	performQueryCount(queryState, 'state', callback2)
	}

	var functionUS = function(callback3){
    	performQueryCount(queryUS, 'us', callback3)
	}

	var functionClicks = function(callback4){
    	performQueryCount(queryClicks, 'clicks', callback4)
	}

	var functionCtr = function(callback5){
    	performQueryCount(queryCtr, 'ctr', callback5)
	}

	var query_arr = [
    	functionGivenClicked, functionState, functionUS, functionClicks, functionCtr
	]
 	async.parallel(query_arr, function (err, result) {
 		var temp = {}
 		for (i in result){
 			keys = Object.keys(result[i])
 			for (key in keys){
 				temp[keys[key]] = result[i][keys[key]]
 			}
 		}
 		var data = {'ad_id': adId, 'state': state, 'count': temp}
		callback(err,data)
	});

}

function performQueryCount(query, name, callback){
	//var queryStatement = 'SELECT count(*) as count from '+ state +'_events';// where country = "US" and state = "' + state + '"';
	dbconnection.query(query, function(err, rows, fields) {
		//dbconnection.end()
		var parser = JSON.parse(JSON.stringify(rows))
		var key = "'" + name +"'"
		console.log(name)
		var count = parser[0].count
		var data = {}
		if(name == 'clicked_ad'){
			data = {'clicked_ad': count}
		}else if(name == 'state'){
			data = {'state': count}
		}else if(name == 'us'){
			data = {'us': count}
		}else if(name == 'clicks'){
			data = {'clicks': count}
		}else if(name == 'ctr'){
			data = {'ctr': count}
		}

		//var data = {key: parser[0].count}
		console.log("Finished performing query: " + query)
        callback(err, data)
    });

}

function selectQuery(func){
	async.parallel(stateVariables.state_func, function (err, result) {
		func(err,result)
	});
}

results_data = {}
function printData(state, err, rows) {
	var parser = JSON.parse(JSON.stringify(rows))
	console.log(state)
	console.log(parser[0].count)
}

function selectQueryByStateCopy(state, callback){
	//var queryStatement = 'SELECT count(*) as count from events where country = "US" and state = "' + state + '"';
	var queryStatement = 'SELECT count(*) as count from '+ state +'_events';// where country = "US" and state = "' + state + '"';
	dbconnection.query(queryStatement, function(err, rows, fields) {
		//dbconnection.end()
		var parser = JSON.parse(JSON.stringify(rows))
		var data = {'state': state, 'count': parser[0].count}
		console.log("Finished query of select for US and "+ state)
        callback(err, data)
    });
}

function selectQueryByState(state, callback){
 	var functionX = function(callback1){
    	selectQueryByStateGivenClickedAd(state, callback1)
	}
	var state_func_test_1 = [
    	functionX
	]
 	async.series(state_func_test_1, function (err, result) {
		callback(err,result)
	});
}

function selectQueryByStateGivenClickedAd(state, callback){

	var queryStatement = 'select count(*) as count from ' + 
		'(select t1.display_id from (select display_id from clicked_ads where clicked_ad_id = 88230)t1) as temp1 ' +
		'inner join ' +
		'(select * from '+ state +'_events) as temp2 ' +
		'on temp1.display_id = temp2.display_id'

	//var queryStatement = 'select display_id, clicked_ad_id from clicked_ads where clicked_ad_id = 88230';
	//console.log(queryStatement)
	//var queryStatement = 'SELECT count(*) as count from '+ state +'_events';// where country = "US" and state = "' + state + '"';
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

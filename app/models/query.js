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

 	///check if this was processed already, ad_id-state combination
 	var queryCheck = 'select * from counts_per_ad_id_and_state where ad_id = '+adId +' and state = "' +state +'"'

 	dbconnection.query(queryCheck, function(err, rows, fields) {
 		if(rows.length == 0){
 			console.log("This entry doesn't exist. Processing all queries.")
 			var insertTemp1 = 'create table temp1 as (select display_id from clicked_ads where clicked_ad_id = '+adId+')'
 			dbconnection.query(insertTemp1, function(err, rows, fields) {
 				var insertTemp2 = 'create table temp2 as (select display_id from '+ state +'_events)'
 				dbconnection.query(insertTemp2, function(err, rows, fields) {
 					processNewQueries(adId, state, callback)
 				});
 			});
 		}else{
 			console.log("This entry already exist. Returning results.")
 			callback(err, rows[0])
 		}

 	});
}

function performQueryCount(query, name, callback){
	console.log(query)
	//var queryStatement = 'SELECT count(*) as count from '+ state +'_events';// where country = "US" and state = "' + state + '"';
	dbconnection.query(query, function(err, rows, fields) {
		//dbconnection.end()
		if(err){
			console.log(err);
		}else{
			console.log("QUERY: "+ query)
			console.log("ROWS: "+ rows)
			var parser = JSON.parse(JSON.stringify(rows))
			//var key = "'" + name +"'"
			//console.log(name)
			var count = parser[0].count
			var data = {}
			if(name == 'clicks_per_state'){
				data = {'clicks_per_state': count}
			}else if(name == 'total_state'){
				data = {'total_state': count}
			}else if(name == 'total_us'){
				data = {'total_us': count}
			}else if(name == 'total_clicks'){
				data = {'total_clicks': count}
			}else if(name == 'ctr'){
				data = {'ctr': count}
			}
			console.log("Finished performing query: " + query)
	        callback(err, data)
	    }
    });

}

function processNewQueries(adId, state, callback){
	// var queryStateGivenClickedAd = 'select count(*) as count from ' + 
	// 	'(select t1.display_id from (select display_id from clicked_ads where clicked_ad_id = '+ adId +')t1) as temp1 ' +
	// 	'inner join ' +
	// 	'(select * from '+ state +'_events) as temp2 ' +
	// 	'on temp1.display_id = temp2.display_id'


	// var queryStateGivenClickedAd = 'select count(*) as count from (' + 
	// 	'(select display_id from clicked_ads where clicked_ad_id = '+ adId +') as temp1 ' +
	// 	'inner join ' +
	// 	'(select * from '+ state +'_events) as temp2 ' +
	// 	'on temp1.display_id = temp2.display_id)'

	var queryStateGivenClickedAd = 'select count(*) as count from ' + 
		'(select * from temp1) as t1 ' +
		'inner join ' +
		'(select * from temp2) as t2 ' +
		'on t1.display_id = t2.display_id'

	//var queryState = 'select count(*) as count from '+ state +'_events'
	var queryState = 'select count(*) as count from temp2'
	//var queryClicks = 'select count(*) as count from clicked_ads where clicked_ad_id = '+ adId
	var queryClicks = 'select count(*) as count from temp1'
	var queryCtr = 'select ctr as count from click_rate where ad_id = '+ adId

	///////////////////////
	/// Remove query since this number doesn't change and we get better performanace
	// var queryUS = 'select count(*) as count from US_events'
	// var functionUS = function(callback3){
    //	performQueryCount(queryUS, 'total_us', callback3)
	// }
	////////////////////////

	var functionGivenClicked = function(callback1){
    	performQueryCount(queryStateGivenClickedAd, 'clicks_per_state', callback1)
	}

	var functionState = function(callback2){
    	performQueryCount(queryState, 'total_state', callback2)
	}

	var functionClicks = function(callback4){
    	performQueryCount(queryClicks, 'total_clicks', callback4)
	}

	var functionCtr = function(callback5){
    	performQueryCount(queryCtr, 'ctr', callback5)
	}

	// var query_arr = [
 //    	functionGivenClicked, functionState, functionUS, functionClicks, functionCtr
	// ]

	var query_arr = [
    	functionGivenClicked, functionState, functionClicks, functionCtr
	]

 	async.parallel(query_arr, function (err, result) {
 		var temp = {}
 		for (i in result){
 			keys = Object.keys(result[i])
 			for (key in keys){
 				temp[keys[key]] = result[i][keys[key]]
 			}
 		}
 		var data = {'ad_id': adId, 'state': state, 'total_clicks': temp.total_clicks, 'clicks_per_state':temp.clicks_per_state, 
 				   'total_us':18595452, 'total_state': temp.total_state, 'ctr':temp.ctr}
		var queryInsert = 'INSERT into counts_per_ad_id_and_state '+
			'(ad_id,state,total_clicks,clicks_per_state,total_us,total_state,ctr)' +
			'values '+
			'(' + adId +',"'+state+'",'+temp.total_clicks+','+temp.clicks_per_state+',18595452,'+temp.total_state+','+temp.ctr+')'

 		dbconnection.query(queryInsert, function(err, rows, fields) {
 			var deleteTable1 = 'drop table temp1'
 			dbconnection.query(deleteTable1, function(err1, rows1, fields1) {
 				var deleteTable2 = 'drop table temp2'
 				dbconnection.query(deleteTable2, function(err1, rows1, fields1) {
 					callback(err,data)
 				});
 			});
 		});
 	});

}

//exports.selectQueryByState = selectQueryByState

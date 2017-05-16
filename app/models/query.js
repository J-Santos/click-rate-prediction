var async             =   require("async");
var dbconnection      =   require('../dbconnection.js');
var stateVariables    =   require("../state_variables");
var ejs				  =	  require('ejs');


var kmean = require('kmeans');

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
 	var queryCheckState = 'select * from history where clicked_ad_id = '+adId +' and state_or_country = "' +state +'"'

 	dbconnection.query(queryCheckState, function(err, rows, fields) {
 		if(rows.length == 0){
 			console.log("This entry doesn't exist. Processing all queries.")

			var createInnerJoinTemp1 = 'create table temp1 as (select t1.clicked_ad_id, t2.state, t2.display_id, t2.hour, t2.minute from ' + 
				'(select display_id, clicked_ad_id from clicked_ads where clicked_ad_id = '+adId+') as t1 ' +
				'inner join ' +
				'(select state, display_id, hour, minute from '+ state +'_events) as t2 ' +
				'on t1.display_id = t2.display_id)'
			console.log("Temp1 Query: "+ createInnerJoinTemp1);
			dbconnection.query(createInnerJoinTemp1, function(err1, rows1, fields1) {
				var createTemp2 = 'create table temp2 as (select t3.clicked_ad_id, t4.state, t4.display_id, t4.hour, t4.minute from '+
					'(select display_id, clicked_ad_id from clicked_ads where clicked_ad_id = '+adId+') as t3 '+
					'inner join '+
					'(select country as state, display_id, hour, minute from US_events) as t4 '+
					'on t3.display_id = t4.display_id)'
				console.log("Finished creating temp1.")

				var queryCheckUS = 'select * from history where clicked_ad_id = '+adId +' and state_or_country = "US" limit 5'
				dbconnection.query(queryCheckUS, function(errUSCheck, rowsUSCheck, fieldsUSCheck) {
					console.log("Finished querying history.")
					if(rows.length == 0){
						console.log("Temp2 Query: "+ createTemp2);
						dbconnection.query(createTemp2, function(err2, rows2, fields2) {
							console.log("Finished creating temp2.")
							processNewQueries(adId, state, callback, false)
						});
					}else{
						processNewQueries(adId, state, callback, true)
					}
				});
			});
 		}else{
 			console.log("This entry already exist. Returning results.")
 			var queryCheckUS = 'select * from history where clicked_ad_id = '+adId +' and state_or_country = "US"'
 			dbconnection.query(queryCheckUS, function(errUS, rowsUS, fieldsUS) {
 				var queryCTR = 'select ctr from click_rate where ad_id = '+ adId
				dbconnection.query(queryCTR, function(errCTR, rowsCTR, fieldsCTR) {
	 				var parser = JSON.parse(JSON.stringify(rowsCTR))
	 				var clusters_us = createClusters(processRowsForClusters(rowsUS), 2)
					var clusters_state = createClusters(processRowsForClusters(rows), 2)
			 		var data_res = {'ad_id': adId, 'state': state, 'state_cluster': clusters_state, 'us_cluster':clusters_us, 'ctr':parser[0].ctr}
					callback(err, data_res)
 				});

 			});
 		}

 	});
}

function performQueryCount(query, name, callback){
	dbconnection.query(query, function(err, rows, fields) {
		if(err){
			console.log(err);
		}else{
			//console.log("QUERY: "+ query)
			var parser = JSON.parse(JSON.stringify(rows))
			var data = {}
			if(name == 'ctr'){
				var count = parser[0].count
				data = {'ctr': count}
			}else if(name == 'state_cluster'){
				arr = processRowsForClusters(rows)
				data = {'state_cluster': arr}
			}else if(name == 'us_cluster'){
				arr = processRowsForClusters(rows)
				data = {'us_cluster': arr}
			}
			console.log("Finished performing query: " + query)
	        callback(err, data)
	    }
    });
}

function processRowsForClusters(rows){
	var array_2D = [];
	for (var i in rows) {
		array_2D.push([rows[i].hour, rows[i].minute])
	}
	return array_2D;
}

function createClusters(input, numOfClusters){
	var km = kmean.create(input, numOfClusters);
	return km.process();
}

function processNewQueries(adId, state, callback_res, us_exist){
	var queryCtr = 'select ctr as count from click_rate where ad_id = '+ adId
	var queryStateCluster = 'select hour, minute from temp1'


	var queryUSCluster;
	var queryUSNew = 'select hour, minute from temp2'
	var queryUSExist = 'select * from history where clicked_ad_id = '+adId +' and state_or_country = "US"'

	if(us_exist){
		queryUSCluster = queryUSExist;
	}else{
		queryUSCluster = queryUSNew
	}

	////Define function to run async
	var functionCtr = function(callback5){
    	performQueryCount(queryCtr, 'ctr', callback5)
	}

	var functionStateCluster = function(callback6){
    	performQueryCount(queryStateCluster, 'state_cluster', callback6)
	}
	var functionUSCluster = function(callback7){
    	performQueryCount(queryUSCluster, 'us_cluster', callback7)
	}

	var query_arr = [
    	functionCtr, functionStateCluster, functionUSCluster
	]

 	async.parallel(query_arr, function (err, result) {
 		var temp = {}
 		for (i in result){
 			keys = Object.keys(result[i])
 			for (key in keys){
 				temp[keys[key]] = result[i][keys[key]]
 			}
 		}
 		var data = {'ad_id': adId, 'state': state, 'state_cluster': temp.state_cluster, 'us_cluster':temp.us_cluster, 
 				   'ctr':temp.ctr}

		var queryInsertTemp1 = 'insert into history select * from temp1'
		var queryInsertTemp2 = 'insert into history select * from temp2'

		if(us_exist){
			dbconnection.query(queryInsertTemp1, function(err1, rows1, fields1) {
				console.log("Finished inserting data in history table.")
				var deleteTables = 'drop table temp1'
				dbconnection.query(deleteTables, function(err1, rows1, fields1) {
					console.log("Finished deleting temp tables.")
					console.log("US CLUSTER LENGTH: "+temp.us_cluster.length)
					console.log("STATE CLUSTER LENGTH: "+temp.state_cluster.length)
					var clusters_us = createClusters(temp.us_cluster, 2)

					// if(temp.state_cluster.length < 2){

					// }

					var clusters_state = createClusters(temp.state_cluster, 2)
			 		var data_res = {'ad_id': adId, 'state': state, 'state_cluster': clusters_state, 'us_cluster':clusters_us, 'ctr':temp.ctr}
					callback_res(err, data_res)
				});
			});
		}else{

			dbconnection.query(queryInsertTemp1, function(err1, rows1, fields1) {
				dbconnection.query(queryInsertTemp2, function(err2, rows2, fields2) {
					console.log("Finished inserting data in history table.")
					var deleteTables = 'drop table temp1, temp2'
					dbconnection.query(deleteTables, function(err1, rows1, fields1) {
						console.log("Finished deleting temp tables.")
						console.log("US CLUSTER LENGTH: "+temp.us_cluster.length)
						console.log("STATE CLUSTER LENGTH: "+temp.state_cluster.length)
						var clusters_us = createClusters(temp.us_cluster, 2)
						var clusters_state = createClusters(temp.state_cluster, 2)
				 		var data_res = {'ad_id': adId, 'state': state, 'state_cluster': clusters_state, 'us_cluster':clusters_us, 'ctr':temp.ctr}
						callback_res(err, data_res)
					});
				});

			});
		}
 	});
}

exports.file_4 = function (req, callback){
	console.log(req.params)
   	var country="US";
   	var state=req.params.state
   	var dma = req.params.dma
   	//var dma="807";
   	var date1=req.params.start_date;
   	var date2=req.params.end_date;
   	var start_time=req.params.start_time
   	var end_time=req.params.end_time

   	/*query1--- to list total number of users in the given range in each platform*/

  	var query="select count(uuid) as total_count,platform from "+state+"_events where dma='"+dma+"' and day between '"+date1+"' and '"+date2+"' and hour between + "+
       "'"+start_time+"' and '"+end_time+"' GROUP BY platform";


    /*query1-- to list number of users in per platform per hour*/


   	var query1="select count(uuid) as total_count,platform,hour from "+state+"_events where dma='"+dma+"' and day between '"+date1+"' and '"+date2+"' and hour between + "+
           "'"+start_time+"' and '"+end_time+"' GROUP BY hour, platform";


  	/*
   	var query2="select count("+state+"_events.uuid) as total_count,platform,traffic_source from "+state+"_events,page_views where dma='"+dma+"' and day between '"+date1+"' and '"+date2+"' and hour between + "+
       "'"+start_time+"' and '"+end_time+"' and "+state+"_events.document_id=page_views.document_id GROUP BY platform, traffic_source";
    */

    //console.log(query);
   	dbconnection.query(query,function (err,result) {
	    if(err){
	        //console.log("some error");
	        console.log(err);
	    }else{
	        //console.log("result");
	        //console.log(result);
			/*
			var query1="select events.count(uuid) as total_count,traffic_source as source, platform from events,page_views where events.country='"+country+"' and events.state='"+state+"' and events.dma='"+dma+"' and events.day between '"+date1+"' and '"+date2+"' and events.hour between + "+
			  "'"+start_time+"' and '"+end_time+"' and events.document_id=page_views.document_id GROUP BY source";
			*/
	        console.log(query1);


		    dbconnection.query(query1,function (err,result1) {
		    	if(err){
		        	console.log(err);
		        }else{
		            //console.log("result1");
					var final_result={
		                result1:result,
		                result2:result1
		          	};
		            //console.log(final_result);
		            callback(err, final_result)
		        }
		     });
	    }
   	});
}



var async             =   require("async");
var dbconnection      =   require('../dbconnection.js');
var stateVariables    =   require("../state_variables");
var ejs				  =	  require('ejs');


var kmeans = require('node-kmeans');
var kmean = require('kmeans');

dbconnection.connect(function(err){
 if(!err) {
     console.log("Database is connected ...");  
 } else {
     console.log("Error connecting database ... \n\n");  
 }
 });

function testing(){
	var data = [
		  {'company': 'Microsoft' , 'size': 91259, 'revenue': 60420},
		  {'company': 'IBM' , 'size': 400000, 'revenue': 98787},
		  {'company': 'Skype' , 'size': 700, 'revenue': 716},
		  {'company': 'SAP' , 'size': 48000, 'revenue': 11567},
		  {'company': 'Yahoo!' , 'size': 14000 , 'revenue': 6426 },
		  {'company': 'eBay' , 'size': 15000, 'revenue': 8700},
		];

	let vectors = new Array();
	for (let i = 0 ; i < data.length ; i++) {
	  vectors[i] = [ data[i]['size'] , data[i]['revenue']];
	}
 	//console.log(vectors)
	// var kmeans = require('node-kmeans');
	kmeans.clusterize(vectors, {k: 2}, (err,res) => {
	  if (err) console.error(err);
	  else {
	  	//console.log('%o',res[0]);}
	  	console.log('clusterize');
	  }
	});
	console.log('HEEERERERERFERERE')
	//var k = require('kmeans');
	var km = kmean.create([
	  [1, 2],
	  [5, 4],
	  [2, 5],
	  [8, 4]
	], 2);

	var result = km.process();
	console.log(result);

	//console.log(result.means);
	//console.log(result.clusters);
}

exports.dbQuery = function (req, callback){
	//testing()
 	var state = req.params.state
 	var adId = req.params.ad_id

 	///check if this was processed already, ad_id-state combination
 	var queryCheckState = 'select * from history where clicked_ad_id = '+adId +' and state_or_country = "' +state +'"'

 	dbconnection.query(queryCheckState, function(err, rows, fields) {
 		if(rows.length == 0){
 			console.log("This entry doesn't exist. Processing all queries.")

 			//var insertTemp2 = 'create table temp2 as (select display_id, hour, minute from '+ state +'_events)'
			//console.log("Finished creating temp1.")
			var createInnerJoinTemp1 = 'create table temp1 as (select t1.clicked_ad_id, t2.state, t2.display_id, t2.hour, t2.minute from ' + 
				'(select display_id, clicked_ad_id from clicked_ads where clicked_ad_id = '+adId+') as t1 ' +
				'inner join ' +
				'(select state, display_id, hour, minute from '+ state +'_events) as t2 ' +
				'on t1.display_id = t2.display_id)'
			dbconnection.query(createInnerJoinTemp1, function(err1, rows1, fields1) {
				var createTemp2 = 'create table temp2 as (select t3.clicked_ad_id, t4.state, t4.display_id, t4.hour, t4.minute from '+
					'(select display_id, clicked_ad_id from clicked_ads where clicked_ad_id = '+adId+') as t3 '+
					'inner join '+
					'(select country as state, display_id, hour, minute from US_events) as t4 '+
					'on t3.display_id = t4.display_id)'

				console.log("Finished creating temp1.")
				dbconnection.query(createTemp2, function(err2, rows2, fields2) {
					console.log("Finished creating temp2.")
					processNewQueries(adId, state, callback)
				});
			});
 		}else{
 			console.log("This entry already exist. Returning results.")
 			var queryCheckUS = 'select * from history where clicked_ad_id = '+adId +' and state_or_country = "US"'
 			//console.log(rows)
 			//var clusters_state = processRowsForClusters(rows)
 			dbconnection.query(queryCheckUS, function(errUS, rowsUS, fieldsUS) {
 				//var clusters_us = processRowsForClusters(rowsUS)
 				//console.log("US")
 				//console.log(rowsUS)
 				var queryCTR = 'select ctr from click_rate where ad_id = '+ adId
				dbconnection.query(queryCTR, function(errCTR, rowsCTR, fieldsCTR) {
	 				//var clusters_us = processRowsForClusters(rowsUS)
	 				//console.log("US")
	 				//console.log(rowsUS)
	 				var parser = JSON.parse(JSON.stringify(rowsCTR))
	 				var clusters_us = createClusters(processRowsForClusters(rowsUS), 2)
					var clusters_state = createClusters(processRowsForClusters(rows), 2)
			 		var data_res = {'ad_id': adId, 'state': state, 'state_cluster': clusters_state, 'us_cluster':clusters_us, 'ctr':parser[0].ctr}
					callback(err, data_res)
 				});

 			});
 			//callback(err, rows[0])
 		}

 	});

 	// dbconnection.query(queryCheck, function(err, rows, fields) {
 	// 	if(rows.length == 0){
 	// 		console.log("This entry doesn't exist. Processing all queries.")
 	// 		var insertTemp1 = 'create table temp1 as (select display_id from clicked_ads where clicked_ad_id = '+adId+')'
 	// 		dbconnection.query(insertTemp1, function(err1, rows1, fields1) {
 	// 			var insertTemp2 = 'create table temp2 as (select display_id, hour, minute from '+ state +'_events)'
 	// 			console.log("Finished creating temp1.")
 	// 			dbconnection.query(insertTemp2, function(err2, rows2, fields2) {
 	// 				//'(select * from temp2) as t2 ' +
 	// 				var createInnerJoinTemp3 = 'create table temp3 as (select t2.display_id, t2.hour, t2.minute from ' + 
		// 				'(select * from temp1) as t1 ' +
		// 				'inner join ' +
		// 				'(select display_id, hour, minute from '+ state +'_events) as t2 ' +
		// 				'on t1.display_id = t2.display_id)'
		// 			console.log("Finished creating temp2.")
		// 			dbconnection.query(createInnerJoinTemp3, function(err3, rows3, fields3) {
		// 				var createTemp4 = 'create table temp4 as (select t4.display_id, t4.hour, t4.minute from '+
		// 					'(select * from temp1) as t1 ' +
		// 					'inner join '+
		// 					'(select display_id, hour, minute from US_events) as t4 '+
		// 					'on t1.display_id = t4.display_id)'
		// 				console.log("Finished creating temp3.")
 	// 					dbconnection.query(createTemp4, function(err4, rows4, fields4) {
 	// 						console.log("Finished creating temp4.")
 	// 						processNewQueries(adId, state, callback)
 	// 					});
 	// 				});
 	// 			});
 	// 		});
 	// 	}else{
 	// 		console.log("This entry already exist. Returning results.")
 	// 		callback(err, rows[0])
 	// 	}

 	// });
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
			//console.log("ROWS: "+ rows[0])
			var parser = JSON.parse(JSON.stringify(rows))
			//var key = "'" + name +"'"
			//console.log(name)
			//var count = -1//parser[0].count
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
				var count = parser[0].count
				data = {'ctr': count}
			}else if(name == 'state_cluster'){
				arr = processRowsForClusters(rows)
				data = {'state_cluster': arr}
				// var array_2D = [];
				// for (var i in rows) {
    //     			console.log('HOUR: ', rows[i].hour);
    //     			console.log('MINUTE: ', rows[i].minute);
    //     			array_2D.push([rows[i].hour, rows[i].minute])
   	//  			}
   	//  			data = {'state_cluster': array_2D}
			}else if(name == 'us_cluster'){
				arr = processRowsForClusters(rows)
				data = {'us_cluster': arr}
				// var array_2D = [];
				// for (var i in rows) {
    //     			console.log('HOUR: ', rows[i].hour);
    //     			console.log('MINUTE: ', rows[i].minute);
    //     			array_2D.push([rows[i].hour, rows[i].minute])
   	//  			}
   	//  			data = {'us_cluster': array_2D}
			}
			console.log("Finished performing query: " + query)
	        callback(err, data)
	    }
    });

}

function processRowsForClusters(rows){
	var array_2D = [];
	for (var i in rows) {
		//console.log('HOUR: ', rows[i].hour);
		//console.log('MINUTE: ', rows[i].minute);
		array_2D.push([rows[i].hour, rows[i].minute])
	}
	return array_2D;
}

function createClusters(input, numOfClusters){
	var km = kmean.create(input, numOfClusters);
	return km.process();
}

function processNewQueries(adId, state, callback_res){
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

	// var queryStateGivenClickedAd = 'create table temp3 as (select * from ' + 
	// 	'(select * from temp1) as t1 ' +
	// 	'inner join ' +
	// 	'(select * from temp2) as t2 ' +
	// 	'on t1.display_id = t2.display_id)'

	// var queryStateGivenClickedAd = 'select count(*) as count from ' + 
	// 	'(select * from temp1) as t1 ' +
	// 	'inner join ' +
	// 	'(select * from temp2) as t2 ' +
	// 	'on t1.display_id = t2.display_id'

	///////var queryStateGivenClickedAd = 'select count(*) as count from temp3'
	//var queryState = 'select count(*) as count from '+ state +'_events'
	////////var queryState = 'select count(*) as count from temp2'
	//var queryClicks = 'select count(*) as count from clicked_ads where clicked_ad_id = '+ adId
	/////////var queryClicks = 'select count(*) as count from temp1'




	var queryCtr = 'select ctr as count from click_rate where ad_id = '+ adId
	var queryStateCluster = 'select hour, minute from temp1'
	var queryUSCluster = 'select hour, minute from temp2'

	///////////////////////
	/// Remove query since this number doesn't change and we get better performanace
	// var queryUS = 'select count(*) as count from US_events'
	// var functionUS = function(callback3){
    //	performQueryCount(queryUS, 'total_us', callback3)
	// }
	////////////////////////

	// var functionGivenClicked = function(callback1){
 //    	performQueryCount(queryStateGivenClickedAd, 'clicks_per_state', callback1)
	// }

	// var functionState = function(callback2){
 //    	performQueryCount(queryState, 'total_state', callback2)
	// }

	// var functionClicks = function(callback4){
 //    	performQueryCount(queryClicks, 'total_clicks', callback4)
	// }



	var functionCtr = function(callback5){
    	performQueryCount(queryCtr, 'ctr', callback5)
	}

	var functionStateCluster = function(callback6){
    	performQueryCount(queryStateCluster, 'state_cluster', callback6)
	}
	var functionUSCluster = function(callback7){
    	performQueryCount(queryUSCluster, 'us_cluster', callback7)
	}
	// var query_arr = [
 //    	functionGivenClicked, functionState, functionUS, functionClicks, functionCtr
	// ]

	// var query_arr = [
 //    	functionGivenClicked, functionState, functionClicks, functionCtr, functionStateCluster, functionUSCluster
	// ]
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
 		//callback(err,data)

		var queryInsert = 'INSERT into counts_per_ad_id_and_state '+
			'(ad_id,state,state_cluster,us_cluster,ctr) ' +
			'values '+
			'(' + adId +',"'+state+'",'+temp.state_cluster+','+temp.us_cluster+','+temp.ctr+')'

		var queryInsertTemp1 = 'insert into history select * from temp1'
		var queryInsertTemp2 = 'insert into history select * from temp2'

 		//console.log("Finished deleting temp tables.")
		//callback_res(err,data)
		//var clusterObject;
		//var km_us = kmean.create(temp.us_cluster, 2);

		///Insert process data into history table for future requests
		///Then, delete the temp tables
		dbconnection.query(queryInsertTemp1, function(err1, rows1, fields1) {
			dbconnection.query(queryInsertTemp2, function(err2, rows2, fields2) {
				// var data1 = {'ad_id': adId, 'state': state, 'state_cluster': temp.state_cluster, 'us_cluster':temp.us_cluster, 
				//   			'ctr':temp.ctr, 'stCluster': clusters_state, 'usCluster': clusters_us}
				console.log("Finished inserting data in history table.")
				var deleteTables = 'drop table temp1, temp2'
				dbconnection.query(deleteTables, function(err1, rows1, fields1) {
					console.log("Finished deleting temp tables.")

					var clusters_us = createClusters(temp.us_cluster, 2)
					var clusters_state = createClusters(temp.state_cluster, 2)
			 		var data_res = {'ad_id': adId, 'state': state, 'state_cluster': clusters_state, 'us_cluster':clusters_us, 'ctr':temp.ctr}
					callback_res(err, data_res)
				});
			});

		});

// var clusters_us = createClusters(temp.us_cluster, 2)

// var clusters_state = createClusters(temp.state_cluster, 2)

// // var data1 = {'ad_id': adId, 'state': state, 'state_cluster': temp.state_cluster, 'us_cluster':temp.us_cluster, 
// //   			'ctr':temp.ctr, 'stCluster': clusters_state, 'usCluster': clusters_us}

//  var data1 = {'ad_id': adId, 'state': state, 'state_cluster': clusters_state, 'us_cluster':clusters_us, 'ctr':temp.ctr}

// callback_res(err, data1)

 		// 	var deleteTables = 'drop table temp1, temp2'
		// console.log(data)
		// dbconnection.query(deleteTables, function(err1, rows1, fields1) {
		// 	console.log("Finished deleting temp tables.")
		// 	//callback_res(err,data)
		// 	//var clusterObject;
		// 	var km_us = kmean.create(temp.us_cluster, 2);

		// 	var clusters_us = km_us.process();
		// 	//console.log(result);

		// 	var km_state = kmean.create(temp.state_cluster, 2);
		// 	var clusters_state = km_state.process();

		// 	var data1 = {'ad_id': adId, 'state': state, 'state_cluster': temp.state_cluster, 'us_cluster':temp.us_cluster, 
		// 	  			'ctr':temp.ctr, 'cluster': clusterObject, 'stCluster': clusters_state, 'usCluster': clusters_us}

		// 	callback_res(err, data1)
	 		
		// });
 	});

}



exports.file_4 = function (req, callback){
	console.log(req.params)
   	var country="US";
   	var state=req.params.state
   	var dma="807";
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

    console.log(query);
   dbconnection.query(query,function (err,result) {
      if(err)
      {
          console.log("some error");
          console.log(err);
      }
      else
      {
          console.log("result");
          console.log(result);



          /*
          var query1="select events.count(uuid) as total_count,traffic_source as source, platform from events,page_views where events.country='"+country+"' and events.state='"+state+"' and events.dma='"+dma+"' and events.day between '"+date1+"' and '"+date2+"' and events.hour between + "+
              "'"+start_time+"' and '"+end_time+"' and events.document_id=page_views.document_id GROUP BY source";

            */
          console.log(query1);


          dbconnection.query(query1,function (err,result1) {

              if(err)
              {
                  console.log("some error 2");
                  console.log(err);
              }
              else
              {
                  console.log("result1");
                  console.log(result1);

                  var final_result={
                      result1:result,
                      result2:result1
              };
                  console.log(final_result);
                    // res.send(final_result);
                    callback(err, final_result)
              }
          })

      }
   });
 	
}



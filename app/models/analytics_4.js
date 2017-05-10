/**
 * Created by Lenovo on 5/5/2017.
 */
//var mysql= require('mysql');
var ejs=require('ejs');
var dbconnection      =   require('../dbconnection.js');

exports.file_4=function(req,res)
{

    console.log("inside run");

    // var connection = mysql.createConnection({
    //     host     : 'jesdbinstance.cimw8kngg4r8.us-west-2.rds.amazonaws.com',
    //     user     : 'dbuser',
    //     password : '239dbuser',
    //     port : 3306,
    //     database : 'cmpe239projectdb'
    // });

   dbconnection.connect(function (err) {
      if(err)
      {
          console.log("error while connecting to db");
          console.log(err);
      }
      else
        console.log("connnected successfully");
   });

/*
   console.log(req.param("state"));
   console.log(req.param("start_date"));
   console.log(req.param("end_date"));
   console.log(req.param("start_time"));
   console.log(req.param("end_time"));
*/

   var temp=req.param("start_date");
   var date1=temp.split("-");
  // console.log(date1[2]);
   var date2=date1[2].split("T");
   date1=date2[0];
  //  console.log(date1);

    var temp=req.param("end_date");
    var date2=temp.split("-");
  //  console.log(date2[2]);
    var temp=date2[2].split("T");
    date2=temp[0];
 //   console.log(date2);

   var country="US";
   var state=req.param("state");
   var dma="807";
   var date1=date1;
   var date2=date2;
   var start_time=req.param("start_time");
   var end_time=req.param("end_time");


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
                    res.send(final_result);
              }
          })




      }
   });
}


/****

********SunEdison*********

 : Anurag Bhardwaj

Version                     Author                      Change
1.0                     Anurag Bhardwaj             Initial Version


****/

var pjson = require('./package.json');
var region = process.env['AWS_REGION'];

if (!region || region === null || region === "") {
    region = "us-west-2";
    console.log("AWS Lambda Redshift Database Loader using default region " + region);
}

//Requiring aws-sdk. 
var aws = require('aws-sdk');
aws.config.update({
    region : region
});

//Requiring S3 module. 
var s3 = new aws.S3({
    apiVersion : '2006-03-01',
    region : region
});
//Requiring dynamoDB module. 
var dynamoDB = new aws.DynamoDB({
    apiVersion : '2012-08-10',
    region : region
});

//Requiring SNS module. 
var sns = new aws.SNS({
    apiVersion : '2010-03-31',
    region : region
});

//Importing exteral file constants. 
require('./constants');

//Importing kmsCrypto. 
var kmsCrypto = require('./kmsCrypto');
kmsCrypto.setRegion(region);

var common = require('./common');
var async = require('async');
var uuid = require('node-uuid');

//Importing postgre. 
var pg = require('pg');

//Importing https. 
var http = require('https');

var upgrade = require('./upgrades');
var zlib = require('zlib');

//Importing querystring to get more DB calls into the script.
var querystring = require('querystring');
var parseString = require('xml2js').parseString;

var responseString = '';
var test;


var conString = "postgresql://XXXX:XXXX@sunedisondatawarehouse.cgnr3c8sn1sz.us-west-2.redshift.amazonaws.com:5439/sunedison";

var queryCreateCPMSkeleton = 'CREATE TABLE cpm(internal_id varchar(255),site_id varchar(255),homeowner varchar(255),ho_email varchar(255),ho_phone varchar(255),sales_engine varchar(255),sales_agent_name varchar(255),cpm varchar(255),sai varchar(255),sai_territory varchar(255),purchase_type varchar(255),contract_status varchar(255),fully_executed_date date,credit_check_status varchar(255),cc_date date,proposed_system_size_kw varchar(255),site_survey_status varchar(255),site_survey_scheduled_date date,site_survey_completed_date date,site_survey_date_change_reason varchar(255),pda_status varchar(255),pda_last_update_date date,permit_status varchar(255),permit_submit_date date,target_permit_date date,permit_receive_date date,sales_order varchar(255),equipment_status varchar(255),equipment_status_date varchar(255),installation_status varchar(255),target_install_date date,actual_install_date date,main_panel_upgrade_required varchar(255),commissioned_date varchar(255),ahj_inspection_status varchar(255),ahj_inspection_date date,mp2_status varchar(255),sai_mp2_approval_date varchar(255),permission_to_apply_for_pto varchar(255),utility_interconnection_status varchar(255),interconnection_sign_off_date varchar(255),pto_status varchar(255),pto_date varchar(255),mp3_status varchar(255),final_lien_waiver_6_3b varchar(255),days_since_delivery varchar(255));';
var queryMakeCPM = "insert into cpm select site.site_id, site.site_id, site.homeowner, customer.email as ho_email, customer.homephone as ho_phone, customer.assigned_partner_id as sales_engine,case when customer.assigned_partner_id like 'Evolve Solar' then customer.sales_agent else customer.assigned_to_partner_sales_agent end as sales_agent_name,site.cpm_list as cpm,site.sai_installer as sai,site.sai_territory_id,(customer.purchase_type_id || '(' || customer.financing_program || ')') as purchase_type,customer.homeowner_lease_contract_st_id as contract_status,customer.fully_executed_date,customer.homeowner_credit_check_status as credit_check_status,customer.credit_check_last_update_date as cc_date,customer.total_system_size_echo_watts as proposed_system_size_kw,site.site_visit_status_id as site_survey_status,site.site_survey_scheduled_date,site.site_visit_date as site_survey_completed_date,site.site_survey_date_change_reason,customer.homeowner_proj_document_sta_id as pda_status,customer.proj_definition_doc_last_upda as pda_last_update_date,site.permit_status,site.permit_submit_date,site.target_permit_date,site.actual_permit_date as permit_recieve_date,case when len(site.nr_interconnected_so) > 1 then sales_order.number else (case when len(site.salesorder_to_homeowner_id) > 1 then sales_order.number end) end as sales_order,CASE WHEN (CASE WHEN LEN(site.nr_interconnected_so)>1 THEN sales_order.actual_delivery_date ELSE (CASE WHEN len(site.salesorder_to_homeowner_id)>1 THEN sales_order.actual_delivery_date END)END) IS NOT NULL THEN 'Delivered' ELSE (CASE WHEN (CASE WHEN len(site.nr_interconnected_so)>1 THEN site.nr_interconnected_so ELSE (CASE WHEN len(site.salesorder_to_homeowner_id)>1 THEN site.salesorder_to_homeowner_id END)END) IS NOT NULL THEN 'Ordered' ELSE NULL END) END as equipment_status,CASE WHEN (CASE WHEN len(site.nr_interconnected_so)>1 THEN sales_order.actual_delivery_date ELSE (CASE WHEN len(site.nr_interconnected_so)>1 THEN sales_order.actual_delivery_date END)END) IS NOT NULL THEN (CASE WHEN len(site.nr_interconnected_so)>1 THEN sales_order.actual_delivery_date ELSE (CASE WHEN len(site.salesorder_to_homeowner_id)>1 THEN sales_order.actual_delivery_date END)END) ELSE (CASE WHEN (CASE WHEN len(site.nr_interconnected_so)>1 THEN site.nr_interconnected_so ELSE   (CASE WHEN LENGTH(site.salesorder_to_homeowner_id)>1 THEN site.salesorder_to_homeowner_id END)END) IS NOT NULL THEN (CASE WHEN LENGTH(site.nr_interconnected_so)>1 THEN sales_order.current_promise_date ELSE (CASE WHEN LENGTH(site.salesorder_to_homeowner_id)>1 THEN sales_order.current_promise_date END)END) ELSE NULL END) END as equipment_status_date,site.install_status as installation_status,site.target_install_date,site.install_completed_date as actual_install_date,customer.service_panel_upgrade as main_panel_upgrade_required,site.commissioned_date,site.ahj_inspection_status,site.ahj_inspection_approval_date as ahj_inspection_date,nvl(site.installer2_payment_status,site.ms2_payment_status) as mp2_status,NVL(site.milestone_2_payment_approval_,site.installer2_payment_approval_d) as sai_mp2_approval_date,case when site.tranche_id not like '' then 'Yes' else (case when customer.financing_program like 'PPA 1.0' then 'No' else 'Yes' end)end as permission_to_apply_for_pto,site.doc_processing_status13 as utility_interconnection_status,site.submittal_date13 as interconnection_sign_off_date,site.doc_processing_status14 as pto_status,site.permission_to_operate_date as pto_date,nvl(site.installer3_payment_status,site.ms3_payment_status) as mp3_status,site.lien_doc_processing_status as final_lien_waiver_6_3b,CASE WHEN site.se2_payment_approval_date IS NULL THEN trunc(convert_timezone('US/Pacific',getdate())) - (CASE WHEN len(site.nr_interconnected_so)>1 THEN sales_order.actual_delivery_date ELSE (CASE WHEN LENGTH(site.salesorder_to_homeowner_id)>1 THEN sales_order.actual_delivery_date END)END) END as days_since_delivery FROM site left join customer on site.homeowner_id = customer.record left join sales_order on NVL((CASE when len(site.nr_interconnected_so) != 0 and len(site.nr_interconnected_so) !=1 then substring(site.nr_interconnected_so, 8, (LEN(site.nr_interconnected_so)-7)) end),(CASE when len(site.salesorder_to_homeowner_id) != 0 and len(site.salesorder_to_homeowner_id) !=1 then substring(site.salesorder_to_homeowner_id, 8, (LEN(site.salesorder_to_homeowner_id)-7)) end)) = sales_order.number where CASE WHEN((homeowner_lease_contract_st_id='Credit Check Completed') OR (homeowner_lease_contract_st_id='Signed by HO, Pending SE Review') OR (homeowner_lease_contract_st_id='Signed by HO, Awaiting CC Data') OR(homeowner_lease_contract_st_id='Signed by HO, Dealer Action Required') OR(homeowner_lease_contract_st_id='Signed by HO, Pending SE Signature') OR(homeowner_lease_contract_st_id='Fully Executed') OR(homeowner_lease_contract_st_id='Contract Cancelled') OR(homeowner_lease_contract_st_id='Sale Hold') OR(homeowner_lease_contract_st_id='Contract Re-sign Required') OR(homeowner_lease_contract_st_id='Sent Trust Doc To Ho')) THEN 1 ELSE 0 END = 1 and site.date_created >= '2012-01-01' and CASE WHEN customer.partner_sub_type_id='SALES ENGINE (Seller)' THEN 1 ELSE 0 END = 1 and (customer.firstname not like 'TL%') and (customer.lastname not like 'TL%') and customer.is_homeowner like 'Yes' and customer.subsidiary_no_hierarchy not like 'EFQ' and site.homeowner not in ('Mavani, Saurabh', 'Mendoza, Consuelo', 'Murphy and Halter, Patty and Robert', 'Santos, Tammy', 'Shah, Dushyant', 'Shah, Kalpesh', 'Suri, Deepak', 'Tailor, Harish', 'Walkin, Gitty', 'Sutton, Sora', 'Schachter, Samuel', 'Hill, Elizabeth', 'MacRobert, Alan', 'Umang, Dave', 'Wayne, Martin', 'Mareno, John', 'Larocca, Thomas', 'McHenry, Andrea', 'Bhargava, Marut') and case when site.site_id = customer.site_id then 1 else 0 end = 1 and customer.lead_lost like 'No' and ((site.salesorder_to_homeowner_id not like '' and site.nr_interconnected_so like '') or (site.salesorder_to_homeowner_id like '' and site.nr_interconnected_so not like '') or (site.salesorder_to_homeowner_id not like '' and site.nr_interconnected_so not like '') or (site.salesorder_to_homeowner_id like '' and site.nr_interconnected_so like ''));";
var queryDropCPM = 'DROP TABLE IF EXISTS cpm;';

exports.handler = function(event, context) {
	dropCPM();
}

var dropCPM = function(){
	pg.connect(conString, function(err,client){
	    if(err){
	        return console.log("Connection error. ", err);
	    }
	
	    console.log("Connection Established.");
	
	    //Querying redshift. 
	    client.query(queryDropCPM, function(err,result){
	        if(err){
	            console.log("Error returning query: " + err);
	            client.end();
	            context.done("Fatal Error");
	        }
	       	console.log("Table Dropped.");
	       	createCPMSkeleton();
	    });
	    
	});
}

var createCPMSkeleton = function(){
	pg.connect(conString, function(err,client){
	    if(err){
	        return console.log("Connection error. ", err);
	    }
	
	    console.log("Connection Established.");
	
	    //Querying redshift. 
	    client.query(queryCreateCPMSkeleton, function(err,result){
	        if(err){
	            console.log("Error returning query: " + err);
	            client.end();
	            context.done("Fatal Error");
	        }
	       	console.log("Skeleton Table Created.");
	        makeCPMForTableau();
	    });
	});
}

var makeCPMForTableau = function(){
	pg.connect(conString, function(err,client){
	    if(err){
	        return console.log("Connection error. ", err);
	    }
	
	    console.log("Connection Established.");
	
	    //Querying redshift. 
	    client.query(queryMakeCPM, function(err,result){
	        if(err){
	            console.log("Error returning query: " + err);
	            client.end();
	            context.done("Fatal Error");
	        }
	       	console.log("CPM Table Ready.");
	       	createCPMTempToRemoveDuplicates();
	        
	    });
	    pg.end();
	});
}

var createCPMTempToRemoveDuplicates = function(){
	pg.connect(conString, function(err,client){
	    if(err){
	        return console.log("Connection error. ", err);
	    }
	
	    console.log("Connection Established.");
	
	    //Querying redshift. 
	    var queryCPMTemp = "CREATE TABLE cpm_temp(internal_id varchar(255),site_id varchar(255),homeowner varchar(255),ho_email varchar(255),ho_phone varchar(255),sales_engine varchar(255),sales_agent_name varchar(255),cpm varchar(255),sai varchar(255),sai_territory varchar(255),purchase_type varchar(255),contract_status varchar(255),fully_executed_date date,credit_check_status varchar(255),cc_date date,proposed_system_size_kw varchar(255),site_survey_status varchar(255),site_survey_scheduled_date date,site_survey_completed_date date,site_survey_date_change_reason varchar(255),pda_status varchar(255),pda_last_update_date date,permit_status varchar(255),permit_submit_date date,target_permit_date date,permit_receive_date date,sales_order varchar(255),equipment_status varchar(255),equipment_status_date varchar(255),installation_status varchar(255),target_install_date date,actual_install_date date,main_panel_upgrade_required varchar(255),commissioned_date varchar(255),ahj_inspection_status varchar(255),ahj_inspection_date date,mp2_status varchar(255),sai_mp2_approval_date varchar(255),permission_to_apply_for_pto varchar(255),utility_interconnection_status varchar(255),interconnection_sign_off_date varchar(255),pto_status varchar(255),pto_date varchar(255),mp3_status varchar(255),final_lien_waiver_6_3b varchar(255),days_since_delivery varchar(255));";
	    client.query(queryCPMTemp, function(err,result){
	        if(err){
	            console.log("Error returning query: " + err);
	            client.end();
	            context.done("Fatal Error");
	        }
	       	console.log("CPM Temp Created.");
	       	filterUniqueCPMRows();
	    });
	    pg.end();
	});
}

var filterUniqueCPMRows = function(){
	pg.connect(conString, function(err,client){
	    if(err){
	        return console.log("Connection error. ", err);
	    }
	
	    console.log("Connection Established.");
	
	    //Querying redshift. 
	    var queryFilterCPM = "INSERT INTO cpm_temp SELECT DISTINCT * FROM cpm";
	    client.query(queryFilterCPM, function(err,result){
	        if(err){
	            console.log("Error returning query: " + err);
	            client.end();
	            context.done("Fatal Error");
	        }
	       	console.log("CPM Filtered into CPM_TEMP.");
	       	dropCPMTable();
	    });
	    pg.end();
	});
}

var dropCPMTable = function(){
	pg.connect(conString, function(err,client){
	    if(err){
	        return console.log("Connection error. ", err);
	    }
	
	    console.log("Connection Established.");
	
	    //Querying redshift. 
	    var queryDropCPMForFilter = "drop table CPM";
	    client.query(queryDropCPMForFilter, function(err,result){
	        if(err){
	            console.log("Error returning query: " + err);
	            client.end();
	            context.done("Fatal Error");
	        }
	       	console.log("CPM Dropped");
	       	renameCPMTempToCPM();
	    });
	    pg.end();
	});
}

var renameCPMTempToCPM = function(){
	pg.connect(conString, function(err,client){
	    if(err){
	        return console.log("Connection error. ", err);
	    }
	
	    console.log("Connection Established.");
	
	    //Querying redshift. 
	    var queryRenameCPMTempToCPM = "ALTER TABLE cpm_temp rename to cpm";
	    client.query(queryRenameCPMTempToCPM, function(err,result){
	        if(err){
	            console.log("Error returning query: " + err);
	            client.end();
	            context.done("Fatal Error");
	        }
	       	console.log("CPM_Temp renamed.");
	    });
	    pg.end();
	});
}

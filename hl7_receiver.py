import os
import json
import boto3
import hl7apy
from hl7apy import parser
from hl7apy.parser import parse_message
from hl7apy.parser import parse_field
from hl7apy.core import Group, Segment

region = os.environ['region']
account = os.environ['account']

#create boto3 clients to interact with other AWS services
cm_client = boto3.client(service_name='comprehendmedical', region_name=region)
client = boto3.client('lambda')
sqs_client = boto3.client("sqs", region_name=region)
sns_client = boto3.client('sns')

#main function invoked by lambda
def lambda_handler(event, context):

    #HL7 segments that will be parsed into JSON
    scopedSegments = ['MSH','EVN','PID','PV1','PD1','OBX']
    
    for record in event['Records']:
        
        msg = parse_message(record["body"].replace('\n', '\r').replace('^^','^""^'))
        
        data='{'
        
        for segment in msg.children:
            if isinstance(segment, Segment):
                if segment.name in scopedSegments:
                    for field in segment.children:
                            index=0
                            for element in field.children:
                                    if element.value != '""':
                                        data=data + '"' +   field.name+'_'+str(index) + '":"' +  element.value.replace('\\F\\','|').replace('\\S\\','^').replace('\\R\\','~').replace('\\E\\','').replace('\\T\\','&') + '",'
                                    index=index+1
        
        data = data[:-1] + ', "medications":[], "conditions": [] }'
        
        data2=json.loads(data)

       #invoke Amazon Location Services and gather geo coordinates based on patient address
       #this is an optional step but can add value to geographically locate patients
        
        geoEvent={}
        
        try:
            geoEvent = {
                "address_line": data2["PID_11_0"],
                "municipality_name": data2["PID_11_2"],
                "state_code": data2["PID_11_3"],
                "post_code": data2["PID_11_4"]
            }
        except:
            print("Issues parsing the address")
        
        geo_response = client.invoke(
            FunctionName='arn:aws:lambda:'+region+':'+account+':function:get-geo-location',
            InvocationType='RequestResponse',
            Payload=json.dumps(geoEvent)
        )
        
        resp = geo_response["Payload"].read().decode().replace('"','')
        
        geo_location = {"location_geo": resp}
        
        data2.update(geo_location)
        
               
        #invoke Amazon Comnprehend Medical with admission notes in OBX5
        #this is an optional step but can add value to discover and parse admission notes

        medIndx=0
        conIndx=0
        try:
            cm_result = cm_client.detect_entities(Text= data2["OBX_5_0"])
            entities = cm_result['Entities'];
            for entity in entities:
                #print('Entity', entity["Category"], entity["Text"])
                if entity["Category"] == 'MEDICATION':
                    #print(entity["Text"])
                    for attribute in entity["Attributes"]:
                        if attribute["Type"] in ['DOSAGE']:
                            #print(entity["Text"],attribute["Text"])
                            med = {entity["Category"]:entity["Text"] + "("+attribute["Text"]+")"}
                            data2["medications"].append(med)
                            
                   
                if entity["Category"] == 'MEDICAL_CONDITION':
                    cond = {entity["Category"]:entity["Text"]}
                    data2["conditions"].append(cond)
        except:
            print("Error parsing comprehend response")
            
                
        medicalConditions=""
        medications=""
        
        for key in data2["conditions"]:
            #print(key)
            medicalConditions += key["MEDICAL_CONDITION"] + ", "
            
        for key in data2["medications"]:
            #print(key)
            medications += key["MEDICATION"] + ", "
            
        
        message="""Patient Name: %s, %s
Event: %s
Facility: %s
Family Doctor: %s, %s
Department Contact: %s
Medical Conditions on encouter: %s
Medications: %s""" % (data2["PID_5_0"],data2["PID_5_1"],data2["EVN_1_0"],data2["MSH_6_0"],data2["PD1_4_1"],data2["PD1_4_2"],"(416)555-1234",medicalConditions[:-2],medications[:-2])
        
        response = sns_client.publish(
            TargetArn="arn:aws:sns:"+region+":"+account+":" + data2["PD1_4_0"] +"-provider-notifications",
            Message=message
        )
        print(response)
        
    return {
        'statusCode': 200,
        'body': json.dumps(data2)
    }
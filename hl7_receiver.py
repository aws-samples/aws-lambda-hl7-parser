#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Purpose

When configured as an AWS SQS destination, this Python AWS Lambda function pops 
incoming HL7 messages from an AWS SQS FIFO queue and parses them into a JSON object. 
It parses Segments, Elements and Fields. Component, sub components and repetition 
functionality is not currently supported.

"""

import os
import json
import boto3
from hl7apy.parser import parse_message
from hl7apy.core import Segment
from botocore.exceptions import ClientError

#email address to be used to send notifications
sender = os.environ['sender']

#create boto3 clients to interact with other AWS services

cm_client = boto3.client(service_name='comprehendmedical')
client = boto3.client('lambda')
sqs_client = boto3.client("sqs")
ses_client = boto3.client('ses')

#main function invoked by lambda
def lambda_handler(event, context):

    principal = event.get("requestContext", {}).get("identity", {}).get("userArn", "unknown")

    #HL7 segments that will be parsed into JSON
    scopedSegments = ['MSH','EVN','PID','PV1','PD1','OBX']
    
    charset = "UTF-8"
    
    #parse incoming HL7 message and create a JSON object
    for record in event['Records']:
        
        msg = parse_message(record["body"].replace('\n', '\r').replace('^^','^""^'))

        parsedJsonMessage = {}
        for segment in msg.children:
            if isinstance(segment, Segment):
                if segment.name in scopedSegments:
                    for field in segment.children:
                            index=0
                            for element in field.children:
                                    if element.value != '""':
                                        parsedJsonMessage[field.name+'_'+str(index)] = element.value.replace('\\F\\','|').replace('\\S\\','^').replace('\\R\\','~').replace('\\E\\','').replace('\\T\\','&')
                                    index=index+1
        
        parsedJsonMessage['medications'] = []
        parsedJsonMessage['conditions'] = []
        
        #instantiate data needed for notifications
        msgId               = parsedJsonMessage.get("MSH_10_0","")
        patLastName         = parsedJsonMessage.get("PID_5_0","")
        patFirstName        = parsedJsonMessage.get("PID_5_1","")
        eventType           = parsedJsonMessage.get("EVN_1_0","")
        facility            = parsedJsonMessage.get("MSH_6_0","")
        providerLastName    = parsedJsonMessage.get("PD1_4_1","")
        providerFirstName   = parsedJsonMessage.get("PD1_4_2","")
        providerEmail       = parsedJsonMessage.get("PD1_4_0","")
        patAddressLine      = parsedJsonMessage.get("PID_11_0","")
        patAddressCity      = parsedJsonMessage.get("PID_11_2","")
        patAddressProvince  = parsedJsonMessage.get("PID_11_3","")
        patAddressPostal    = parsedJsonMessage.get("PID_11_4","")
        admissionNotes      = parsedJsonMessage.get("OBX_5_0","")
        
        #enrinch parsedJsonMessage JSON document with geo location information based on patient address from Amazon Location Service
        addressEnrichedJson = discoverAddress(
           patAddressLine,
           patAddressCity,
           patAddressProvince,
           patAddressPostal,
           parsedJsonMessage
        )
               
        #enrich addressEnrichedJson JSON document with Conditions and Medications from Amazon Comprehend Medical       
        updatedJsonMessage = discoverConditionsMedications(
            admissionNotes,
            addressEnrichedJson
        )    
        
        medicalConditions   =""
        medications         =""
        
        #prepare conditions and medications strings for email notification
        medicalConditions = ", ".join(updatedJsonMessage.get("conditions",[]))
        medications = ", ".join(updatedJsonMessage.get("medications",[]))
        
        #send outbound email notification via Amazon SES
        sendEmailNotification(
            patLastName,
            patFirstName,
            eventType,
            facility,
            providerLastName,
            providerFirstName,
            providerEmail,
            medicalConditions,
            medications,
            charset)
        
    maskedEmail = maskEmail(providerEmail)
    print(f"User {principal} issued a notification request for the following Hl7 message {msgId} and the following destination {maskedEmail}")
        
    return {
        'statusCode': 200,
        'body': 'OK'
    }
    
    
def sendEmailNotification(patLastName, patFirstName,eventType,facility,providerLastName,providerFirstName,providerEmail,conditions,medications,charset):
    
    #function to send outbound email notifications via Amazon SES
    
    message="""Patient Name: %s, %s
Event: %s
Facility: %s
Family Doctor: %s, %s
Medical Conditions on encouter: %s
Medications: %s""" % (patLastName,patFirstName,eventType,facility,providerLastName,providerFirstName,conditions,medications)
    recipient = providerEmail
    subject = "New %s notification for %s %s" % (eventType,patFirstName,patLastName)
        
    try:
        #Provide the contents of the email.
        #recipient email address needs to be validated against MDM solution for authenticity and validity
        
        dest = {'ToAddresses': [recipient]}
        msg = {'Body': { 'Text': {'Charset': charset,'Data': message}},'Subject': {'Charset': charset,'Data': subject}}
            
        ses_client.send_email(Destination=dest,Message=msg,Source=sender)
        
    except Exception as err:
        print('Error sending provider notification: ' + str(err) )
        
        
def discoverConditionsMedications(admissionNotes,jsonDoc):
    #invoke Amazon Comnprehend Medical with admission notes in OBX5
    #this is an optional step but can add value to discover and parse admission notes
    #in the event of an issue, the idea is to keep going with the notification

    medIndx=0
    conIndx=0
    try:
        cm_result = cm_client.detect_entities(Text=admissionNotes)
        entities = cm_result['Entities']
        
        for entity in entities:
            if entity["Category"] == 'MEDICATION':
                jsonDoc["medications"].append(entity["Text"])
                      
            if entity["Category"] == 'MEDICAL_CONDITION':
                jsonDoc["conditions"].append(entity["Text"])
    except Exception as err:
        print("Error parsing comprehend response: " + str(err))
    finally:
        return jsonDoc
        
        
def discoverAddress(patAddressLine,patAddressCity,patAddressProvince,patAddressPostal,jsonDoc):
    #invoke Amazon Location Services and gather geo coordinates based on patient address
    #this is an optional step but can add value to geographically locate patients
    #in the event of an issue, the idea is to keep going with the notification
        
    geoEvent={}
    
    try:
        geoEvent = {
            "address_line": patAddressLine,
            "municipality_name": patAddressCity,
            "state_code": patAddressProvince,
            "post_code": patAddressPostal
        }
        
        geo_response = client.invoke(
            FunctionName='function:get-geo-location',
            InvocationType='RequestResponse',
            Payload=json.dumps(geoEvent)
        )
        
        resp = json.loads(geo_response["Payload"].read().decode())
        jsonDoc.update({"location_geo": str(resp.get('Latitude',"0.0")) + "," + str(resp.get('Longitude',"0.0"))})
    except Exception as err:
        print("Issues parsing the address: " + str(err))
    finally:
        return jsonDoc
        
def maskEmail(providerEmail):
    lo = providerEmail.find('@')
    if lo>0:
        return providerEmail[0]+'********'+providerEmail[lo-1:]

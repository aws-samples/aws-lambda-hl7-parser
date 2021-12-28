#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Purpose

Gather geo-coordinates based on an incoming address_line

"""

import boto3
import botocore
import json


def lambda_handler(event, context):
    
    principal       = event.get("requestContext", {}).get("identity", {}).get("userArn", "unknown")
    address         = event.get("address_line", "")
    municipality    = event.get("municipality_name", "")
    state           = event.get("state_code", "")
    postal          = event.get("post_code", "")
    text            = " ".join([ address, municipality, state, postal ])
    
    index_name="Canada"
    country_code = "CAN"
    
    location = boto3.client("location", config=botocore.config.Config(user_agent="Amazon Lambda"))

    try:
        response = location.search_place_index_for_text(IndexName=index_name, FilterCountries=[country_code], Text=text)
        
        data = response["Results"]
        if len(data) >= 1:
            point = data[0]["Place"]["Geometry"]["Point"]
            label = data[0]["Place"]["Label"]
            
            response = {
                "Longitude": point[0],
                "Latitude": point[1],
                "Label": label
            }
        else:
            response = {
                "Error": "No geocoding results found"
            }
            
    except Exception as e:
        #Consider implementing custom Amazon CloudWatch metrics to record specific application errors. 
        #You can view statistical graphs and trigger alerts for your published metrics with the AWS Management Console. 
        
        print(f"User {principal} requested \"{text}\", but raised unexpected exception {str(e)}")
        response = {
                    "Exception": str(e)
        }
        return response
    
    print(f"User {principal} issued a request with the following parameters {text} and returned {response}")

    return response

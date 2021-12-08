import boto3
import botocore
import json
import logging
import os
import time

def lambda_handler(event, context):
    
    print(event)
    
    index_name="Canada"
    datasource="Esri"
    pricing='RequestBasedUsage'

    country_code = "CAN"
    
    location = boto3.client("location", config=botocore.config.Config(user_agent="Amazon Lambda"))

    try:
            
            text = ("%s, %s %s %s" % (event["address_line"], event["municipality_name"], event["state_code"], event["post_code"]))
            response = location.search_place_index_for_text(IndexName=index_name, FilterCountries=[country_code], Text=text)
            
            data = response["Results"]
            if len(data) >= 1:
                point = data[0]["Place"]["Geometry"]["Point"]
                label = data[0]["Place"]["Label"]
                
                
                response = {
                    "Longitude": point[0],
                    "Latitude": point[1],
                    "Label": label,
                    "MultipleMatch": False
                }
                
                if len(data) > 1:
                    response["MultipleMatch"] = True
            else:
                
                
                response = {
                    "Error": "No geocoding results found"
                }
    except Exception as e:
        response ={
            "Exception": str(e)
        }
    
    print(str(response["Latitude"]) + "," + str(response["Longitude"]))

    return str(response["Latitude"]) + "," + str(response["Longitude"])
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

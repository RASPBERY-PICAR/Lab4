import json
import boto3
import pandas as pd
import numpy as np

dict_CO2_MAX = {}
client = boto3.client('iot-data', region_name='us-east-1')


def lambda_handler(event):
    global dict_CO2_MAX

    # TODO1: Get your data
    # Get your maximum of CO2 data
    veh_id = event["vehicle_id"]
    ved_co2 = event["vehicle_CO2"]

    publishChange = False

    if veh_id in dict_CO2_MAX:
        if ved_co2 > dict_CO2_MAX[veh_id]:
            publishChange = True
        else:
            publishChange = False
    else:
        publishChange = True

    # TODO2: Send response back to your device
    if publishChange:
        dict_CO2_MAX[veh_id] = ved_co2
        client.publish(
            topic=veh_id,
            qos=0,
            payload=json.dumps({"vehicle_ID": veh_id, "CO2_MAX": ved_co2})
        )

    # TODO3: Send the results to IOT analytics for aggregation
    if publishChange:
        dict_CO2_MAX[veh_id] = ved_co2
        client.publish(
            topic='CO2_MAX',
            qos=1,
            payload=json.dumps({"vehicle_ID": veh_id, "CO2_MAX": ved_co2})
        )

    # TODO4: Send results to a monitor client
    if publishChange:
        dict_CO2_MAX[veh_id] = ved_co2
        client.publish(
            topic='monitor',
            qos=1,
            payload=json.dumps({"vehicle_ID": veh_id, "CO2_MAX": ved_co2})
        )

    # return {
    #     'statusCode': 200,
    #     'event': event,
    #     'dict_CO2_MAX': dict_CO2_MAX[veh_id]
    # }


# if __name__ == "__main__":
#     test_data = pd.read_csv("data2/vehicle0.csv")
#     for i in range(2):
#         print(test_data.loc[i].to_dict())
#         # print(lambda_handler(test_data.loc[i].to_dict()))

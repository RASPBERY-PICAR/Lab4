import json
import logging
import sys

import greengrasssdk

# Logging
logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# SDK Client
client = greengrasssdk.client("iot-data", region_name='us-east-1')

# # Counter
# my_counter = 0

dict_CO2_MAX = {}


def lambda_handler(event, context):
    global dict_CO2_MAX
    # TODO1: Get your data
    veh_id = event["vehicle_id"]
    timestep = int(event["timestep_time"])
    ved_co2 = event["vehicle_CO2"]

    # TODO2: Calculate max CO2 emission
    # if we have the record of id in dict, and current co2 < recorded MAX, do nothing
    if (veh_id in dict_CO2_MAX) and (ved_co2 < dict_CO2_MAX[veh_id]):
        return

    # else, update reocrd and publish
    dict_CO2_MAX[veh_id] = ved_co2
    # TODO3: Return the result
    # Send response back to device
    client.publish(
        topic=veh_id,
        qos=0,
        payload=json.dumps(
            {"vehicle_ID": veh_id, "CO2_MAX": ved_co2, "time_step": timestep})
    )
    # Send the results to IOT analytics for aggregation
    client.publish(
        topic='CO2_MAX',
        qos=1,
        payload=json.dumps(
            {"vehicle_ID": veh_id, "CO2_MAX": ved_co2, "time_step": timestep})
    )

    # client.publish(
    #     topic="hello/world/counter",
    #     payload=json.dumps(
    #         {"message": "Hello world! Sent from Greengrass Core.  Invocation Count: {}".format(
    #             my_counter)}
    #     ),
    # )
    # my_counter += 1

    return

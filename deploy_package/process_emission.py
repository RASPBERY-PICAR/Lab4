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
    timestep = event["timestep_time"]
    ved_co2 = event["vehicle_CO2"]

    # TODO2: Calculate max CO2 emission
    publishChange = False

    if veh_id in dict_CO2_MAX:
        if ved_co2 > dict_CO2_MAX[veh_id]:
            publishChange = True
        else:
            publishChange = False
    else:
        publishChange = True

    # TODO3: Return the result

    if publishChange:
        dict_CO2_MAX[veh_id] = ved_co2
        client.publish(
            topic=veh_id,
            qos=0,
            payload=json.dumps({"vehicle_ID": veh_id, "CO2_MAX": ved_co2})
        )
        client.publish(
            topic='CO2_MAX',
            qos=1,
            payload=json.dumps({"vehicle_ID": veh_id, "CO2_MAX": ved_co2})
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

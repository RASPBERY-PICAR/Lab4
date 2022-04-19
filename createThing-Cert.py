# Connecting to AWS
import boto3
import os
import json
# Create random name for things
import random
import string
import shutil

# Parameters for Thing
# thingArn = ''
# thingId = ''
# thingName = ''.join(
#     [random.choice(string.ascii_letters + string.digits) for n in range(15)])
defaultPolicyName = 'lab4policy'
thingClient = boto3.client('iot', region_name='us-east-1')
###################################################


def createThing(i):
    global thingClient
    # thingName = "device_{}".format(i)
    thingName = ''.join(
        [random.choice(string.ascii_letters + string.digits) for n in range(15)])
    thingResponse = thingClient.create_thing(
        thingName=thingName
    )
    data = json.loads(json.dumps(thingResponse, sort_keys=False, indent=4))
    for element in data:
        if element == 'thingArn':
            thingArn = data['thingArn']
        elif element == 'thingId':
            thingId = data['thingId']
    createCertificate(thingName, i)
    return


def createCertificate(thingName, i):
    global thingClient
    path = "certificates/device_{}/".format(i)
    if os.path.exists(path):
        shutil.rmtree(path, True)
    os.makedirs(path)

    certResponse = thingClient.create_keys_and_certificate(
        setAsActive=True
    )
    data = json.loads(json.dumps(certResponse, sort_keys=False, indent=4))
    for element in data:
        if element == 'certificateArn':
            certificateArn = data['certificateArn']
        elif element == 'keyPair':
            PublicKey = data['keyPair']['PublicKey']
            PrivateKey = data['keyPair']['PrivateKey']
        elif element == 'certificatePem':
            certificatePem = data['certificatePem']
        elif element == 'certificateId':
            certificateId = data['certificateId']

    # dict = {
    #     "ThingName": thingName,
    #     "certificateArn": certificateArn,
    #     "PublicKey": PublicKey,
    #     "PrivateKey": PrivateKey,
    #     "certificatePem": certificatePem,
    #     "certificateId": certificateId
    # }

    # jsonName = "device_{}.json".format(i)
    privateName = "device_{}.private.pem".format(i)
    certName = "device_{}.certificate.pem".format(i)

    # with open(path+jsonName, 'w') as outfile:
    #     outfile.write(json.dumps(dict))
    with open(path+privateName, 'w') as outfile:
        outfile.write(PrivateKey)
    with open(path+certName, 'w') as outfile:
        outfile.write(certificatePem)

    response = thingClient.attach_policy(
        policyName=defaultPolicyName,
        target=certificateArn
    )
    response = thingClient.attach_thing_principal(
        thingName=thingName,
        principal=certificateArn
    )

    response = thingClient.add_thing_to_thing_group(
        thingGroupName="lab4group",
        thingName=thingName
    )

    return


if __name__ == "__main__":
    for i in range(10):
        createThing(i)

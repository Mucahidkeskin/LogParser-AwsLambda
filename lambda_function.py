import boto3
import json
import io
import zipfile

from decimal import Decimal

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
dynamodb_client = boto3.resource('dynamodb')


def lambda_handler(event, context):
    # get bucket and file name
    bucket = event['Records'][0]['s3']['bucket']['name']
    eventFileName = event['Records'][0]['s3']['object']['key'].replace("%40", "@")
    print(eventFileName)

    if (".zip" in eventFileName):
        zipped_file = s3_resource.Object(bucket_name=bucket, key=eventFileName)
        buffer = io.BytesIO(zipped_file.get()["Body"].read())
        zipped = zipfile.ZipFile(buffer)

        for file in zipped.namelist():
            final_file_path = file + '.extension'
            with zipped.open(file, "r") as f_in:
                content = f_in.read().decode('utf-8')
                print(content)
                if (".libatlog" in file):
                    print(content)
                    s = json.loads(content, parse_float=Decimal)
                    eventFileName = eventFileName[:(eventFileName.rfind('/') + 1)]
                    eventFileName = eventFileName + file
                    print(eventFileName)
    else:
        json_object = s3_client.get_object(Bucket=bucket, Key=(eventFileName))
        file_reader = json_object['Body'].read().decode("utf-8")
        s = json.loads(file_reader, parse_float=Decimal)
    # set variables for parsing
    bInfo = s['BoardInformation'][0]
    sysMod = s['SystemModules']
    batDet = s['BatteryDetails']
    packDet = s['PackDetails']
    count = 0
    mainList = []
    boardInfo = []
    sysList = []
    batList = []
    packList = []

    boardInfo.append(bInfo['Time'])
    boardInfo.append(bInfo['SoftwareVersion'])
    boardInfo.append(bInfo['HardwareVersion'])
    boardInfo.append(bInfo['SerialNumber'])
    boardInfo.append(bInfo['BootloaderVersion'])
    boardInfo.append(bInfo['ModelCode'])
    boardInfo.append(bInfo['ProjectCode'])
    # transpose values for echarts
    for j in sysMod:
        time = []
        id = []
        C1 = []
        C2 = []
        C3 = []
        C4 = []
        C5 = []
        C6 = []
        C7 = []
        C8 = []
        C9 = []
        C10 = []
        C11 = []
        C12 = []
        C13 = []
        C14 = []
        C15 = []
        C16 = []
        C17 = []
        C18 = []
        T1 = []
        T2 = []
        T3 = []
        T4 = []
        T5 = []
        Tpcb = []
        Tfet = []
        CC = []
        VM = []
        listTemp = []
        j = j['Modules']
        if (j == []):
            continue
        for i in j:
            time.append(i['Time'])
            id.append(i['ID'])
            C1.append(i['C1']['V'])
            C2.append(i['C2']['V'])
            C3.append(i['C3']['V'])
            C4.append(i['C4']['V'])
            C5.append(i['C5']['V'])
            C6.append(i['C6']['V'])
            C7.append(i['C7']['V'])
            C8.append(i['C8']['V'])
            C9.append(i['C9']['V'])
            C10.append(i['C10']['V'])
            C11.append(i['C11']['V'])
            C12.append(i['C12']['V'])
            C13.append(i['C13']['V'])
            C14.append(i['C14']['V'])
            C15.append(i['C15']['V'])
            C16.append(i['C16']['V'])
            C17.append(i['C17']['V'])
            C18.append(i['C18']['V'])
            T1.append(str(i['T1']))
            T2.append(str(i['T2']))
            T3.append(str(i['T3']))
            T4.append(str(i['T4']))
            T5.append(str(i['T5']))
            Tpcb.append(str(i['Tpcb']))
            Tfet.append(str(i['Tfet']))
            CC.append(str(i['CC']))
            VM.append(str(i['VM']))
        listTemp.append(time)
        listTemp.append(id)
        listTemp.append(C1)
        listTemp.append(C2)
        listTemp.append(C3)
        listTemp.append(C4)
        listTemp.append(C5)
        listTemp.append(C6)
        listTemp.append(C7)
        listTemp.append(C8)
        listTemp.append(C9)
        listTemp.append(C10)
        listTemp.append(C11)
        listTemp.append(C12)
        listTemp.append(C13)
        listTemp.append(C14)
        listTemp.append(C15)
        listTemp.append(C16)
        listTemp.append(C17)
        listTemp.append(C18)
        listTemp.append(T1)
        listTemp.append(T2)
        listTemp.append(T3)
        listTemp.append(T4)
        listTemp.append(T5)
        listTemp.append(Tpcb)
        listTemp.append(Tfet)
        listTemp.append(CC)
        listTemp.append(VM)
        sysList.append(listTemp)
    time = []
    Vpack = []
    Ipack = []
    soc = []
    for j in batDet:
        time.append(j['Time'])
        Vpack.append(j['Vpack'])
        Ipack.append(j['Ipack'])
        soc.append(j['SOC'])
    batList.append(time)
    batList.append(Vpack)
    batList.append(Ipack)
    batList.append(soc)

    time = []
    VCmax = []
    VCmin = []
    Vd = []
    Tmax = []
    Tmin = []
    Td = []
    Tmean = []
    for j in packDet:
        time.append(j['Time'])
        VCmax.append(j['VCmax'])
        VCmin.append(j['VCmin'])
        Vd.append(j['Vd'])
        Tmax.append(j['Tmax'])
        Tmin.append(j['Tmin'])
        Td.append(j['Td'])
        Tmean.append(j['Tmean'])
    packList.append(time)
    packList.append(VCmax)
    packList.append(VCmin)
    packList.append(Vd)
    packList.append(Tmax)
    packList.append(Tmin)
    packList.append(Td)
    packList.append(Tmean)

    mainList.append(bInfo)
    mainList.append(sysList)
    mainList.append(batList)
    mainList.append(packList)
    jsonstr = json.dumps(mainList)
    # save file as .json
    eventFileName = eventFileName.replace(".libatlog", "")
    s3_client.put_object(Body=str(jsonstr), Bucket=bucket, Key=(eventFileName + '.json'))
    return "success"
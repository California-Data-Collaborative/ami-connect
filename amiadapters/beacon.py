from datetime import datetime, timedelta
import logging
import json
import os
import requests
import time

logger = logging.getLogger(__name__)


def fn_generate_report():

    from dotenv import load_dotenv
    load_dotenv()
    user = os.environ.get('BEACON_AUTH_USER')
    password = os.environ.get('BEACON_AUTH_PASSWORD')

    auth = requests.auth.HTTPBasicAuth(user, password)

    cols = ['Account_Billing_Cycle',
            'Account_Email',
            'Account_First_Name',
            'Account_Full_Name',
            'Account_ID',
            'Account_Last_Name',
            'Account_Phone',
            'Account_Portal_Status',
            'Account_Status',
            'Alert_Code',
            'Backflow_Gallons',
            'Battery_Level',
            'Billing_Address_Line1',
            'Billing_Address_Line2',
            'Billing_Address_Line3',
            'Billing_City',
            'Billing_Country',
            'Billing_State',
            'Billing_Zip',
            'Connector_Type',
            'Current_Leak_Rate',
            'Current_Leak_Start_Date',
            'Demand_Zone_ID',
            'Dials',
            'Endpoint_Install_Date',
            'Endpoint_SN',
            'Endpoint_Status',
            'Endpoint_Type',
            'Estimated_Flag',
            'Flow',
            'Flow_Time',
            'Flow_Unit',
            'High_Read_Limit',
            'Last_Comm_Time',
            'Location_Address_Line1',
            'Location_Address_Line2',
            'Location_Address_Line3',
            'Location_Address_Parity',
            'Location_Area',
            'Location_Bathrooms',
            'Location_Building_Number',
            'Location_Building_Type',
            'Location_City',
            'Location_Continuous_Flow',
            'Location_Country',
            'Location_County_Name',
            'Location_DHS_Code',
            'Location_District',
            'Location_Funding',
            'Location_ID',
            'Location_Irrigated_Area',
            'Location_Irrigation',
            'Location_Latitude',
            'Location_Longitude',
            'Location_Main_Use',
            'Location_Name',
            'Location_Pool',
            'Location_Population',
            'Location_Site',
            'Location_State',
            'Location_Water_Type',
            'Location_Year_Built',
            'Location_Zip',
            'Low_Read_Limit',
            'Meter_Continuous_Flow',
            'Meter_ID',
            'Meter_Install_Date',
            'Meter_Manufacturer',
            'Meter_Model',
            'Meter_Note',
            'Meter_Size',
            'Meter_Size_Desc',
            'Meter_Size_Unit',
            'Meter_SN',
            'Person_ID',
            'Portal_ID',
            'Raw_Read',
            'Read',
            'Read_Code_1',
            'Read_Code_2',
            'Read_Code_3',
            'Read_Method',
            'Read_Note',
            'Read_Sequence',
            'Read_Time',
            'Read_Unit',
            'Reader_Initials',
            'Register_Note',
            'Register_Number',
            'Register_Resolution',
            'Register_Unit_Of_Measure',
            'SA_Start_Date',
            'Service_Point_Class_Code',
            'Service_Point_Class_Code_Normalized',
            'Service_Point_Cycle',
            'Service_Point_ID',
            'Service_Point_Latitude',
            'Service_Point_Longitude',
            'Service_Point_Route',
            'Service_Point_Timezone',
            'Service_Point_Type',
            'Signal_Strength',
            'Supply_Zone_ID',
            'Trouble_Code',
            'Utility_Use_1',
            'Utility_Use_2']

    params = {
        'Start_Date': datetime(2024,8,1),
        'End_Date': datetime(2024,8,2),
        'Resolution': "hourly",
        'Header_Columns': ','.join(cols),
        # 'Meter_ID': '015170783',
        'Has_Endpoint': True
    }

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    #request report
    r = requests.post(
        url='https://api.beaconama.net/v2/eds/range',
        headers=headers,
        params=params,
        auth=auth
    )

    t = json.loads(r.text)
    logger.info(t)

    if r.status_code == 429:
        secs_to_wait = int(t['args'][2])
        time_to_resume = datetime.now() + timedelta(seconds=secs_to_wait)
        logger.info(f"need to wait {secs_to_wait} seconds until {time_to_resume} ({t})")
        return f"rate limit exceeded ({t})"
    elif r.status_code != 202:
        return f"error when requesting report. status code: {r.status_code} (expected status code 200)"

    # request report status
    while True:

        r2 = requests.get(
            url=f"https://api.beaconama.net{t['statusUrl']}",
            headers=headers,
            auth=auth)

        t2 = json.loads(r2.text)
        logger.info(t2)

        import pdb; pdb.set_trace()

        if t2['state'] == 'done':
            break
        elif t2['state'] == 'exception':
            return f"error {t2['message']}"
        elif r2.status_code != 200:
            return f"error when generating report. status code: {r.status_code} (expected status code 200)"
        else:
            time.sleep(60)

    # download report
    try:
        logger.info("downloading report attemp 1")
        r3 = requests.get(
            url='https://api.beaconama.net' + t2['reportUrl'],
            headers=headers,
            auth=auth)
    except:
        try:
            logger.info("waiting for attempt 2")
            time.sleep(60)
            logger.info("downloading report attemp 2")
            r3 = requests.get(
                url='https://api.beaconama.net' + t2['reportUrl'],
                headers=headers,
                auth=auth)
        except Exception as e:
            logger.info("error. can't download report")
            return f"error when downloading report. exception: {e}"

    if r3.status_code != 200:
        return f"error when downloading report. status code: {r.status_code} (expected status code 200)"

    print(r3.content)


if __name__ == "__main__":
    fn_generate_report()
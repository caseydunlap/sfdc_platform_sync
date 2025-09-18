import boto3
import pytz
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta,timezone,date,time
from dateutil.relativedelta import relativedelta
import urllib
import json
import re
import io
from io import BytesIO
import base64
import numpy as np
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key, load_der_private_key

#Function to fetch secrets from Secrets Manager
def get_secrets(secret_names, region_name="us-east-1"):
    secrets = {}
    
    client = boto3.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    for secret_name in secret_names:
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name)
        except Exception as e:
                raise e
        else:
            if 'SecretString' in get_secret_value_response:
                secrets[secret_name] = get_secret_value_response['SecretString']
            else:
                secrets[secret_name] = base64.b64decode(get_secret_value_response['SecretBinary'])

    return secrets
    
def extract_secret_value(data):
    if isinstance(data, str):
        return json.loads(data)
    return data

secrets = ['snowflake_bizops_user','snowflake_account','snowflake_key_pass','snowflake_bizops_wh','snowflake_bizops_role','sfdc_client_secret','sfdc_client_id','sfdc_hostname']

fetch_secrets = get_secrets(secrets)

#Extract all secrets
extracted_secrets = {key: extract_secret_value(value) for key, value in fetch_secrets.items()}

#Secrets
snowflake_user = extracted_secrets['snowflake_bizops_user']['snowflake_bizops_user']
snowflake_account = extracted_secrets['snowflake_account']['snowflake_account']
snowflake_key_pass = extracted_secrets['snowflake_key_pass']['snowflake_key_pass']
snowflake_bizops_wh = extracted_secrets['snowflake_bizops_wh']['snowflake_bizops_wh']
snowflake_role = extracted_secrets['snowflake_bizops_role']['snowflake_bizops_role']
hostname = extracted_secrets['sfdc_hostname']['sfdc_hostname']
client_id = extracted_secrets['sfdc_client_id']['sfdc_client_id']
client_secret = extracted_secrets['sfdc_client_secret']['sfdc_client_secret']

#sfdc version
version = 'v60.0'

password = snowflake_key_pass.encode()

#AWS S3 Configuration params
s3_bucket = 'aws-glue-assets-bianalytics'
s3_key = 'BIZ_OPS_ETL_USER.p8'

#Function to download file from S3
def download_from_s3(bucket, key):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read()
    except Exception as e:
        print(f"Error downloading from S3: {e}")
        return None

#Download the private key file from S3
key_data = download_from_s3(s3_bucket, s3_key)

#Try loading the private key as PEM
private_key = load_pem_private_key(key_data, password=password)

#Extract the private key bytes in PKCS8 format
private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

#Get latest call center data from Snowflake
ctx = snowflake.connector.connect(
    user=snowflake_user,
    account=snowflake_account,
    private_key=private_key_bytes,
    role=snowflake_role,
    warehouse=snowflake_bizops_wh)

cs = ctx.cursor()

script = f"""
with
providers as (
select
"Provider Id",
"HHAX Unique Id"
from analytics.bi.dimprovider p
where p."Is Demo" = false and lower(p."Provider Name") not like '%test%' and lower(p."Provider Name") not like '%demo%'
and lower(p."Provider Name") not like '%training%' and lower(p."Provider Name") not like '%do not use%' and lower(p."Provider Name") not like '%sandbox%' and p."Provider Name" not like '%TDE%' and p."Is Active" = true),

offices as (
select
o."Provider Id",
count(distinct o."Office Id") as offices
from analytics.bi.dimoffice o
inner join providers p on o."Provider Id" = p."Provider Id"
where lower("Office Name") not like '%test%' and lower("Office Name") not like '%demo%'
and lower("Office Name") not like '%training%' and lower("Office Name") not like '%do not use%' and lower("Office Name") not like '%sandbox%' and "Office Name" not like '%TDE%'
and "Is Active" = true
group by o."Provider Id"),

visits_current as (
select
f."Provider Id",
sum(case when f."Is Confirmed" = true and f."Manual Edit" = false and f."Call In Device Type" is not null and f."Call Out Device Type" is not null then 1 else 0 end) as strict_evv_compliant_visits,
sum(case when f."Is Confirmed" = true then 1 else 0 end) as confirmed_visits
from analytics.bi.factvisitcallperformance f
where f."Visit Date Month"::date = dateadd(month,-1,date_trunc('month',current_date())::date)
group by f."Provider Id"),

census_current as (
select
f."Provider Id",
count(distinct "Patient Id") as census_current
from analytics.bi.factvisitcallperformance f
where f."Visit Date Month"::date = dateadd(month,-1,date_trunc('month',current_date())::date)
group by f."Provider Id"),

visits_last as (
select
f."Provider Id",
sum(case when f."Is Confirmed" = true then 1 else 0 end) as confirmed_visits_last,
sum(case when "Billed" = 'yes' and f."Is Confirmed" = true then 1 else 0 end) as billed_visits_last
from analytics.bi.factvisitcallperformance f
where f."Visit Date Month"::date = dateadd(month,-2,date_trunc('month',current_date())::date)
group by f."Provider Id"),

active_caregivers as (
select
c."Provider Id",
count(distinct c."Caregiver Id") as active_caregivers
from ANALYTICS.BI.DIMCAREGIVER c
where c."Caregiver Id" in (select f."Caregiver Id" from analytics.bi.factvisitcallperformance f where c."Caregiver Id" = f."Caregiver Id" and f."Is Confirmed" = true and f."Visit Date Month"::date = dateadd(month,-1,date_trunc('month',current_date())::date))
group by c."Provider Id"),

visits_w_exceptions as (
select
f."Provider Id",
count(distinct v.global_visit_id) as visits_w_exceptions
from analytics.core.visit_call_exception_stat v
inner join analytics.bi.factvisitcallperformance f on v.global_visit_id = f."Visit Id"
where total_exceptions_hist > 0 and date_trunc('month',v.visit_date)::date = dateadd(month,-1,date_trunc('month',current_date())::date)
group by f."Provider Id"),

rejected_visits as (
select
f."Provider Id",
count(distinct 
case 
when trim(v."Current Status") in ('Failed','Rejected (277ca)','Rejected (999)','Compliance Fail','Denied (835)','Denied (BRF)')
then v."Visit Id" end) as rejected_visits_last
from analytics.bi.dimrcoclaimstrackerbatches b
left join analytics.bi.dimrcoclaimstrackervisits v on b."Batch ID Key" = v."Batch ID Key"
inner join analytics.bi.factvisitcallperformance f on v."Visit Id" = f."Visit Id"
where date_trunc('month',v."Visit Date")::date = dateadd(month,-2,date_trunc('month',current_date())::date)
group by f."Provider Id"),

initial_join as (
select
p."Provider Id",
p."HHAX Unique Id",
o.offices as office_count,
coalesce(vc.strict_evv_compliant_visits,0) as evv_visits,
coalesce(vc.confirmed_visits,0) as confirmed_visits_current,
coalesce(vl.confirmed_visits_last,0) as confirmed_visits_last,
coalesce(vl.billed_visits_last,0) as billed_visits_last,
coalesce(c.active_caregivers,0) as active_caregivers,
coalesce(vwe.visits_w_exceptions,0) as visits_w_exceptions,
coalesce(r.rejected_visits_last,0) as rejected_visits_last,
coalesce(cc.census_current,0) as census_current
from providers p
left join offices o on p."Provider Id" = o."Provider Id"
left join visits_current vc on p."Provider Id" = vc."Provider Id"
left join visits_last vl on p."Provider Id" = vl."Provider Id"
left join active_caregivers c on p."Provider Id" = c."Provider Id"
left join visits_w_exceptions vwe on p."Provider Id" = vwe."Provider Id"
left join rejected_visits r on p."Provider Id" = r."Provider Id"
left join census_current cc on cc."Provider Id" = p."Provider Id"),

agg_by_hhax_id as (
select
"HHAX Unique Id" as hhax_id,
sum(office_count) as offices,
sum(evv_visits) as evv_visits,
sum(confirmed_visits_current) as confirmed_visits_current,
sum(confirmed_visits_last) as confirmed_visits_last,
sum(billed_visits_last) as billed_visits_last,
sum(active_caregivers) as active_caregivers,
sum(visits_w_exceptions) as visits_with_exceptions,
sum(rejected_visits_last) as rejected_visits_last,
sum(census_current) as census_current
from initial_join
group by hhax_id),

calculations as (
select
hhax_id,
census_current,
active_caregivers,
confirmed_visits_current,
((confirmed_visits_current - visits_with_exceptions) / nullif(confirmed_visits_current,0)) as first_pass_yield,
evv_visits / nullif(confirmed_visits_current,0) as evv_rate,
billed_visits_last / nullif(confirmed_visits_last,0) as billing_rate_last,
rejected_visits_last / nullif(billed_visits_last,0) as rejection_rate_last,
offices
from agg_by_hhax_id)

select * from calculations;
"""

payload = cs.execute(script)
dataset = pd.DataFrame.from_records(iter(payload), columns=[x[0] for x in payload.description])

dataset['CENSUS_CURRENT'] = dataset['CENSUS_CURRENT'].astype('Int64')
dataset['ACTIVE_CAREGIVERS'] = dataset['ACTIVE_CAREGIVERS'].astype('Int64')
dataset['CONFIRMED_VISITS_CURRENT'] = dataset['CONFIRMED_VISITS_CURRENT'].astype('Int64')
dataset['OFFICES'] = dataset['OFFICES'].astype('Int64')
dataset['FIRST_PASS_YIELD'] = (dataset['FIRST_PASS_YIELD'].astype(float) * 100).round(2)
dataset['EVV_RATE'] = (dataset['EVV_RATE'].astype(float) * 100).round(2)
dataset['BILLING_RATE_LAST'] = (dataset['BILLING_RATE_LAST'].astype(float) * 100).round(2)
dataset['REJECTION_RATE_LAST'] = (dataset['REJECTION_RATE_LAST'].astype(float) * 100).round(2)

dataset = dataset.replace({np.nan: None, pd.NA: None})

eastern = pytz.timezone('America/New_York')
name = (datetime.now(eastern) - relativedelta(months=1)).strftime('%B %Y')

###for testing#####
# test_df = pd.DataFrame({
#     'HHAX_ID': ['0010Z00002C75ALQAZ', '0010Z00002C8UsPQAV'],
#     'CENSUS_CURRENT': [28, 35],
#     'ACTIVE_CAREGIVERS': [3,7],
#     'CONFIRMED_VISITS_CURRENT': [100,150],
#     'OFFICES' : [1,2],
#     'REJECTION_RATE_LAST': [23.65,21.54],
#     'EVV_RATE' : [90.65,88.54],
#     'BILLING_RATE_LAST': [91.32,90.67],
#     'FIRST_PASS_YIELD': [100.00,32.32]})

account_updates = {
    row['HHAX_ID']: {
        'Actual_Census__c': row['CENSUS_CURRENT'],
        'of_Caregivers__c': row['ACTIVE_CAREGIVERS'],
        'Confirmed_Visits__c': row['CONFIRMED_VISITS_CURRENT'],
        'of_Offices__c': row['OFFICES'],
        'Account__c' : row['HHAX_ID'],
        'First_Pass_Yield__c' : row['FIRST_PASS_YIELD'],
        'EVV_Compliance__c' : row['EVV_RATE'],
        'Billing_Rate__c' : row['BILLING_RATE_LAST'],
        'Rejection_Rate__c' : row['REJECTION_RATE_LAST'],
        'Name':name}
    for _, row in dataset.iterrows()
}

#Function to fetch access token and instance url from Salesforce
def fetch_access_token_and_url(hostname,client_id,client_secret,type):
    
    url = f'{hostname}/services/oauth2/token'

    payload = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret
    }

    if type == 'access_token':
        return requests.post(url, data=payload).json().get('access_token')
    elif type == 'url':
        return requests.post(url, data=payload).json().get('instance_url')
        
#Build request headers
headers = {
"Authorization": f"Bearer {fetch_access_token_and_url(hostname,client_id,client_secret,'access_token')}",
"Content-Type": "application/json"}

#Fetch instance url from Salesforce
instance_url = fetch_access_token_and_url(hostname,client_id,client_secret,'url')

#Push data to Salesforce
for account_id, field_updates in account_updates.items():

    url = f"{instance_url}/services/data/{version}/sobjects/Historical_Census__c/"

    try:
        response = requests.post(url, headers=headers,json=field_updates)

        if response.status_code == 201:
            result = response.json()
        else:
            print(f"✗ Failed to create record: {response.status_code}")
            print(response.json())
    except requests.exceptions.RequestException as e:
            print(f"Error updating Account {account_id}: {e}")
            

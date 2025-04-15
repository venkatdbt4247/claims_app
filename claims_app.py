import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
import pandas as pd
import math
st.title("Insurance Claims Prediction ")

#session = get_active_session()

cnx = st.connection( "snowflake")

session = cnx.session()


session.sql("use role INSURANCECLAIMSPREDICTIONREGRESSION_DATA_SCIENTIST")

# get a list of area for a drop list selection
AREA = AREA = session.sql("select distinct AREA from raw.motor_insurance_policy_claims")
pd_area = AREA.to_pandas()

# get a list of vehicle power for a drop list selection
VEHPOWER = session.sql("select distinct VEHPOWER from raw.motor_insurance_policy_claims")
pd_vehpower = VEHPOWER.to_pandas()

# get a list of vehicle age for a drop list selection
VEHAGE = session.sql("select distinct VEHAGE from raw.motor_insurance_policy_claims")
pd_vehage = VEHAGE.to_pandas()

# get a list of driver age for a drop list selection
DRIVAGE = session.sql("select distinct DRIVAGE from raw.motor_insurance_policy_claims")
pd_drivage = DRIVAGE.to_pandas()


# get a list of bonusmalus for a drop list selection
BONUSMALUS = session.sql("select distinct BONUSMALUS from raw.motor_insurance_policy_claims")
pd_bonusmalus = BONUSMALUS.to_pandas()

# get a list of vehbran for a drop list selection
VEHBRAND = session.sql("select distinct VEHBRAND from raw.motor_insurance_policy_claims")
pd_vehbrand = VEHBRAND.to_pandas()

# get a list of vehgas for a drop list selection
VEHGAS = session.sql("select distinct VEHGAS from raw.motor_insurance_policy_claims")
pd_vehgas = VEHGAS.to_pandas()


# get a list of region for a drop list selection
REGION = session.sql("select DISTINCT REGION from raw.motor_insurance_policy_claims")
pd_region = REGION.to_pandas()

st.markdown("""
     <style>
     .stApp {
           background-color: #f0f8ff;
     }
     </style>
""", unsafe_allow_html=True)

st.markdown("""
     <style>
     div[data-baseweb="select"] {
        background-color: #ffffff;
        border-radius: 10px;
        padding: 5px;
        border: 2px solid #4CAF50
     }
     div[data-baseweb="select"] > div {
        color: #333333;
        font-weight: bold;
     }
     </style>
""", unsafe_allow_html=True)


st.markdown("""
     <style>
     div[data-baseweb="text_input"] {
        background-color: #ffffff;
        border-radius: 10px;
        padding: 5px;
        border: 2px solid #4CAF50
     }
     div[data-baseweb="select"] > div {
        color: #333333;
        font-weight: bold;
     }
     </style>
""", unsafe_allow_html=True)


st.markdown("""
     <style>
     div.stButton > button.first-child {
        background-color: #4CAF50;
        color: white;
        border: none;
        border-radius: 10px;
        padding: 0.5em 1em;
        font-weight: bold;
        transition: 0.3s;
        border: 2px solid #4CAF50
     }
     div.stButton > button:first-child-hove {
        background-color: #45a049;
     }
     </style>
""", unsafe_allow_html=True)

# Oyt the list of area into a drop list selector 

sel_exposure = st.text_input("Enter the Exposure (range from 0 to 1):")

sel_area = st.selectbox('Select the Consumer area:', pd_area)
sel_vehpower = st.selectbox('Select the Vehicle power:', pd_vehpower)

sel_vehage = st.selectbox('Select the Vehicle age:', pd_vehage)
sel_drivage = st.selectbox('Select the Driver Age:', pd_drivage)


sel_bonusmalus = st.selectbox('Select the credit score :', pd_bonusmalus)
sel_vehbrand = st.selectbox('Select The Vehicle brand:', pd_vehbrand)


sel_vehgas = st.selectbox('Select the Vehicle Fuel Type :', pd_vehgas)
sel_region = st.selectbox('Select the Vehicle owner region:', pd_region)

sel_rows = st.text_input("Enter the sample size")
if sel_drivage < 18:
    Driver_Age_Banded  = "DRIVAGE_Under_18"
elif 18 > sel_drivage <= 21:
    Driver_Age_Banded  = "DRIVAGE_18-21"
elif 21 > sel_drivage <= 25:
     Driver_Age_Banded  = "DRIVAGE_21-25"
elif 25 > sel_drivage <= 35:
     Driver_Age_Banded  = "DRIVAGE_25-35"
elif 35 > sel_drivage <= 45:
     Driver_Age_Banded  = "DRIVAGE_35-45"
elif 45 > sel_drivage <= 55:
     Driver_Age_Banded  = "DRIVAGE_45-55"
elif 55 > sel_drivage <= 70:
     Driver_Age_Banded  = "DRIVAGE_55-70"
else:
    Driver_Age_Banded  = "DRIVAGE_Over_70,"

if sel_vehage < 0:
    Vehicle_Age_Banded  = "VEHAGE_NewVehicle"
elif 0 > sel_vehage <= 1:
    Vehicle_Age_Banded  = "VEHAGE_0-1"
elif 1 > sel_vehage <= 4:
     Vehicle_Age_Banded  = "VEHAGE_1-4"
elif 4 > sel_vehage <= 10:
     Vehicle_Age_Banded  = "VEHAGE_4-10"
else:
    Vehicle_Age_Banded  = "VEHAGE_Over_10,"

density = session.sql('select distinct density from  raw.motor_insurance_policy_claims where region = \'\'' + str(sel_region) + '\'\'')
pd_density = density.to_pandas()
sel_density = st.selectbox('Select the density owner region:', pd_density)

# sel_log_density = math.log(sel_density)


if st.button('Submit'):
    for i in range(int(sel_rows)):
        sql_insert = 'insert into raw.motor_insurance_policy_claims (EXPOSURE,AREA ,VEHPOWER,VEHAGE ,DRIVAGE, BONUSMALUS,VEHBRAND,VEHGAS ,DENSITY, REGION)select ' + str(sel_exposure) +',\'\''+  str(sel_area) + '\'\',\''+  str(sel_vehpower) + '\',\''+  str(sel_vehage)   +'\',\''+  str(sel_drivage)+ '\',\''+  str(sel_bonusmalus) + '\',\'\''+  str(sel_vehbrand)   +'\'\',' +  str(sel_vehgas)   +',\''+  str(sel_region)   +'\',\''+  str(sel_density) + '\''
        session.sql(sql_insert)
        st.write(sql_insert)
        st.write("success")

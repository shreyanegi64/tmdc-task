import streamlit as st
import pandas as pd
import numpy as np
import time
import matplotlib.pyplot as plt

st.set_option('deprecation.showPyplotGlobalUse', False)

# Upload ride data 
ride_data_2020 = pd.read_csv("fact_ride_table_2020.csv")
ride_data_2021 = pd.read_csv("fact_ride_table_2021.csv")
ride_data_2022 = pd.read_csv("fact_ride_table_2022.csv")

# Concatenated Dataframe for Ride data
ride_data = pd.concat([ride_data_2020, ride_data_2021, ride_data_2022] , ignore_index= True)

# Data Manipulation using pandas
ride_data['r_booked_time'] = pd.to_datetime(ride_data['r_booked_time'])
ride_data['year'] = ride_data['r_booked_time'].dt.year
ride_data['month'] = ride_data['r_booked_time'].dt.month
ride_data['month_name'] = pd.to_datetime(ride_data['month'], format='%m').dt.strftime('%b')
years = ride_data["year"].unique()

rad = st.sidebar.radio("Navigation" , ["Home" , "Ride Info"])

if rad == "Ride Info":
    st.header("Uber Ride Details")
    
    with st.spinner(text='In progress'):
        time.sleep(2)
        
    yearly_count = ride_data.groupby('year').size().reset_index(name='total_rides').sort_values('year')
    monthly_count = ride_data.groupby(['year', 'month' , 'month_name']).size().reset_index(name='total_rides').sort_values(['year' , 'month'])

    
    col1, col2 = st.columns(2)
    
# Calculating Yearly Rides
    with col1:
        yearly_count.plot(x='year', y='total_rides', kind='bar' , color = "LightPink" )
        plt.xlabel('Year')
        plt.ylabel('Total Rides')
        plt.title('Total Rides per Year')
        plt.xticks(rotation=0)
        st.pyplot()
    
# Calculating Monthly Rides per Year
    with col2:
        for year, data in monthly_count.groupby('year'):
            plt.plot(data['month_name'], data['total_rides'], label=str(year) , marker='o')
        plt.xlabel('Month')
        plt.ylabel('Total Rides')
        plt.title('Monthly Rides per Year')
        plt.legend(title='Year', loc='upper right')
        st.pyplot()
        
# Filter data according to year
    select_year = st.slider("Select year" , min_value = int(min(years)) , max_value= int(max(years)) , value= int(min(years)))
    
    # Condition for year 2020
    if select_year == 2020:
        def ride_details_2020(ride_id):
        # Filter the ride_data DataFrame based on ride_id
            filtered_data = ride_data_2020[ride_data_2020['r_id'] == ride_id][['r_id', 'r_d_id', 'r_pass_id', 'r_pickup_location', 'r_drop_location', 'final_amount']]
            return filtered_data

        count_2020 = len(ride_data_2020)
        st.subheader("Total Rides in year 2020 was " + str(count_2020) )
        if st.checkbox("show data"):
            st.write(ride_data_2020)
        
        ride_id = st.text_input("Enter ride ID:")
        if st.button("Check"):
            details = ride_details_2020(ride_id)
            if not details.empty:
                st.write("Ride Details:")
                st.write(details)
            else:
                st.write("Invalid Ride ID!! You may change the year")

    # Condition for year 2021
    elif select_year == 2021:
        def ride_details_2021(ride_id):
        # Filter the ride_data DataFrame based on ride_id
            filtered_data = ride_data_2021[ride_data_2021['r_id'] == ride_id][['r_id', 'r_d_id', 'r_pass_id', 'r_pickup_location', 'r_drop_location', 'final_amount']]
            return filtered_data   
        
        count_2021 = len(ride_data_2021)
        st.subheader("Total Rides in year 2021 was " + str(count_2021) )
        if st.checkbox("Show Data"):
            st.write(ride_data_2021)
        
        ride_id = st.text_input("Enter ride ID:")
        if st.button("Check"):
            details = ride_details_2021(ride_id)
            if not details.empty:
                st.write("Ride Details:")
                st.write(details)
            else:
                st.write("Invalid Ride ID!! You may change the year")

    # Condition for year 2022   
    else:
        def ride_details_2022(ride_id):
        # Filter the ride_data DataFrame based on ride_id
            filtered_data = ride_data_2022[ride_data_2022['r_id'] == ride_id][['r_id', 'r_d_id', 'r_pass_id', 'r_pickup_location', 'r_drop_location', 'final_amount']]
            return filtered_data  
        
        count_2022 = len(ride_data_2022)
        st.subheader("Total Rides in year 2022 was " + str(count_2022) )
        if st.checkbox("Show Data"):
            st.write(ride_data_2022)
            
        ride_id = st.text_input("Enter ride ID:")
        if st.button("Check"):
            details = ride_details_2022(ride_id)
            if not details.empty:
                st.write("Ride Details:")
                st.write(details)
            else:
                st.write("Invalid Ride ID!! You may change the year") 

if rad == "Home":
    st.header("Welcome to Uber")
    st.write("Movement is what we power. It's our lifeblood. It runs through our veins. It's what gets us out of bed each morning. It pushes us to constantly reimagine how we can move better. For you. For all the places you want to go. For all the things you want to get. For all the ways you want to earn. Across the entire world. In real time. At the incredible speed of now.")
    st.image("auto.png")
    st.image("bike.png")
    st.image('car.png')
    st.image("uber.png")
            


    
    

    



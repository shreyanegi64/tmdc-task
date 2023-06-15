from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
from faker import Faker
import numpy as np
import random
import datetime
fake = Faker('en_IN')

def generate_v_ids(start, add, number):
    v_ids = []
    current_id = start
    for _ in range(number):
        v_ids.append(f"V{current_id:04d}")
        current_id += add
    return v_ids

def generate_d_ids(start, add, num_records):
    d_ids = []
    current_id = start
    for _ in range(num_records):
        d_ids.append(f"D{current_id:04}")
        current_id += add
    return d_ids

def generate_pass_ids(start, add, number):
    pass_ids = []
    current_id = start
    for _ in range(number):
        pass_ids.append(f"P{current_id:04d}")
        current_id += add
    return pass_ids

def generate_c_ids(start, add, number):
    c_ids = []
    current_id = start
    for _ in range(number):
        c_ids.append(f"C{current_id:04d}")
        current_id += add
    return c_ids

def generate_r_ids(start, add, number):
    r_ids = []
    current_id = start
    for _ in range(number):
        r_ids.append(f"R{current_id:04d}")
        current_id += add
    return r_ids

def generate_feedback_ids(start, add, number):
    feedback_ids = []
    current_id = start
    for _ in range(number):
        feedback_ids.append(f"F{current_id:04d}")
        current_id += add
    return feedback_ids

date = "2023-6-4"
date = datetime.datetime.strptime(date , "%Y-%m-%d").date()

num_records_2022 = 1000
num_records_2021 = 900
num_records_2020 = 850

pass_2020 = 1000
pass_2021 = 1200
pass_2022 = 1500

# number of records for coupon table
c_2020 = 600
c_2021 = 800
c_2022 = 1000

# number of records for ride and feedback table
ride_2020 = 3000
ride_2021 = 5000
ride_2022 = 7000

def generate_and_upload_vehicle_data(year, num_records , connection_string ):
    types = ['Auto', 'Bike', 'Four wheeler']
    if year == 2020:
        v_ids = generate_v_ids(1, 1, num_records)
        v_type = np.random.choice(types, size=num_records, p=[0.70, 0.0, 0.30])
    elif year == 2021:
        v_ids = generate_v_ids(851, 1, num_records)
        v_type = np.random.choice(types, size=num_records, p=[0.50, 0.20, 0.30])
    elif year == 2022:
        v_ids = generate_v_ids(1751, 1, num_records)
        v_type = np.random.choice(types, size=num_records, p=[0.40, 0.30, 0.30])

    vehicle_data = []
    for i, v_id in enumerate(v_ids):
        v_N = 'MP-{0:02d}-{1:04d}'.format(random.randint(1, 10), random.randint(0, 9999))
        v_name = ''
        v_color = ''
        if v_type[i] == 'Auto':
            v_name = 'Auto'
            v_color = 'Yellow'
        elif v_type[i] == 'Bike':
            bike_name = ['Ninja', 'Raptor', 'Thunderbird', 'Viper', 'Fury', 'Cyclone', 'Bullet', 'Firestorm', 'Dominator', 'Apache']
            v_name = np.random.choice(bike_name)
            color = ['Red', 'Black', 'White', 'Yellow']
            v_color = np.random.choice(color)
        else:
            name = ['Brezza', 'Ertiga', 'Skoda', 'Suzuki', 'Bolero', 'Celerio']
            v_name = np.random.choice(name)
            color = ['Red', 'Black', 'White', 'Yellow']
            v_color = np.random.choice(color)

        v_insurance_end_date = fake.date_between(start_date='-1y', end_date='+5y')

        record = {'v_id': v_id, 'v_number': v_N, 'v_type': v_type[i], 'v_name': v_name, 'v_color': v_color, 'v_insurance_end_date': v_insurance_end_date}
        vehicle_data.append(record)

    vehicle_df = pd.DataFrame(vehicle_data)
    vehicle_df.to_csv(f"vehicle_table_{year}.csv", header=True, index=False)

    upload_to_azure("vehicle_table", year, connection_string)

    return vehicle_df

v_ids_2020 = generate_v_ids(1, 1, num_records_2020)
v_ids_2021 = generate_v_ids(851, 1, num_records_2021)
v_ids_2022 = generate_v_ids(1751, 1, num_records_2022)

def generate_and_upload_driver_data(year, num_records , v_ids , connection_string):
    first_date = datetime.datetime(year, 1, 1)
    last_date = datetime.datetime(year, 12, 31)
    date_range = pd.date_range(first_date, last_date, freq='D')
    
    if year == 2020:
        d_ids = generate_d_ids(1, 1, num_records)
    elif year == 2021:
        d_ids = generate_d_ids(851, 1, num_records)
    elif year == 2022:
        d_ids = generate_d_ids(1751, 1, num_records)

    random.shuffle(v_ids)

    driver_data = []
    for i, d_id in enumerate(d_ids):
        d_ph_N = fake.phone_number()  
        d_dob = fake.date_of_birth(minimum_age=18, maximum_age=60)
        d_doj = np.random.choice(date_range)
        d_gender = np.random.choice(['M', 'F'], p=[0.8, 0.2])
        if d_gender == 'M':
            d_name = fake.name_male()
        else:
            d_name = fake.name_female()
        d_email = f"{d_name.lower().replace(' ','')}@{random.choice(['gmail.com', 'yahoo.com'])}"
        d_status = np.random.choice(['W', 'L'], p=[0.8, 0.2])
        if d_status == 'L':
            d_date_resigned = fake.date_between(start_date = d_doj + datetime.timedelta(days=365+1))
        else:
            d_date_resigned = np.random.choice(['-'])
        joined_with_referral = random.choice(['Y', 'N'])
        d_license_N = fake.license_plate()
        d_v_id = v_ids[i]

        record = {
            'd_id': d_id,
            'd_name': d_name,
            'd_ph_N': d_ph_N,
            'd_email': d_email,
            'd_dob': d_dob,
            'd_doj': d_doj,
            'd_gender': d_gender,
            'd_status': d_status,
            'd_date_resigned':d_date_resigned ,
            'joined_with_referral': joined_with_referral,
            'd_license_N': d_license_N,
            'd_v_id': d_v_id
        }
        driver_data.append(record)

        # Convert DataFrame to CSV string
        driver_df = pd.DataFrame(driver_data)
        driver_csv = driver_df.to_csv(f"driver_table_{year}.csv", header=True, index=False)
     
    upload_to_azure("driver_table", year, connection_string)

    return driver_df
    
d_ids_2020 = generate_d_ids(1, 1, num_records_2020)
d_ids_2021 = generate_d_ids(851, 1, num_records_2021)
d_ids_2022 = generate_d_ids(1751, 1, num_records_2022)

def generate_and_upload_passenger_data(year , pass_records , connection_string):
    if year == 2020:
        pass_ids = generate_pass_ids(1, 1, pass_records)
    elif year == 2021:
        pass_ids = generate_pass_ids(1001, 1, pass_records)
    elif year == 2022:
        pass_ids = generate_pass_ids(2201, 1, pass_records)
        
    first_date = datetime.datetime(year, 1, 1)
    last_date = datetime.datetime(year, 12, 31)
    date_range = pd.date_range(first_date, last_date, freq='D')
        
    pass_data = []
    for pass_id in pass_ids:
        pass_ph_no = fake.phone_number()
        pass_dob = fake.date_of_birth(minimum_age = 18 , maximum_age=60)
        sign_up_date = np.random.choice(date_range)
        pass_gender = np.random.choice(['M' , 'F'])
        if pass_gender == 'M':
            pass_name = fake.name_male()
        else:
            pass_name = fake.name_female()
        pass_email = f"{pass_name.lower().replace(' ','')}@{np.random.choice(['gmail.com' , 'yahoo.com'])}"
        device_os = np.random.choice(['Android' , 'iOS'] , p = [0.70 , 0.30])
        occupation = ['Teacher' , 'Student' , 'Lawyer' , 'Doctor' , 'Journalist' , 'Makeup Artist']
        pass_occupation = np.random.choice(occupation , p =['0.20' , '0.40' , '0.05' , '0.10' , '0.05' , '0.20'])
        wallet_attached = np.random.choice(['Y' , 'N'] , p = ['0.25' , '0.75'])

        record = {'pass_id': pass_id , 'pass_name' : pass_name , 'pass_ph_no' : pass_ph_no ,
                'pass_email' : pass_email , 'pass_dob' : pass_dob , 'sign_up_date' : sign_up_date ,
                'pass_gender': pass_gender  ,  'device_os':device_os , 'wallet_attached': wallet_attached ,'pass_occupation': pass_occupation }
        pass_data.append(record)


        pass_df = pd.DataFrame(pass_data)
        pass_df.to_csv(f"pass_table_{year}.csv", header=True, index=False)
        
    upload_to_azure("pass_table", year, connection_string)
    
    return pass_df
    
pass_ids_2020 = generate_pass_ids(1, 1, pass_2020)
pass_ids_2021 = generate_pass_ids(1001, 1, pass_2021)
pass_ids_2022 = generate_pass_ids(2201, 1, pass_2022)

def generate_and_upload_coupon_data(year , coupon_records , pass_ids , connection_string):
    first_date = datetime.datetime(year, 1, 1)
    last_date = datetime.datetime(year, 12, 31)
    date_range = pd.date_range(first_date, last_date, freq='D')
    
    if year == 2020:
        c_ids = generate_c_ids(1 , 1 , coupon_records)
        c_pass_ids = pass_ids
            
    elif year == 2021:
        c_ids = generate_c_ids(601 , 1 , coupon_records)
        c_pass_ids = pass_ids

    elif year == 2022:
        c_ids = generate_c_ids(1401 , 1 , coupon_records)
        c_pass_ids = pass_ids
    
    code = ['OFF20' , 'NEWOFF' , 'CARDAXIS']

    coupon_data = []
    for c_id in c_ids:
        c_allocation_date = np.random.choice(date_range)
        c_code = np.random.choice(code)

        c_allocation_date = pd.to_datetime(c_allocation_date)
        
        if c_code == 'OFF20':
            c_discount = np.random.choice(['20'])
            c_validity_date = fake.date_between(start_date = c_allocation_date + datetime.timedelta(days=5) , end_date = c_allocation_date + datetime.timedelta(days=15))

        elif c_code == 'NEWOFF':
            c_discount = np.random.choice(['5'])
            c_validity_date = fake.date_between(start_date =c_allocation_date + datetime.timedelta(days=1) , end_date = c_allocation_date + datetime.timedelta(days=3))

        elif c_code == 'CARDAXIS':
            c_discount = np.random.choice(['15'])
            c_validity_date = fake.date_between(start_date =c_allocation_date + datetime.timedelta(days=3) , end_date = c_allocation_date + datetime.timedelta(days=10))

        c_pass_id = np.random.choice(c_pass_ids)  
        # coupon_used = np.random.choice(['Y' , 'N']) , 'coupon_used' : coupon_used 
        

        # c_allocation_date = fake.date_between(first_date = )
        # coupon_validity = fake.date_between(first_date = '-2M' , last_date = '+2M') 

        record = {'c_id': c_id , 'c_code' : c_code , 'c_discount%' : c_discount , 'c_pass_id':c_pass_id ,
                # 'sign_up_date'  : sign_up_date ,
                'c_allocation_date' : c_allocation_date ,
                'c_validity_date':c_validity_date }
        coupon_data.append(record)


        coupon_df = pd.DataFrame(coupon_data)
        coupon_df.to_csv(f"coupon_table_{year}.csv", header=True, index=False)
        
    upload_to_azure("coupon_table", year, connection_string)

    return coupon_df

c_ids_2020 = generate_c_ids(1 , 1 , c_2020)
c_ids_2021 = generate_c_ids(601, 1, c_2021)
c_ids_2022 = generate_c_ids(1401, 1, c_2022)
    
def generate_and_upload_c_bridge_data(year, c_b_records, pass_ids , coupon_df , connection_string):
    df = pd.DataFrame({'c_pass_id': pass_ids})

    df2 = pd.merge(df, coupon_df, on='c_pass_id', how='left')
    coupon_bridge_table = df2.groupby('c_pass_id')['c_id'].apply(list).reset_index()
    coupon_bridge_table['c_ids_alloted'] = coupon_bridge_table['c_id'].apply(lambda x: ['-'] if pd.isna(x[0]) else x)
    coupon_bridge_table.drop('c_id', axis=1, inplace=True)
    coupon_bridge_table.to_csv(f"c_bridge_table_{year}.csv", header=True, index=False)
    
    upload_to_azure("c_bridge_table", year, connection_string)

    return coupon_bridge_table

def generate_and_upload_ride_data(year , ride_records , pass_ids , d_ids , c_ids , coupon_df , coupon_bridge_table , driver_df , connection_string):
    if year == 2020:
        r_ids = generate_r_ids(1  , 1 , ride_records)
    elif year == 2021:
        r_ids = generate_r_ids(3001 , 1 , ride_records)
    else:
        r_ids = generate_r_ids(8001 , 1 , ride_records)
        
    first_date = datetime.datetime(year, 1, 1)
    last_date = datetime.datetime(year, 12, 31)
        
    ride_data = []
    for r_id in r_ids:
        bool_value = ['Y' , 'N']
        area = ['Vijay Nagar' , 'Palasia' , 'AB Road (Agra Bombay Road)' , 'Old Palasia' , 'South Tukoganj' , 'New Palasia' , 'Rajendra Nagar' , 'Geeta Bhawan' , 
            'Bhawarkuan' , 'Annapurna Road' , 'Scheme No. 54' , 'Nipania' , 'Bengali Square' , 'Sudama Nagar' , 'Navlakha' , 'Rau' , 'Khandwa Road' ,
            'Silicon City (Rau)' , 'Manorama Ganj' , 'Bicholi Mardana]']
        mode = ['cash' , 'online']
        method = ['upi' , 'credit card' , 'debit card' , 'wallet']
        status = ['completed' , 'failed' , 'pending']
        r_pass_id = np.random.choice(pass_ids)
        r_d_id = np.random.choice(d_ids)
        r_booked_time = fake.date_time_between(start_date = first_date , end_date = last_date )
        r_status = np.random.choice(['1' , '0'] , p = [0.70 , 0.30])
        #ride_type = np.random.choice(['Individual' , 'Sharing'])
        r_pickup_location = np.random.choice(area)
        # ride drop location
        r_drop_location = np.random.choice(area)
        while r_pickup_location == r_drop_location:
            r_drop_location = np.random.choice(area)

        if r_status == '1': # ride completed
            r_start_time = fake.date_time_between_dates(datetime_start = r_booked_time + datetime.timedelta(minutes=15) , datetime_end = r_booked_time + datetime.timedelta(minutes=30))
            dist_covered_km = fake.random_int(min = 2 , max = 20)
            r_drop_location_changed = np.random.choice(bool_value , p = [0.40 , 0.60])
            if r_drop_location_changed == 'Y':
                r_drop_location_changed_to = np.random.choice(area)
                while r_drop_location_changed_to == r_drop_location:
                    r_drop_location_changed_to = np.random.choice(area)
                extra_dist_covered_km = fake.random_int(min = 2 , max = 20)    
            else:
                r_drop_location_changed_to = 'location not changed'
                extra_dist_covered_km = 0
            total_dist_covered_km = dist_covered_km + extra_dist_covered_km
            r_drop_time = fake.date_time_between_dates(datetime_start = r_start_time + datetime.timedelta(minutes=10) , datetime_end= r_start_time +datetime.timedelta(minutes=60))
            # ride_coupon_id = np.random.choice(c_ids)
            # ride_coupon_used = np.random.choice(bool_value)
            ride_fare = 5 * total_dist_covered_km
            payment_mode = np.random.choice(mode , p = [0.50 , 0.50])
            if payment_mode == 'cash':
                payment_method = np.random.choice(['cash'])
                payment_status = np.random.choice(['completed'])
            else:
                payment_method = np.random.choice(method , p = [0.25 , 0.25, 0.25, 0.25])
                payment_status = np.random.choice(status , p = [0.85 , 0.05 , 0.1])
            payment_mode_changed = np.random.choice(bool_value , p =[0.10 , 0.90])
            if payment_mode_changed == 'Y':
                payment_mode_changed_to = np.random.choice(mode)
                while payment_mode_changed_to == payment_mode:
                    payment_mode_changed_to = np.random.choice(mode)    
            else:
                payment_mode_changed_to = 'same mode'
            ride_tip = np.random.choice([0 ,10 , 20 , 50])
            # total_amount = ride_fare + ride_tip
            ride_cancelled_by_whom = 'NA'
            penalty = 0
            driver_rating = round(random.uniform(1, 5), 1)
            cancellation_reason = "NA"


        else:
            r_start_time = 'NA'
            dist_covered_km = 0 
            r_drop_location_changed = 'NA'
            r_drop_location_changed_to = 'NA' 
            extra_dist_covered_km = '0'
            ride_coupon_id = 'NA'
            ride_coupon_used = 'NA'
            total_dist_covered_km = '0'
            r_drop_time = 'NA'
            ride_fare = '0'
            payment_mode = 'NA'
            payment_method = 'NA'
            payment_status = 'NA'
            payment_mode_changed = 'NA'
            payment_mode_changed_to = 'NA'
            ride_tip = '0'
            # total_amount = '0'
            ride_cancelled_by_whom = np.random.choice(['D' , 'Pass'])
            # cancellation_reason = np.random.choice(['Changed Mood' , '' ])
            if ride_cancelled_by_whom == 'Pass':
                penalty = 20
                cancellation_reason = np.random.choice(['Changed Mood' , 'Driver Unreachable' , 'No cab available' , 'Driver asked' , 'Driver asking more money than the app says' ])

            else:
                penalty = 0
                cancellation_reason = np.random.choice(['Passenger Unreachable' , 'Bad Location' ])

            driver_rating = 'NA'

        record = {'r_id':r_id , 'r_pass_id': r_pass_id , 'r_d_id' : r_d_id , 
                  'r_booked_time':r_booked_time ,
                'r_status' : r_status , 'r_start_time' : r_start_time , 'r_pickup_location':r_pickup_location , 
                'r_drop_location' : r_drop_location, 'dist_covered_km' : dist_covered_km , 'r_drop_location_changed' : r_drop_location_changed , 
                'r_drop_location_changed_to' : r_drop_location_changed_to , 'extra_dist_covered_km': extra_dist_covered_km ,
                'total_dist_covered_km' : total_dist_covered_km , 'r_drop_time':r_drop_time ,'ride_fare' : ride_fare , 'ride_tip': ride_tip ,
                'ride_cancelled_by_whom': ride_cancelled_by_whom, 'cancellation_reason' : cancellation_reason , 
                'penalty':penalty ,  'payment_mode': payment_mode , 
                'payment_method' : payment_method  , 'payment_mode_changed' : payment_mode_changed ,
                'payment_mode_changed_to' : payment_mode_changed_to , 'payment_status' : payment_status , 'driver_rating':driver_rating
                #   'ride_coupon_id':ride_coupon_id , 'ride_coupon_used' : ride_coupon_used 
                    }
        ride_data.append(record)

        ride_df = pd.DataFrame(ride_data)

        ride_df['r_v_id'] = ride_df['r_d_id'].map(driver_df.set_index('d_id')['d_v_id'])
        ride_df['r_shift'] = pd.cut(ride_df['r_booked_time'].dt.hour,
                                bins=[0, 4, 8, 12, 16, 20, 24],
                                labels=['Mid Night' ,'Early Morning', 'Morning', 'Noon' , 'Evening' , 'Night' ],
                                right=False)
        ride_df['ride_fare'] = pd.to_numeric(ride_df['ride_fare'])
        ride_df['ride_tip'] = pd.to_numeric(ride_df['ride_tip'])
        ride_df['ride_surge_charge'] = np.where(ride_df['r_shift'] == 'Early Morning', 
                                                (ride_df['ride_fare'] / 4),
                                                np.where(ride_df['r_shift'] == 'Mid Night', 
                                                        (ride_df['ride_fare'] / 4),
                                                        0))
        ride_df['coupons_alloted'] = ride_df['r_pass_id'].map(coupon_bridge_table.set_index('c_pass_id')['c_ids_alloted'])
        ride_df['coupon_used'] = ride_df.apply(lambda row: 'No coupon alloted' if row['coupons_alloted'][0] == '-' or row['r_status'] == '0' else np.random.choice(bool_value), axis=1)

        ride_df['ride_coupon_id'] = ride_df.apply(lambda row: np.random.choice(row['coupons_alloted']) if row['coupon_used'] == 'Y' else 'No coupon_used', axis=1)

        ride_df['coupon_discount'] = ride_df['ride_coupon_id'].map(coupon_df.set_index('c_id')['c_discount%'])
        ride_df['coupon_discount'] = pd.to_numeric(ride_df['coupon_discount']).fillna(0)
        # ride_df['ride_fare'] = pd.to_numeric(ride_df['ride_fare'])
        ride_df['coupon_amount'] = (ride_df['ride_fare'] / 100) * ride_df['coupon_discount'].fillna(0)
        ride_df['coupon_amount'] = pd.to_numeric(ride_df['coupon_amount'])

        ride_df['final_amount'] = ride_df['ride_fare'] + ride_df['ride_surge_charge'] + ride_df['ride_tip'] - ride_df['coupon_amount']
        ride_df.round(2)
        ride_df.to_csv(f"fact_ride_table_{year}.csv", header=True, index=False)
    
    upload_to_azure("fact_ride_table", year, connection_string)

    return ride_df
    
r_ids_2020 = generate_r_ids(1  , 1 , ride_2020)
r_ids_2021 = generate_r_ids(3001, 1, ride_2021)
r_ids_2022 = generate_r_ids(8001, 1, ride_2022)
    
def generate_and_upload_feedback_data(year , f_records , ride_df , vehicle_df , r_ids , connection_string ):
    if year == 2020:
        f_ids = generate_feedback_ids(1 , 1 , f_records)
        # f_r_id = r_ids
        # f_v_id = v_ids
            
    elif year == 2021:
        f_ids = generate_feedback_ids(601 , 1 , f_records)
        # f_r_id = r_ids
        # f_v_id = v_ids

    elif year == 2022:
        f_ids = generate_feedback_ids(1401 , 1 , f_records)
        # f_r_id = r_ids
        # f_v_id = v_ids
        
    feed_df = pd.merge(vehicle_df, ride_df, left_on='v_id' , right_on='r_v_id')
    name_df = feed_df[['r_id' , 'v_id' , 'v_type' , 'r_status' ]]
    filtered_df = name_df[name_df['r_status'] == '1']
    
    feedback_data = []
    
    for feedback_id in f_ids:
        record = {'feedback_id' : feedback_id  }
        feedback_data.append(record)


        f_df = pd.DataFrame(feedback_data)
        test_df = pd.DataFrame({'r_id': r_ids})
        test_df['feedback_id'] = f_df['feedback_id']
        feedback_df = pd.merge(test_df , filtered_df , on = 'r_id' )
        bike = {
            'rate the use of helmet of your driver' ,
            'rate the friendliness of your driver'
        }

        other_vehicle = {
            'rate the cleanliness of your cab' ,
            'how was the drivers behavior'
        }


        feedback_df['questions'] = feedback_df.apply(lambda row: bike if row['v_type'] == 'Bike' else other_vehicle , axis =1)
        feedback_df = feedback_df.explode('questions')
        feedback_df['answers'] = np.random.randint(1, 5, size=len(feedback_df))
        feedback_df.drop('r_status', axis=1, inplace=True)
        
        feedback_df.to_csv(f"feedback_table_{year}.csv", header=True, index=False)

    upload_to_azure("feedback_table", year, connection_string)

    return feedback_df

def upload_to_azure(file_prefix, year, connection_string):
    container_name = "container name"
    folder_name = "uber_data"
    subfolder_name = None
    
    if "vehicle" in file_prefix:
        subfolder_name = "vehicle_data"
    elif "driver" in file_prefix:
        subfolder_name = "driver_data"
    elif "pass" in file_prefix:
        subfolder_name = "passenger_data"
    elif "coupon" in file_prefix:
        subfolder_name = "coupon_data"
    elif "c_bridge" in file_prefix:
        subfolder_name = "coupon_bridge_data"
    elif "ride" in file_prefix:
        subfolder_name = "ride_data"
    elif "feedback" in file_prefix:
        subfolder_name = "feedback_data"
    
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    # container_client.create_container()

    file_path = f"{file_prefix}_{year}.csv"
    blob_path = f"{folder_name}/{subfolder_name}/{file_path}"
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

    with open(file_path, "rb") as data:
        blob_client.upload_blob(data , overwrite = True)
        
# Set up Azure Storage connection string
connection_string = "connection string" 

if date == datetime.datetime.now().date():
    year = 2020
    v_df_2020 = generate_and_upload_vehicle_data(year, num_records_2020 , connection_string)
    d_df_2020 = generate_and_upload_driver_data(2020, num_records_2020, v_ids_2020 , connection_string) 
    p_df_2020 = generate_and_upload_passenger_data(2020 , pass_2020 , connection_string)
    c_df_2020 = generate_and_upload_coupon_data(2020 , c_2020 , pass_ids_2020 , connection_string)
    c_b_df_2020 = generate_and_upload_c_bridge_data(2020, pass_2020, pass_ids_2020 , c_df_2020 , connection_string)
    r_df_2020 = generate_and_upload_ride_data(2020 ,ride_2020, pass_ids_2020 , d_ids_2020 ,c_ids_2020 , c_df_2020 , c_b_df_2020 , d_df_2020 , connection_string )
    f_df_2020 = generate_and_upload_feedback_data(2020 , ride_2020 , r_df_2020 , v_df_2020 , r_ids_2020 , connection_string )

    
elif date == datetime.datetime.now().date() + datetime.timedelta(days=1):
    year = 2021
    v_df_2021 = generate_and_upload_vehicle_data(year, num_records_2021 , connection_string)
    d_df_2021 = generate_and_upload_driver_data(year, num_records_2021, v_ids_2021 , connection_string)
    p_df_2021 = generate_and_upload_passenger_data(year , pass_2021 , connection_string)
    c_df_2021 = generate_and_upload_coupon_data(year , c_2021 , pass_ids_2021 , connection_string)
    c_b_df_2021 = generate_and_upload_c_bridge_data(year, pass_2021, pass_ids_2021 , c_df_2021 , connection_string)
    r_df_2021 = generate_and_upload_ride_data(year ,ride_2021, pass_ids_2021 , d_ids_2021 ,c_ids_2021 , c_df_2021 , c_b_df_2021 , d_df_2021 , connection_string)
    f_df_2021 = generate_and_upload_feedback_data(year , ride_2021 , r_df_2021 , v_df_2021 , r_ids_2021, connection_string )
    
else:
    year = 2022
    v_df_2022 = generate_and_upload_vehicle_data(year, num_records_2022 , connection_string)
    d_df_2022 = generate_and_upload_driver_data(year, num_records_2022, v_ids_2022 , connection_string)
    p_df_2022 = generate_and_upload_passenger_data(year , pass_2022 , connection_string )
    c_df_2022 = generate_and_upload_coupon_data(year , c_2022 , pass_ids_2022 , connection_string)
    c_b_df_2022 = generate_and_upload_c_bridge_data(year, pass_2022, pass_ids_2022 , c_df_2022 , connection_string)
    r_df_2022 = generate_and_upload_ride_data(year ,ride_2022, pass_ids_2022 , d_ids_2022 ,c_ids_2022 , c_df_2022 , c_b_df_2022 , d_df_2022 , connection_string )
    f_df_2022 = generate_and_upload_feedback_data(year , ride_2022 , r_df_2022 , v_df_2022 , r_ids_2022 , connection_string )


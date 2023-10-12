create or replace procedure MAIN.usp_Insurance_Premium_Prediction()
    returns text
    language python
    runtime_version = 3.8
    packages =(
        'numpy==1.24.3',
        'pandas==1.5.3',
        'scikit-learn==1.3.0',
        'scikit-learn-extra==0.3.0',
        'snowflake-snowpark-python==*'
    )
    handler = 'main'
    EXECUTE AS CALLER
    as 'import numpy as np
import pandas as pd
from snowflake.snowpark import Row
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,expr,regexp_replace,cast,year,current_date,when
from sklearn.linear_model import LinearRegression
from snowflake.snowpark.types import StructType,StructField,StringType,DoubleType


def calculate_increase_percentage(speed, count):
    # Convert speed and count to integers
    speed = float(speed)
    count = int(count)

    ranges = {
        (0, 20): {20: 0.02, 40: 0.04, float(''inf''): 0.06},
        (20, 40): {20: 0.02, 40: 0.04, float(''inf''): 0.06},
        (40, 60): {20: 0.04, 40: 0.04, float(''inf''): 0.06},
        (60, float(''inf'')): {20: 0.06, 40: 0.06, float(''inf''): 0.06}
    }
    
    for speed_range, count_percentages in ranges.items():
        if speed_range[0] <= speed < speed_range[1]:
            for count_range, percentage in count_percentages.items():
                if count < count_range:
                    return percentage
    return 1.0
def calculate_seatbelt_adjustment(seatbelt_count):
    seatbelt_count=int(seatbelt_count)
    seatbelt_adjustments = {
        0: -0.01,
        20: 0.02,
        40: 0.04,
        60: 0.06,
        80: 0.08
    }
    for speed, adjustment in sorted(seatbelt_adjustments.items(), reverse=True):
        if seatbelt_count >= speed:
            return adjustment
    return list(seatbelt_adjustments.values())[-1]
def calculate_age_adjustment(age):
    age=int(age)
    age_ranges = {
      (0, 3): 0.01,
      (4, 4): 0.015,
      (5, 5): 0.02,
      (6, 6): 0.025,
      (7, 7): 0.03,
      (8, 8): 0.04,
      (9, 9): 0.05,
      (10, 10): 0.06,
      (11, 14): 0.075,
      (15, 20): 0.1,
      (21, 30): 0.15,
      (31, 40): 0.2,
      (41, float(''inf'')): 0.3
    }
    for age_range, adjustment in age_ranges.items():
        start, end = age_range
        if start <= age <= end:
          #print(adjustment)
          return adjustment
    return 0
def calculate_adjusted_price(base_price, adjustments):
    # Cast both inputs to float to ensure they are treated as floating-point numbers
    base_price = float(base_price)
    adjustments = float(adjustments)

    # Perform the calculation
    val = base_price * (1 + adjustments)
    return val
def calculate_discount_price(driving_score,price):
    discount_ranges = {
        (100, 90): 0.1,
        (90, 80): 0.05,
        (80, 75): 0.025
    }
    discount=0.0
    
    for key in discount_ranges:
        if key[1] <= driving_score <= key[0]:
            discount = discount_ranges[key]
            break 
    return price*discount

def main(session: snowpark.Session): 
    car_brief = ''PUBLIC.CAR_BRIEF''
    car_info=''PUBLIC.CAR_INFO''
    data=''MAIN.VEHICLE_TELEMATICS_GOLD''
    cb_df = session.table(car_brief)
    ci_df = session.table(car_info)
    data_df = session.table(data)
    cb_df = cb_df.withColumn("CAR_ID", cast(col("CAR_ID"), "INTEGER"))
    ci_df = ci_df.withColumn("MODEL_ID", cast(col("MODEL_ID"), "INTEGER"))
    data_df = data_df.withColumn("ID", cast(col("ID"), "INTEGER"))
    data_df = data_df.withColumn("ID", col("ID") % 45)
    data_df = data_df.withColumn("ID", when(col("ID") == 0, 45).otherwise(col("ID")))
    data_df.show()
    data_df = data_df.join(ci_df, data_df["ID"] == ci_df["MODEL_ID"])
    data_df=data_df=data_df.join(cb_df,ci_df["MODEL_ID"]==cb_df["CAR_ID"])
    filter_condition = (
    (col("ODO_DISTANCE") >= expr("CAST(SPLIT(RANGEINKM, ''-'')[0] AS INT)")) &
    (col("ODO_DISTANCE") < expr("CAST(SPLIT(RANGEINKM, ''-'')[1] AS INT)"))
    )
    data_df = data_df.filter(filter_condition)
    selected_columns = [''ID'',
                        ''VIN'',
                        ''ODO_DISTANCE'',
                        ''SPEED_HARDBRAKE'',
                        ''SPEED_HARDACCELERATION'',
                        ''NO_OF_TIMES_HARDBRAKE'',
                        ''NO_OF_TIMES_HARDACCELERATION'',
                        ''COUNT_SEATBELT_UNLATCHED'',
                        ''COUNT_INVALID_SPEED'',
                        ''CAR_MANUFACTURER'',
                        ''CAR_MODEL'',
                        ''MANUFACTURE_YEAR'',
                        ''RANGEINKM'',
                        ''BASIC_PRICE'']
    data_df = data_df.select(selected_columns)
    data_df = data_df.withColumn("BASIC_PRICE", cast(regexp_replace(col("BASIC_PRICE"), "\\\\$", ""), "FLOAT"))
    data_df = data_df.withColumn("AGE", year(current_date()) - col("MANUFACTURE_YEAR"))
    data_df = data_df.withColumn("SPEED_HARDACCELERATION",when(col("SPEED_HARDACCELERATION").isNull(), 0).otherwise(col("SPEED_HARDACCELERATION")))
    data_df = data_df.withColumn("SPEED_HARDBRAKE",when(col("SPEED_HARDBRAKE").isNull(), 0).otherwise(col("SPEED_HARDBRAKE")))
    events = [''HARDBRAKE'', ''HARDACCELERATION'']
    vin_adjustments = {}
    for event in events:
        columns = [''VIN'', f''SPEED_{event}'', f''NO_OF_TIMES_{event}'']
        event_df = data_df[columns]
        for row in event_df.to_local_iterator():
            x=row
            vin = x[0]
            count = x[2]
            speed = x[1]
            if vin not in vin_adjustments:
                vin_adjustments[vin] = 0
                vin_adjustments[vin] += calculate_increase_percentage(speed, count)
        columns = [''VIN'',''COUNT_SEATBELT_UNLATCHED'']
        event_df = data_df[columns]
        for row in event_df.to_local_iterator():
            x=row
            vin =x[0]
            count = x[1]
            if vin not in vin_adjustments:
                vin_adjustments[vin] = 0
            else:
                vin_adjustments[vin] += calculate_seatbelt_adjustment(count)
        for vin, adjustment in vin_adjustments.items():
            if adjustment == 0:
                vin_adjustments[vin] -= 0.05
        columns = [''VIN'',''AGE'']
        event_df = data_df[columns]
        for row in event_df.to_local_iterator():
            x=row
            vin = x[0]
            age = x[1]
            if vin not in vin_adjustments:
                vin_adjustments[vin] = 0
            else:
                vin_adjustments[vin] += calculate_age_adjustment(age)
        adjusted_prices = []
        columns = [''VIN'',''BASIC_PRICE'']
        event_df = data_df[columns]
        for row in event_df.to_local_iterator():
            x=row
            vin = x[0]
            base_price = x[1]
            adjustment = vin_adjustments.get(vin, 0)
            adjusted_price=0
            adjusted_price = calculate_adjusted_price(base_price, adjustment)
            adjusted_prices.append((vin, adjusted_price))
        new_df=session.create_dataframe(adjusted_prices,schema=StructType([StructField("VIN", StringType(), True), StructField("DYNAMIC_PRICE", DoubleType(), True)]))
        data_df=data_df.join(new_df, data_df["VIN"] == new_df["VIN"]).select(data_df["VIN"].alias("VIN"),data_df["SPEED_HARDACCELERATION"],data_df["SPEED_HARDBRAKE"],data_df["NO_OF_TIMES_HARDACCELERATION"],data_df["NO_OF_TIMES_HARDBRAKE"],data_df["COUNT_SEATBELT_UNLATCHED"],data_df["ODO_DISTANCE"],data_df["COUNT_INVALID_SPEED"],data_df["AGE"],data_df["CAR_MANUFACTURER"],data_df["CAR_MODEL"],data_df["MANUFACTURE_YEAR"],data_df["BASIC_PRICE"],new_df["DYNAMIC_PRICE"])
        columns_X = [''SPEED_HARDBRAKE'',''SPEED_HARDACCELERATION'',''NO_OF_TIMES_HARDBRAKE'',''NO_OF_TIMES_HARDACCELERATION'',''COUNT_SEATBELT_UNLATCHED'',''COUNT_INVALID_SPEED'',''ODO_DISTANCE'',''BASIC_PRICE'',''AGE'']
        df_X = data_df.select(columns_X).to_pandas()
        df_Y = data_df.select(col(''DYNAMIC_PRICE'')).to_pandas()
        test_x,test_y=df_X,df_Y
        model = LinearRegression()
        model.fit(test_x, test_y)
        test_predictions = model.predict(data_df.select(columns_X).to_pandas())
        df1 = pd.DataFrame(test_predictions, columns = [''PREMIUM_PRICE''])
        df2 = session.create_dataframe(df1)
        data_df = data_df.withColumn("index", expr("row_number() over (order by 1)"))
        df2 = df2.withColumn("index", expr("row_number() over (order by 1)"))
        data_df = data_df.join(df2, "index").drop("index")
        
    
     
    
        max_count_hardacceleration = data_df.agg(("NO_OF_TIMES_HARDACCELERATION","max"))
        max_count_hardbrake = data_df.agg(("NO_OF_TIMES_HARDBRAKE","max"))
        max_count_seatbelt_unlatched = data_df.agg(("COUNT_SEATBELT_UNLATCHED","max"))
    
     
        # Calculate the Driving_Score column
        data_df = data_df.withColumn(
            "Driving_Score",
            100 * (1 - col("NO_OF_TIMES_HARDACCELERATION") / max_count_hardacceleration.collect()[0][0]
                   + 1 - col("NO_OF_TIMES_HARDBRAKE") / max_count_hardbrake.collect()[0][0]
                   + 1 - col("COUNT_SEATBELT_UNLATCHED") / max_count_seatbelt_unlatched.collect()[0][0]
                    +1 - col("COUNT_INVALID_SPEED") / max_count_seatbelt_unlatched.collect()[0][0])/ 4)  
        
        data_df = data_df.withColumn(
            "FINAL_PREMIUM_PRICE",
            expr("CASE WHEN DRIVING_SCORE > 90 THEN PREMIUM_PRICE - (PREMIUM_PRICE * 0.1) "
                 "WHEN DRIVING_SCORE BETWEEN 80 AND 90 THEN PREMIUM_PRICE - (PREMIUM_PRICE * 0.05) "
                 "WHEN DRIVING_SCORE BETWEEN 75 AND 80 THEN PREMIUM_PRICE - (PREMIUM_PRICE * 0.025) "
                 "WHEN DRIVING_SCORE <50 THEN PREMIUM_PRICE + (PREMIUM_PRICE * 0.15)"
                 "WHEN DRIVING_SCORE BETWEEN 50 AND 60 THEN PREMIUM_PRICE + (PREMIUM_PRICE * 0.125)"
                 "WHEN DRIVING_SCORE BETWEEN 60 AND 70 THEN PREMIUM_PRICE + (PREMIUM_PRICE * 0.1)"
                 "WHEN DRIVING_SCORE BETWEEN 70 AND 75 THEN PREMIUM_PRICE + (PREMIUM_PRICE * 0.75)"
                 "ELSE PREMIUM_PRICE END")
                )
    
        select_columns = [''VIN'',
                                ''AGE'',
                                ''ODO_DISTANCE'',
                                ''CAR_MANUFACTURER'',
                                ''DRIVING_SCORE'',
                                ''BASIC_PRICE'',
                                ''PREMIUM_PRICE'',
                                ''FINAL_PREMIUM_PRICE'']
        final_df = data_df.select(select_columns)
        final_df.write.mode("overwrite").save_as_table("MAIN.INSURANCE_PREMIUM_PRICE")
    
        return ''Successful''




';


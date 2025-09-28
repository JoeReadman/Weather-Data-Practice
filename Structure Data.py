import boto3
import json
import pandas as pd
from datetime import datetime, date

#Setup
bucket_name = "weather-project-raw"
prefix_main = 'data/'
prefix_pollution = 'pollution_data/'

s3 = boto3.client("s3")

response_main = s3.list_objects_v2(Bucket = bucket_name, Prefix = prefix_main)
response_pollution = s3.list_objects_v2(Bucket = bucket_name, Prefix = prefix_pollution)

#List file names
main_data_files = [file["Key"] for file in response_main["Contents"]]
pollution_data_files = [file["Key"] for file in response_pollution["Contents"]]

#Section 1 - Creating main dataframes to upload to Power BI. Due to time stamps not aligning (weather is per 4 hours and pollution is per hour
# , we would need to consider forward filling or elliminating data via join.)

#Prepare lists for data frame concat
main_dfs = []
pollution_dfs = []

#Process each file body for main
for filepath in main_data_files:
    body = s3.get_object(Bucket = bucket_name, Key = filepath)["Body"].read().decode("utf-8")
    data = json.loads(body)

    #Create list of rows representing each 4 hour period to produce 1 data frame per file
    rows = []
    for period in data["list"]:

        rows.append(
        {
        "Date": datetime.fromtimestamp(period["dt"]),
        "City" : data["city"]["name"],
        "Type" : period["weather"][0]["main"],
        "Description" : period["weather"][0]["description"],
        "Temperature" : period["main"]["temp"],
        "Feels Like" : period["main"]["feels_like"],
        "Min Temp" : period["main"]["temp_min"],
        "Max Temp" : period["main"]["temp_max"],
        "Humidity" : period["main"]["humidity"],
        "Wind Speed" : period["wind"]["speed"],
        "Wind Deg" : period["wind"]["deg"],
        "Wind Gust" : period["wind"]["gust"],
        "Visibility" : period["visibility"]
        }
        )

    df = pd.DataFrame(rows)
    main_dfs.append(df)

#Concat to produce a master data frame including all files. Remove duplicate data (5 day forecast pulled every 4 days due to pollution data only covering 4 day period.)
full_main_df = pd.concat(main_dfs)
full_main_df = full_main_df.drop_duplicates(subset = ["Date", "City"])

#Process each file body for pollution
for filepath in pollution_data_files:
    body = s3.get_object(Bucket = bucket_name, Key = filepath)["Body"].read().decode("utf-8")
    data = json.loads(body)

    #City name not included in pollution file body. Needs constructing from file name
    city_construct = filepath.split("_")[2:][:-1]
    city = " ".join(city_construct)

    #Create list of rows representing each 1 hour period to produce 1 data frame per file
    rows = []
    for period in data["list"]:
        rows.append({
            "Date" : datetime.fromtimestamp(period["dt"]),
            "City" : city,
            "Air Quality Index" : period["main"]["aqi"],
            "Carbon Monoxide" : period["components"]["co"],
            "Nitric Oxide" : period["components"]["no"],
            "Nitrogen Oxide" : period["components"]["no2"],
            "Ozone" : period["components"]["o3"],
            "Sulfur Dioxide" : period["components"]["so2"],
            "Ammonia" : period["components"]["nh3"],
            "Particulate matter 2.5 micrograms" : period["components"]["pm2_5"],
            "Particulate matter 10 micrograms" : period["components"]["pm10"]
        })

    df = pd.DataFrame(rows)
    pollution_dfs.append(df)

#Concat to produce a master data frame including all files
full_pollution_df = pd.concat(pollution_dfs)

full_main_df = full_main_df.sort_values(by = ["City", "Date"], ascending = [True, True])
full_pollution_df = full_pollution_df.sort_values(by = ["City", "Date"], ascending = [True, True])

print("full_main_df and full_pollution_df produced successfully!")

#Section 2 - Interpolation of weather data to produce an hourly file. Cons: loss of accuracy. Pros: allows full use of pollution data set and greater volume of data.

interpolated_main = []

for city, group in full_main_df.groupby("City"):
    g = group.set_index("Date").resample("H").asfreq().interpolate(method = "linear")
    g["City"] = city
    interpolated_main.append(g)

full_main_interpolation = pd.concat(interpolated_main).reset_index()
full_main_interpolation[["Type","Description"]] = full_main_interpolation[["Type","Description"]].ffill()

#Save in working directory
today = date.today()
full_main_interpolation.to_csv(f"{today}_forecast_interpolation.csv", index=False)
full_pollution_df.to_csv(f"{today}_pollution.csv", index=False)
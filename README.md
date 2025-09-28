# Weather Data Practice
This project is intended as personal practice and demonstration of knowledge with **APIs**, **Python**, **AWS services**, **SQL** and **PowerBI**.

## Project Overview
The project involved creation of a basic ETL pipeline for hands on experience producing visuals from API data and the tools that can be used for each intermediate step. To minimise costs, only a small .json sample pulled through Open Weather Map's free API plan was used. A large part of the project was using weather data to produce visuals in Power BI and employ common practices that I might expect to use from day to day in a data analysis based role.

## Architecture

**Services & Tools Used:**
- **Python** – Data parsing, API calling, data cleaning/processing
- **AWS Lambda** – Automating data extraction and processing
- **Amazon Event Bridge** - Scheduling data extraction for every 4 days
- **Amazon S3** – Data storage and organisation
- **SQL Server** - Importing data on demand and final cleanup. Used as Power BI Source
- **Power BI** - Data visualisation practice

**Data Flow:**
1. **Ingestion**  
   - Data extracted from Open Weather Map's pollution and 4 day forecast API using Lambda/Event Bridge trigger
   - Uploaded to files to S3

2. **On Demand Data Pull**  
   - Run Structure Data.py to consolidate .csv files ready to import into SQL
   - Create interpolation to address issue with time interval difference between pollution and weather data for the purpose of this exercise
   - Will automatically iterate through all files in the target bucket

3. **SQL Import and Processing**  
   - Use SQL import wizard to import produced .csv files into WeatherData database
   - Join tables and complete final clean-up steps
   - Produce final table to be referenced by Power BI

4. **Power BI Practice**  
   - Intended to explore Power BI and demonstrate key skills with weather data
   - Initial normalisation of data and creation of a date table using DAX
   - Creating measures and columns based on the original data
   - Practice creating different types of visuals to communicate insights

## Challenges & Solutions

- **4 hour vs 1 hour period** – The pollution data available was recorded every hour. However, the forecast data was provided 4 hourly. This meant that extra care should be taken when observing relationships between the data.
  **Solution:** For the purpose of this exercise, weather data was interpolated for the extra hours to allow a much greater volume of data to practice with in Power BI. However, I appreciate that linear interpolation can have negative effects on the reliability of the data.

- **Actual weather data unavailable. Relied on forecast** - Forecast data was used instead of actual weather data as this was not available on the free plan.
  **Solution** - In practice this is a significant issue when trying to compare relationships between weather and pollution. We could not overlook this in real life but for this exercise, where our intention is to create a practice/dummy process, this was permitted.

## Skills Demonstrated
- Cloud data engineering (AWS Lambda, S3, Event Bridge)
- Python scripting for ETL processes
- Importing data and working with different data sources
- Data transformation (JSON to CSV)
- SQL server usage
- Workflow automation
- Creating visuals and employing common practices in Power BI

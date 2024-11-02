# Data Engineering Surf Report
This project aims to pull buoy data from the National Buoy Data Center run by the National Oceanic and Atmospheric Administration's API. 

### Pipeline Architecture
  1. Extract buoy data from text files gathered hourly from the NBDC API
  2. Transform new data into a csv file of daily buoy data
  3. Upload the daily buoy csv file into Amazon S3 bucket as a staging area
  4. Load the csv daily into a SQL database
  5. Use presentation and analysis tools to visualize and analyze historical buoy data
  6. 
### Current Status

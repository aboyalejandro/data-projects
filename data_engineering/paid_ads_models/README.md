# Modeling Facebook and Google Ads data with Fivetran, Snowflake and dbt
Get data at very high level of details: for Facebook Ads, get everything to see at an ad creative level. For Google Ads, get everything at search keyword level.

# Introduction & Goals
- Using Fivetran to get raw data into Snowflake
- Adding sources to dbt setup (simplified files, overall repo is not upload)
- Transformation data inside dbt and send it back to Snowflake

# Contents

- [Used Tools](#used-tools)
  - [Fivetran](#fivetran)
  - [dbdiagram.io](#dbdiagram)
  - [dbt & Snowflake](#dbt)
- [Follow Me On](#follow-me-on)

# Used Tools
- Fivetran for data ingestion
- dbdiagram.io for data modeling
- Snowflake for data warehousing
- dbt for transformation layer

![alt text](images/paid_ads_models_tools_png.png)

# Fivetran
- Understanding Google Ads ERD: https://fivetran.com/docs/applications/google-ads 
- Understandin Facebook Ads ERD: https://fivetran.com/docs/applications/facebook-ads
- Setting up neccesary reports for final model layout.

# dbdiagram

First, after contemplating Fivetran ERDs, I've designed the star models for both cases in dbdiagram.io

### Google Ads:
![alt text](images/google_ads_star_model.png)
Note: some column names might differ from the final dbt model.
### Facebook Ads:
![alt text](images/facebook_ads_star_model.png)
Note: some column names might differ from the final dbt model.

# dbt

Finally, after getting the diagram done, I've started modeling all the tables to get the final results. Both models have surrogate keys, incremental stategy with one-day batch window, and an example macro to extract the country from a campaign string. 

The sources for this raw data are listed in /sources folder and the /models are as shown below:

### Google Ads:
![alt text](images/google_ads_dbt.png)
Note: some column names might differ from the final dbt model.
### Facebook Ads:
![alt text](images/facebook_ads_dbt.png)
Note: some column names might differ from the final dbt model.

# Follow Me On
Linkedin: https://www.linkedin.com/in/alejandro-aboy/ 

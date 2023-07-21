# Extracting and storing weather data every 30 minutes with Mage, PostgreSQL, GCP and Data Studio
Scrape selected countries weather and send it to PostgreSQL and GCP with an easy to consume visualization in Data Studio.

# Introduction & Goals
- Setting up Mage pipeline to request for weather data every 30' minutes.
- Using Mage to get raw data into PostgreSQL and Google Cloud Storage 
- Visualizing data in Data Studio

# Contents

- [Used Tools](#used-tools)
  - [Mage](#mage)
  - [PostgreSQL](#postgreSQL)
  - [Google Cloud Storage](#gcp)
  - [Data Studio](#data_studio)
- [Follow Me On](#follow-me-on)

# Used Tools
- Mage for pipeline orchrestation and extraction.
- PostgreSQL as RDS.
- Google Cloud Storage as data lake.
- Data Studio as BI tool.

![alt text](images/tools.png)

# mage
- Setup data loader.
- Setup data transformer.
- Setup data exporter to PostgreSQL and GCP.

You can see all the pytho code in /mage

![alt text](images/mage_tree.png)

# postgreSQL

Data is added to PostgreSQL destination.

![alt text](images/postgresql.png)

# gcp

After creating service accounts and setting up the bucket, the parquets are added to Google Cloud Storage.

![alt text](images/gcp.png)

# data_studio

Finally, data is reported in data studio.

![alt text](images/data_studio.png)

# Follow Me On
Linkedin: https://www.linkedin.com/in/alejandro-aboy/ 

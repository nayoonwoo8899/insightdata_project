# insightdata_project
Real Time Venmo User Ananlytics
https://github.com/nayoonwoo8899/insightdata_project

# Table of Contents
1. [Motivation](README.md#motivation)
2. [Initial Data & Data Generation](README.md#initial-data-and-data-generation)
3. [Pipeline](README.md#pipeline)
4. [Github Repo Structure](README.md#github-repo-structure)

# Motivation

As a mobile payment service owned by PayPal, venmo allows users to make and share payment with other users who are connected.
With Venmo app, you can easily split the bill, cab fare, or much more. It is widely used especially among millennials and it forms part of the culture where the app's nomenclature is increasingly being used as a verb as in, "I'll venmo you the money when I get back to my dorm." [1]
Armed by its convenient feature (you can split dinner with 3 people in 10 seconds), as well as social network functionality, it gained massive popularity, reached $12 billion in transactions in the first quarter of 2018.[2]

On this project, I developed pipeline to provide platform to perform real time Venmo user analytics (gender, age range, location etc) that can be used by various researchers and marketing group as well as data scientist for their project.

[1] https://www.fool.com/investing/2017/03/22/why-venmo-is-so-popular-with-millennials.aspx

[2] Gagliordi, Natalie (April 26, 2018). "PayPal adds 8 million new active users in Q1". ZDNet


# Initial Data and Data Generation

Venmo provides public REST API that transmits transaction details in JSON format upon request. Unfortunately it has recently limited the maximum transactions per request to 50 and the maximum number of requests to be no more than a few times a minute. Therefore the consumer would not be able to access real-time streaming data at a sufficient rate via the REST API and the streaming input would have to be simulated from historical data instead.

Due to the privacy concern, Venmo doesn't release user information regarding age, gender, or location, so these fields are generated as a demonstration of the functionality too.

Initial Venmo transaction data (JSON format) is stored in AWS S3 bucket.

# Pipeline

<img width="742" alt="pipeline1" src="https://user-images.githubusercontent.com/41222469/45603086-76bd2200-b9f6-11e8-9384-c08469f05b04.png">
Spark is used for batch processing of the existing data and Spark streaming will provide real time analytics.
MySQL is used for table storage.


# Github Repo Structure

./src/ contains source code divided into batch and stream processing tasks.
./docs/ documentations.
./test/ contains automated test.




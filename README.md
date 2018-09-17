# insightdata_project
Real Time Venmo User Ananlytics
https://github.com/nayoonwoo8899/insightdata_project

1. Motivation

As a mobile payment service owned by PayPal, venmo allows users to make and share payment with other users who are connected.
With Venmo app, you can easily split the bill, cab fare, or much more. It is widely used especially among millennials and it forms part of the culture where the app's nomenclature is increasingly being used as a verb as in, "I'll venmo you the money when I get back to my dorm." [1]
Armed by its convenient feature (you can split dinner with 3 people in 10 seconds), as well as social network functionality, it gained massive popularity, reached $12 billion in transactions in the first quarter of 2018.[2]

On this project, I developed pipeline to provide platform to perform real time Venmo user analytics (gender, age range, location etc) that can be used by various researchers and marketing group as well as data scientist for their project.

[1] https://www.fool.com/investing/2017/03/22/why-venmo-is-so-popular-with-millennials.aspx

[2] Gagliordi, Natalie (April 26, 2018). "PayPal adds 8 million new active users in Q1". ZDNet


2. Initial Data & Data Generation

Since I was unable to have access to sufficient amount of streaming data via Venmo API per second, I generated streaming data.
Due to the privacy concern, Venmo doesn't release user information regarding age, gender, location, therefore, user information is generated too.
Initial Venmo transaction data (JSON format) is stored in AWS S3 bucket.

3. Pipeline

<img width="742" alt="pipeline1" src="https://user-images.githubusercontent.com/41222469/45603086-76bd2200-b9f6-11e8-9384-c08469f05b04.png">
Spark is used for batch processing of the existing data and Spark streaming will provide real time analytics.
MySQL is used for table storage.


4. Github Repo Structure




- Used AWS Glue to consume Data from internal MongoDB server. Work Flow is used to Schedule the differenet Stages of the process.
- AWS EventBridge is used to trigger a Refresh and start the process every 1 hour
- AWS s3 is used to store the Data during the raw, staging and final process.
![data pipeline](https://github.com/jaskeerat8/chatstat/assets/32131898/ccfac041-317a-47ec-9041-67593aef6d11)

- The final Data updates the Dashboard built using AWS Quicksight
![Parent Dashboard](https://github.com/jaskeerat8/Chatstat-Internship/assets/32131898/507ef76e-87d2-4a1a-98af-670dd55f3892)

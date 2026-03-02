document all the cross-verification / testing steps / methods at each stage of this cloud based pipeline.



I want the testing methods to ensure that each stage is executed correctly as required.



Stages / steps that I remember are as follows:

1. cloud run / cloud scheduler to ingest github data.

2. ensure the data is present in GCS.

3. Ensure that GCP sends alerts to snowflakes to fetch data into Raw schema.

4. ensure dbt is executed correctly at each stage.

5. ensure the looker dashboard updates accurately.



These are the steps in our pipeline I remember, additionally you can add if I have missed out anything.



I want smart testing methods to ensure each steps have been followed and are executes correctly. Mention these smart methods in the document.
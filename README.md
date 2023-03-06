# Goal
The goal is to develop an application that fetches [BitCoin close price in USD](https://messari.io/api/docs#operation/Get%20Asset%20timeseries) and the EUR/USD reference exchange rate from the [European Central Bank](https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/index.en.html).

# Application architecture
Data retrieval and processing are executed with Python and Pyspark (local with a single thread), executed on an EC2 instance.
Orchestration is made with [Dagster](https://dagster.io/).

# How to run the application
1. A virtual environment is used to isolate the further installed modules from the base environment. From the shell, run: `python3 -m venv cryptocurrencies-etl`.
2. Activate the venv: `source cryptocurrencies-etl/bin/activate`.
3. `cd` in the downloaded, unzipped repository. Install the required modules: `pip install -r requirements.txt`
4. Dagit is a web-based interface for viewing and interacting with Dagster objects. It is used to inspect op, job, and graph definitions, launch runs, view launched runs, and view assets produced by those runs. Launch Dagit as a background process. : `nohup dagit -f main.py -h 0.0.0.0 > dagit.log &`.
5. Orchestrate the jobs: `nohup dagster dev -f main.py -h 0.0.0.0 > dagster.log`.
# Kwenta Data
Scripts for retrieving data and uploading it to decentralized storage using Fleek.

## Environment

Copy the `.env.example` file in this directory to `.env` (which will be ignored by Git):

```bash
cp .env.example .env
```

Then, open `.env` and fill in the specified variables using your API keys.

## Scripts

The folder `./src/scripts` contains useful scripts for retrieving and summarizing data. Scripts are run using [Github Actions](https://github.com/Kwenta/kwenta-data/tree/main/.github/workflows) scheduled workflows. They can be monitored [here](https://github.com/Kwenta/kwenta-data/actions)

Utilities:
* IPFS upload

Currently running:
* Daily stats export

Deprecated:
* Trading competition scripts
* Leaderboard export script
* Optimism quest scripts

### IPFS Upload (js)
Reads all files in the `./data` folder and uploads them to the `data` folder on fleek. This is used as the final job during data workflows to sync data to Fleek buckets.

### Daily stats export (python)
Creates a daily stats data file and saves it locally. The file is read by Kwenta to produce the [stats page](https://kwenta.eth.limo/stats).
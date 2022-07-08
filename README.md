# Kwenta Data
Scripts for retrieving data and uploading it to decentralized storage using Fleek.

## Environment

Copy the `.env.example` file in this directory to `.env` (which will be ignored by Git):

```bash
cp .env.example .env
```

Then, open `.env` and fill in the specified variables using your API keys.

## Scripts

### Blocks (js)
Retrieves block numbers between two specified timestamps. The resulting data will be used as "checkpoints" for calculations like the leaderboard.

### Leaderboard (python)
Retrieves an ordered list of traders by their profit and loss during the period. The resulting data will be used as the leaderboard for futures trading competitions.

### Upload (js)
Uploads all data in the `data` folder to the default Fleek storage bucket for your credentails.

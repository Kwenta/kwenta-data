backfill:
	python ./src/pipelines/export_to_sqlite.py --config ./src/pipelines/configs/trades.json --backfill true
	python ./src/pipelines/export_to_sqlite.py --config ./src/pipelines/configs/transfers.json --backfill true
	python ./src/pipelines/export_to_sqlite.py --config ./src/pipelines/configs/funding_rates.json --backfill true
	python ./src/pipelines/export_to_sqlite.py --config ./src/pipelines/configs/positions.json --backfill true
	python ./src/pipelines/export_to_sqlite.py --config ./src/pipelines/configs/stats.json --backfill true
refresh:
	python ./src/pipelines/export_to_sqlite.py --config ./src/pipelines/configs/trades.json
	python ./src/pipelines/export_to_sqlite.py --config ./src/pipelines/configs/transfers.json
	python ./src/pipelines/export_to_sqlite.py --config ./src/pipelines/configs/funding_rates.json
	python ./src/pipelines/export_to_sqlite.py --config ./src/pipelines/configs/positions.json --backfill true
	python ./src/pipelines/export_to_sqlite.py --config ./src/pipelines/configs/stats.json --backfill true
backfill-debt:
	python ./src/pipelines/market_debt.py --config ./src/pipelines/configs/market_debt.json --backfill true --from 82845694 --to 82846694 --increment 100
debt:
	python ./src/pipelines/market_debt.py --config ./src/pipelines/configs/market_debt.json --from 52456507 --to 83751205 --increment 50000
monitor:
	python ./src/scripts/monitor.py

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
	python ./src/pipelines/market_debt.py --config ./src/pipelines/configs/market_debt.json --increment 25000
	python ./src/pipelines/export_to_sqlite.py --config ./src/pipelines/configs/positions.json --backfill true
	python ./src/pipelines/export_to_sqlite.py --config ./src/pipelines/configs/stats.json --backfill true
backfill-debt:
	python ./src/pipelines/market_debt.py --config ./src/pipelines/configs/market_debt.json --backfill true --from-block 72000000 --increment 100000
debt:
	python ./src/pipelines/market_debt.py --config ./src/pipelines/configs/market_debt.json --from-block 86218379 --increment 25000
monitor:
	python ./src/scripts/monitor.py

backfill:
	python ./src/pipelines/utils/export_to_sqlite.py --config ./src/pipelines/configs/trades.json --backfill true
	python ./src/pipelines/utils/export_to_sqlite.py --config ./src/pipelines/configs/transfers.json --backfill true
	python ./src/pipelines/utils/export_to_sqlite.py --config ./src/pipelines/configs/funding_rates.json --backfill true
	python ./src/pipelines/utils/export_to_sqlite.py --config ./src/pipelines/configs/positions.json --backfill true
	python ./src/pipelines/utils/export_to_sqlite.py --config ./src/pipelines/configs/stats.json --backfill true
refresh:
	python ./src/pipelines/utils/export_to_sqlite.py --config ./src/pipelines/configs/trades.json
	python ./src/pipelines/utils/export_to_sqlite.py --config ./src/pipelines/configs/transfers.json
	python ./src/pipelines/utils/export_to_sqlite.py --config ./src/pipelines/configs/funding_rates.json
	python ./src/pipelines/utils/export_to_sqlite.py --config ./src/pipelines/configs/positions.json --backfill true
	python ./src/pipelines/utils/export_to_sqlite.py --config ./src/pipelines/configs/stats.json --backfill true
monitor:
	python ./src/scripts/monitor.py

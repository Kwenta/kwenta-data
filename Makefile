run_pipelines:
	python ./src/pipelines/utils/export_to_sqlite.py --config ./src/pipelines/configs/trades.json
	python ./src/pipelines/utils/export_to_sqlite.py --config ./src/pipelines/configs/transfers.json
	python ./src/pipelines/utils/export_to_sqlite.py --config ./src/pipelines/configs/positions.json
	python ./src/pipelines/utils/export_to_sqlite.py --config ./src/pipelines/configs/funding_rates.json
	python ./src/pipelines/utils/export_to_sqlite.py --config ./src/pipelines/configs/stats.json
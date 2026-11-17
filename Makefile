.PHONY: all build up down test bench bench-quick plots report verify clean

all: test bench plots report

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down -v

test:
	python3 -m unittest discover -s tests -p "test_*.py" -v

bench:
	python3 scripts/run_experiments.py

bench-quick:
	python3 scripts/run_experiments.py --trials 2

plots:
	python3 scripts/generate_plots.py

report:
	cd report && bash build_report.sh

verify:
	python3 scripts/verify_submission.py

clean:
	rm -rf __pycache__ */__pycache__ */*/__pycache__
	rm -rf .pytest_cache .coverage htmlcov
	rm -rf wal_data data/run_logs data/run_logs_tmp_*
	cd report && rm -f main.aux main.bbl main.blg main.log main.out main.toc main.pdf

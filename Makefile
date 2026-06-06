# Makefile (at project root)
.PHONY: pipeline bronze-up silver-up gold-up clean help

help:
	@echo "Available commands:"
	@echo "  make pipeline   - Run full pipeline"
	@echo "  make bronze-up  - Run from bronze layer onwards"
	@echo "  make silver-up  - Run from silver layer onwards"
	@echo "  make gold-up    - Run only gold layer"
	@echo "  make clean      - Clean all data directories"

pipeline:
	@echo "Running full pipeline..."
	@python scripts/run_full_pipeline.py  

bronze-up:
	@echo "Running from bronze layer..."
	@python scripts/run_full_pipeline.py --from bronze 

silver-up:
	@echo "Running from silver layer..."
	@python scripts/run_full_pipeline.py --from silver  

gold-up:
	@echo "Running gold layer only..."
	@python scripts/run_full_pipeline.py --from gold 

clean:
	@echo "Cleaning data directories..."
	@rm -rf data/bronze data/silver data/gold
	@echo "✓ Data directories cleaned"

# Individual step runs (for debugging)
landing:
	@python scripts/step_01_album_release_landing_zone_ingestion.py

bronze:
	@python scripts/step_02_album_release_bronze_ingestion.py

silver:
	@python scripts/step_03_album_release_silver_creator.py

gold:
	@python scripts/step_04_album_release_gold_creator.py
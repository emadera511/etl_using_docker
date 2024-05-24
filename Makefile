up: 
	docker compose --env-file .env up --build -d 

etl: 
	docker exec etl python pipeline/main_pipeline.py

down: 
	docker compose down  
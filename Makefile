.PHONY: container

container:
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 529082709986.dkr.ecr.us-east-1.amazonaws.com
	docker build -t phl-courts-scraper-batch .
	docker tag phl-courts-scraper-batch:latest 529082709986.dkr.ecr.us-east-1.amazonaws.com/phl-courts-scraper-batch:latest
	docker push 529082709986.dkr.ecr.us-east-1.amazonaws.com/phl-courts-scraper-batch:latest

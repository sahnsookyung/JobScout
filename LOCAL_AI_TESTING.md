# Local AI Stack - Test Commands

## Build and Start Services

```bash
# Build and start all services
docker-compose up --build

# Or start in detached mode
docker-compose up --build -d
```

## Test Individual Services

### 1. Test Ollama Embeddings

```bash
# Wait for Ollama to be ready (check logs)
docker logs -f jobscout-ollama-1

# Test embedding generation (once ready)
curl -X POST http://localhost:11434/api/embed \
  -H "Content-Type: application/json" \
  -d '{
    "model": "nomic-embed-text",
    "input": "Software engineer with 5 years Python experience"
  }'

# Or using OpenAI-compatible API
curl -X POST http://localhost:11434/v1/embeddings \
  -H "Content-Type: application/json" \
  -d '{
    "model": "nomic-embed-text",
    "input": "Software engineer with 5 years Python experience"
  }'
```

### 2. Test GLiNER Entity Extraction

```bash
# Check GLiNER health  
curl http://localhost:8001/health

# Get default labels
curl http://localhost:8001/labels

# Test entity extraction
curl -X POST http://localhost:8001/extract \
  -H "Content-Type: application/json" \
  -d '{
    "text": "We are looking for a Senior Python Developer with 5+ years of experience in Django and React. Must have a Computer Science degree. Offering competitive salary and remote work options.",
    "labels": [
      "programming_language",
      "framework", 
      "experience_years",
      "education_degree",
      "benefit"
    ],
    "threshold": 0.3
  }'
```

### 3. Test Full ETL Pipeline

```bash
# Run the test script
docker-compose run main-driver python -m job_scout_hub.test_etl_real_data

# Check logs
docker logs -f jobscout-main-driver-1

# Verify database
docker-compose exec postgres psql -U user -d jobscout -c "
  SELECT COUNT(*) as total_jobs FROM job_post;
"

docker-compose exec postgres psql -U user -d jobscout -c "
  SELECT COUNT(*) as total_requirements FROM job_requirement_unit;
"

docker-compose exec postgres psql -U user -d jobscout -c "
  SELECT COUNT(*) as total_embeddings FROM job_requirement_unit_embedding;
"
```

## Expose Ollama Externally (Optional)

If you need to access Ollama from outside Docker:

```bash
# Start with expose profile
docker-compose --profile expose up --build -d

# Now you can test from host
curl http://localhost:11434/api/tags
```

## Troubleshooting

### Check Service Health

```bash
# All services
docker-compose ps

# Specific service logs
docker logs jobscout-ollama-1
docker logs jobscout-gliner-1
docker logs jobscout-main-driver-1
```

### Restart Services

```bash
# Restart specific service
docker-compose restart ollama
docker-compose restart gliner

# Full restart
docker-compose down
docker-compose up --build -d
```

### Clean Start (Remove Volumes)

```bash
# WARNING: This deletes all data including models
docker-compose down -v
docker-compose up --build
```

## Expected Results

- **Ollama**: Should pull and serve `nomic-embed-text` model (~768MB)
- **GLiNER**: Should load `urchade/gliner_mediumv2.1` model
- **ETL**: Should extract entities using GLiNER and generate embeddings using Ollama
- **Database**: Should contain jobs with requirement units and vector embeddings

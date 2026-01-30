# Local AI Stack - Quick Start Guide

## ✅ What's Been Set Up

### Services
1. **Ollama** (v0.15.2) - Embedding generation
   - Model: `qwen3-embedding:4b` (1024 dimensions)
   - Endpoint: `http://ollama:11434/v1/embeddings` (OpenAI-compatible)
   
2. **GLiNER** - Entity extraction
   - Model: `urchade/gliner_mediumv2.1`
   - Endpoint: `http://gliner:8001/extract`
   - Labels: 10 SRS-aligned types
   
3. **ETL Integration**
   - Uses GLiNER for extraction
   - Uses Ollama for embeddings
   - Stores in Postgres (Vector 1024)

## 🚀 Quick Start

### Build and Start
```bash
# Start all services
docker-compose up --build -d

# Watch logs
docker logs -f jobscout-ollama-1     # Watch model download
docker logs -f jobscout-gliner-1     # Watch GLiNER startup
docker logs -f jobscout-main-driver-1  # Watch ETL
```

### Test Commands

1. **Test Ollama Embeddings**
```bash
# Wait for model to download (~2.6GB), then test
curl -X POST http://localhost:11434/v1/embeddings \
  -H "Content-Type: application/json" \
  -d '{
    "model": "qwen3-embedding:4b",
    "input": "Software engineer position"
  }'
```

2. **Test GLiNER Extraction**
```bash
curl -X POST http://localhost:8001/extract \
  -H "Content-Type: application/json" \
  -d '{
    "text": "Looking for Senior Python Developer with 5+ years Django experience. Computer Science degree required. Remote work available.",
    "threshold": 0.3
  }'
```

3. **Verify Full Pipeline**
```bash
# Check database
docker-compose exec postgres psql -U user -d jobscout -c "
  SELECT 
    COUNT(*) as total_jobs,
    (SELECT COUNT(*) FROM job_requirement_unit) as requirements,
    (SELECT COUNT(*) FROM job_requirement_unit_embedding) as embeddings
  FROM job_post;
"
```

## 📋 Configuration

### Using Different Models

Edit `job_scout_hub/config.yaml`:

```yaml
etl:
  mock: false
  llm:
    # For OpenAI instead
    base_url: null
    api_key: "sk-..."
    extraction_type: "openai"
    
    # Or keep local
    base_url: "http://ollama:11434/v1"
    extraction_type: "gliner"
```

### Environment Variables

```bash
# In docker-compose.yml or .env
OLLAMA_IMAGE=0.15.2
OLLAMA_EMBEDDING_MODEL=qwen3-embedding:4b
GLINER_MODEL=urchade/gliner_mediumv2.1
```

## 🔍 Troubleshooting

### Ollama model not downloading
```bash
# Check logs
docker logs jobscout-ollama-1

# Manually pull
docker exec jobscout-ollama-1 ollama pull qwen3-embedding:4b
```

### GLiNER out of memory
```bash
# Reduce model size in docker-compose.yml
environment:
  - GLINER_MODEL=urchade/gliner_small
```

### Database dimension mismatch
```bash
# Recreate database with correct dimensions
docker-compose down -v
docker-compose up --build
```

## 📊 Model Info

| Model | Size | Dimensions | Use Case |
|-------|------|------------|----------|
| qwen3-embedding:4b | 2.6GB | 1024 | Semantic search |
| gliner_mediumv2.1 | 500MB | N/A | Entity extraction |

## 🎯 Next Steps

1. Run full ETL: `docker-compose up`
2. Check PgAdmin: http://localhost:5050
3. Query vectors using pgvector operators
4. Tune extraction threshold

## 📚 Documentation

- Full walkthrough: See `walkthrough.md` artifact
- Test commands: See `LOCAL_AI_TESTING.md`
- Implementation plan: See `implementation_plan.md` artifact

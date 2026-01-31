#!/bin/bash

# Test 1: Ollama embedding
echo "=== Testing Ollama Embedding ==="
curl -X POST http://localhost:11435/v1/embeddings \
  -H "Content-Type: application/json" \
  -d '{
    "model": "qwen3-embedding:4b",
    "input": "Senior Software Engineer with 5+ years Python experience"
  }' | python3 -m json.tool

echo -e "\n\n=== Testing Ollama Extraction (qwen3:14b) ==="
# Test 2: Ollama extraction
curl -X POST http://localhost:11435/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "qwen3:14b",
    "messages": [
      {
        "role": "system",
        "content": "You are a helpful assistant that extracts structured data from job descriptions."
      },
      {
        "role": "user",
        "content": "Extract job requirements from the following job description into the requested JSON format.\n\nDescription:\nWe are looking for a Senior Python Developer with 5+ years of experience in Django and React. Must have a Computer Science degree. Offering competitive salary and remote work options."
      }
    ],
    "format": "json",
    "stream": false
  }' | python3 -m json.tool

echo -e "\n\n=== Testing Ollama Health ==="
# Test 3: Ollama health
curl http://localhost:11435/api/tags | python3 -m json.tool

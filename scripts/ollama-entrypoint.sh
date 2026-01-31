#!/bin/bash
set -e

echo "Starting Ollama server..."
# Start server in background
ollama serve &
OLLAMA_PID=$!

# Give server a few seconds to initialize
echo "Waiting for Ollama to initialize..."
sleep 10

# Pull embedding model
EMBEDDING_MODEL="${OLLAMA_EMBEDDING_MODEL:-qwen3-embedding:4b}"
echo "Pulling embedding model: $EMBEDDING_MODEL"
ollama pull "$EMBEDDING_MODEL" || {
    echo "Warning: Failed to pull model $EMBEDDING_MODEL"
}

# Pull extraction model
EXTRACTION_MODEL="${OLLAMA_EXTRACTION_MODEL:-qwen3:14b}"
echo "Pulling extraction model: $EXTRACTION_MODEL"
ollama pull "$EXTRACTION_MODEL" || {
    echo "Warning: Failed to pull model $EXTRACTION_MODEL"
}

echo "Ollama setup complete!"
echo "Available models:"
ollama list || echo "Could not list models"

# Keep container running
wait $OLLAMA_PID

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
echo "Pulling embedding model: $EMBEDDING_MODEL (this may take several minutes)"
ollama pull "$EMBEDDING_MODEL" || {
    echo "Warning: Failed to pull model $EMBEDDING_MODEL"
    echo "You may need to pull it manually or check your internet connection"
}

echo "Ollama setup complete!"
echo "Available models:"
ollama list || echo "Could not list models"

# Keep container running
wait $OLLAMA_PID

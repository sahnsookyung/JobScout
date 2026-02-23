# Modal is a cloud GPU provider.
# The below hosts an openAI API compatible server on modal and is deployed through github actions.

import modal
import os

app = modal.App("qwen3-tei-server")

# Use the TEI image optimised for Ada Lovelace GPUs (SM 8.9) such as the L4
tei_image = modal.Image.from_registry(
    "ghcr.io/huggingface/text-embeddings-inference:89-1.5",
)

# Lightweight Python image used only for the one-time model download.
# The TEI image is a Rust binary with no Python, so we need a separate image here.
python_image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "huggingface_hub"
)

# Persistent volume to cache model weights across cold starts.
# Without this, the 8GB model would be re-downloaded every container boot.
volume = modal.Volume.from_name("qwen3-weights", create_if_missing=True)
HF_CACHE = "/data/hf-cache"
MODEL_ID = "Qwen/Qwen3-Embedding-4B"


@app.function(
    image=python_image,           # Python image, not TEI image
    volumes={"/data": volume},
    timeout=600,                    # allow enough time for the full model download
)
def download_model():
    """
    One-time setup function to pre-download model weights into the persistent volume.
    Run manually before first deploy with: modal run modal/modal_server.py::download_model

    This prevents the 8GB model from being re-downloaded on every cold start.
    Only needs to be re-run if you change the model.
    """
    from huggingface_hub import snapshot_download
    os.makedirs(HF_CACHE, exist_ok=True)
    # Downloads weights, tokenizer, and config — everything TEI needs
    snapshot_download(repo_id=MODEL_ID, cache_dir=HF_CACHE)
    # Explicitly flush writes to the volume before the container exits
    volume.commit()
    print("Model downloaded and committed to volume.")


@app.function(
    image=tei_image,
    gpu="L4",                       # 24GB VRAM, plenty for the ~8GB float16 model
    volumes={"/data": volume},      # mount the volume containing pre-downloaded weights
    scaledown_window=60,            # spin down after 1 min of inactivity to save cost (tradeoff: cold starts)
    timeout=600,                    # generous cap to cover cold start model load time
)
@modal.concurrent(max_inputs=100)  # allow up to 100 concurrent requests per container
@modal.web_server(8000, startup_timeout=300)
def serve():
    """
    Serverless TEI embedding server. Spins up on demand when called by the Docker container
    and shuts down after scaledown_window seconds of inactivity. The persistent volume means
    cold starts only need to load weights from disk, not re-download them.

    Endpoint URL is available in the Modal dashboard after deploying with:
        modal deploy modal/modal_server.py
    
    NOTE: After first deploy, immediately attach a Proxy Auth Token in the Modal dashboard
    to prevent unauthorized access to the endpoint.
    """
    import subprocess

    # Point HuggingFace to the volume cache so TEI loads weights from disk
    os.environ["HF_HOME"] = HF_CACHE

    subprocess.Popen([
        "text-embeddings-router",
        "--model-id", MODEL_ID,
        "--port", "8000",
        "--dtype", "float16",   # ~8GB VRAM, safe within L4's 24GB
        "--auto-truncate",      # silently truncate inputs that exceed the model's max sequence length
    ])
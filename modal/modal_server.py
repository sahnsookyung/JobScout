import modal
import os
import socket
import subprocess

app = modal.App("qwen3-tei-server")

GPU_CONFIG = "L4"
MODEL_ID = "Qwen/Qwen3-Embedding-4B"
HF_CACHE = "/data/hf-cache"
PORT = 8000

volume = modal.Volume.from_name("qwen3-weights", create_if_missing=True)

def download_model():
    from huggingface_hub import snapshot_download
    os.makedirs(HF_CACHE, exist_ok=True)
    snapshot_download(
        repo_id=MODEL_ID,
        cache_dir=HF_CACHE,  # must match HF_CACHE used in spawn_server
    )

def spawn_server() -> subprocess.Popen:
    process = subprocess.Popen([
        "text-embeddings-router",
        "--model-id", MODEL_ID,
        "--port", str(PORT),
        "--dtype", "float16",
        "--auto-truncate",
        "--huggingface-hub-cache", HF_CACHE,  # tell TEI exactly where to find weights
    ])
    # Poll until TEI accepts connections
    while True:
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=1).close()
            print("TEI ready!")
            return process
        except (socket.timeout, ConnectionRefusedError):
            retcode = process.poll()
            if retcode is not None:
                raise RuntimeError(f"TEI exited unexpectedly with code {retcode}")

# Separate image for downloading — Python + huggingface_hub only
download_image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("huggingface_hub")
)

# TEI image for serving — entrypoint cleared so Modal can bootstrap
tei_image = (
    modal.Image.from_registry(
        "ghcr.io/huggingface/text-embeddings-inference:89-1.5",
        add_python="3.11",
    )
    .dockerfile_commands("ENTRYPOINT []")
)

@app.function(
    image=download_image,
    volumes={"/data": volume},
    timeout=600,
)
def download_model_to_volume():
    """
    One-time setup to pre-download model weights into the persistent volume.
    Run manually with: modal run modal/modal_server.py::download_model_to_volume
    Only needs re-running if you change the model.
    """
    download_model()
    volume.commit()
    print("Model downloaded and committed to volume.")

@app.function(
    image=tei_image,
    gpu=GPU_CONFIG,
    volumes={"/data": volume},      # mount pre-downloaded weights
    scaledown_window=60,            # spin down after 1 min idle to save cost
    timeout=600,                    # generous cap to cover cold start load time
)
@modal.concurrent(max_inputs=100)
@modal.web_server(PORT, startup_timeout=300)
def serve():
    """
    Serverless TEI embedding server. Spins up on demand, shuts down after
    scaledown_window seconds of inactivity.

    Deploy with: modal deploy modal/modal_server.py
    NOTE: Attach a Proxy Auth Token in the Modal dashboard immediately after deploying.
    """
    spawn_server()
# Modal is a cloud GPU provider.
# Hosts an OpenAI API compatible embedding server, deployed via GitHub Actions.

import subprocess
import socket
import modal

app = modal.App("qwen3-tei-server")

MINUTES = 60
MODEL_ID = "Qwen/Qwen3-Embedding-4B"
MODEL_DIR = "/model"
PORT = 8000

volume = modal.Volume.from_name("qwen3-weights", create_if_missing=True)


def download_model():
    from huggingface_hub import snapshot_download
    snapshot_download(MODEL_ID, cache_dir=MODEL_DIR, requires_proxy_auth=True)


# TEI image optimised for Ada Lovelace GPUs (SM 8.9) such as the L4.
# - ENTRYPOINT cleared so Modal can bootstrap normally
# - HF_HOME set so TEI and huggingface_hub agree on cache location
# - Model downloaded during image build so weights are always present on cold start
tei_image = (
    modal.Image.from_registry(
        "ghcr.io/huggingface/text-embeddings-inference:89-1.9.1",
        add_python="3.11",
    )
    .dockerfile_commands("ENTRYPOINT []")
    .pip_install("huggingface-hub")
    .env({"HF_HOME": MODEL_DIR})
    .run_function(download_model, volumes={MODEL_DIR: volume})
)


def spawn_server() -> subprocess.Popen:
    """Starts TEI and blocks until it's ready to accept connections."""
    process = subprocess.Popen([
        "text-embeddings-router",
        "--model-id", MODEL_ID,
        "--port", str(PORT),
        "--dtype", "float16",       # ~8GB VRAM, safe within L4's 24GB
        "--auto-truncate",          # truncate inputs exceeding max sequence length
        "--huggingface-hub-cache", MODEL_DIR,  # must match HF_HOME above
    ])
    while True:
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=1).close()
            print("TEI ready!")
            return process
        except (socket.timeout, ConnectionRefusedError):
            retcode = process.poll()
            if retcode is not None:
                raise RuntimeError(f"TEI exited unexpectedly with code {retcode}")


@app.cls(
    image=tei_image,
    gpu="L4",                           # 24GB VRAM, plenty for the ~8GB float16 model
    volumes={MODEL_DIR: volume},        # mount volume containing pre-built weights
    scaledown_window=1 * MINUTES,       # spin down after 1 min idle to save cost
    timeout=10 * MINUTES,              # generous cap to cover cold start load time
)
@modal.concurrent(max_inputs=100)       # allow up to 100 concurrent requests per container
class TextEmbeddingsInference:
    @modal.enter()
    def open_connection(self):
        """Start TEI when the container boots."""
        self.process = spawn_server()

    @modal.exit()
    def terminate_connection(self):
        """Clean up TEI when the container shuts down."""
        self.process.terminate()

    @modal.web_server(PORT, startup_timeout=5 * MINUTES)
    def serve(self):
        """
        Exposes TEI as a web server on the container.
        Endpoint URL available in Modal dashboard after: modal deploy modal/modal_server.py
        NOTE: Attach a Proxy Auth Token in the Modal dashboard immediately after deploying.
        """
        pass
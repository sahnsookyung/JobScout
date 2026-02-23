# Modal is a cloud GPU provider.
# The below hosts an openAI API compatible server on modal and is deployed through github actions.

import os
import socket
import subprocess

import modal

app = modal.App("qwen3-tei-server")

MODEL_ID = "Qwen/Qwen3-Embedding-4B"
PORT = 8000
GPU_CONFIG = "L4"

LAUNCH_FLAGS = [
    "--model-id", MODEL_ID,
    "--port", str(PORT),
    "--dtype", "float16",   # ~8GB VRAM, safe within L4's 24GB
    "--auto-truncate",      # silently truncate inputs that exceed max sequence length
]


def spawn_server() -> subprocess.Popen:
    process = subprocess.Popen(["text-embeddings-router"] + LAUNCH_FLAGS)
    # Poll until TEI accepts connections before accepting traffic
    while True:
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=1).close()
            print("TEI server ready!")
            return process
        except (socket.timeout, ConnectionRefusedError):
            # If the process has exited something went wrong, fail fast
            retcode = process.poll()
            if retcode is not None:
                raise RuntimeError(f"TEI exited unexpectedly with code {retcode}")


def download_model():
    # Spawning the server triggers TEI to download model weights if not present,
    # then we immediately terminate it — weights are now baked into the image.
    spawn_server().terminate()


# Use the TEI image optimised for Ada Lovelace GPUs (SM 8.9) such as the L4.
# - ENTRYPOINT [] clears TEI's default entrypoint so Modal can bootstrap normally.
# - run_function bakes the model weights into the image at build time,
#   eliminating the need for a separate volume or manual download step.
tei_image = (
    modal.Image.from_registry(
        "ghcr.io/huggingface/text-embeddings-inference:89-1.5",
        add_python="3.11",
    )
    .dockerfile_commands("ENTRYPOINT []")
    .run_function(download_model, gpu=GPU_CONFIG)
)


@app.function(
    image=tei_image,
    gpu=GPU_CONFIG,
    scaledown_window=60,        # spin down after 1 min of inactivity to save cost
    timeout=600,                # generous cap to cover cold start model load time
)
@modal.concurrent(max_inputs=100)   # allow up to 100 concurrent requests per container
@modal.web_server(PORT, startup_timeout=300)
def serve():
    """
    Serverless TEI embedding server. Spins up on demand when called by the Docker container
    and shuts down after scaledown_window seconds of inactivity. Model weights are baked
    into the image so cold starts only need to load weights from disk.

    Endpoint URL is available in the Modal dashboard after deploying with:
        modal deploy modal/modal_server.py

    NOTE: After first deploy, immediately attach a Proxy Auth Token in the Modal dashboard
    to prevent unauthorized access to the endpoint.
    """
    spawn_server()
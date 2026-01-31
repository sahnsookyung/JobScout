This describes the logic behind LLM choices.
Several candidates were considered for both extraction and embedding. 

## Extraction
GLiNER is a specialized model for information extraction. It is much smaller than general-purpose LLMs and is specifically trained for this task. It is also open-source and can be run locally. We ultimately decided to use Qwen3-14B for extraction because it handles multiple languages including Asian ones with minimal effort whereas GLiNER is primarily trained on the Latin languages.

gemma3-27b was also considered a good candidate, but that would mean passing around licensing terms to the user which is a bit more difficult to do in containerized services. Therefore to reduce user friction we decided to go with Qwen3-14B.

## Embedding
Qwen3-Embedding-0.6B is a specialized model for information extraction. It is much smaller than general-purpose LLMs and is specifically trained for this task. It is also open-source and can be run locally.

Qwen3-Embedding-8B is a general-purpose LLM that is much larger than Qwen3-Embedding-0.6B. It has double the latency but much better performance on [MTEB benchmarks](https://huggingface.co/spaces/mteb/leaderboard). (Performance ratio of 0.6B to 8B is 70.58:64.34 when it comes to generalized tasks). Therefore we decided to go with Qwen3-Embedding-0.6B for embedding.
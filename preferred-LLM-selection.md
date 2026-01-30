This describes the logic behind LLM choices.
Several candidates were considered for both extraction and embedding. 

## Extraction
GLiNER is a specialized model for information extraction. It is much smaller than general-purpose LLMs and is specifically trained for this task. It is also open-source and can be run locally.

gemma3-27b is a general-purpose LLM that is much larger than GLiNER. It is not specialized for information extraction and is not as good as GLiNER for this task. However, it is a general-purpose LLM and can be used for other tasks as well. It's overkill because it's a VL-model and we can get a smaller model that's faster and more accurate.

## Embedding
Qwen3-Embedding-0.6B is a specialized model for information extraction. It is much smaller than general-purpose LLMs and is specifically trained for this task. It is also open-source and can be run locally.

Qwen3-Embedding-8B is a general-purpose LLM that is much larger than Qwen3-Embedding-0.6B. It has double the latency but much better performance on [MTEB benchmarks](https://huggingface.co/spaces/mteb/leaderboard). (Performance ratio of 0.6B to 8B is 70.58:64.34 when it comes to generalized tasks)

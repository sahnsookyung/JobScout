# TODO

## Add feature: Better resume parsing
Update code to work with non json as well. It must parse common formats like docx, pdf, yaml, txt too. Perhaps there is room to just read it as bytes and give it to an LLM but I'm unsure how well that would work for small models. May be way better to parse the text.

## Test the user-wants feature
This is meant to align better with the natural-language user specified "wants".

## Add feature: Resume generation
Talk to [Resume-Matcher](https://github.com/srbhr/Resume-Matcher) backend to generate resumes for different jobs.

## Fixed: status update on frontend matching pipeline
Currently the matching pipeline just shows one status, it should dynamically reload status updates.

## Remove legacy preferences use. 
It is superseded by user_wants that needs testing.

## Replace github page example with project demo?
Maybe a short gif?

## Design decision: Job Matching concern
Memory usage - Loading ALL jobs could use significant memory with large datasets (10k+ jobs). This is fine for now but may need pagination/batching later.

Performance - Cosine similarity on all jobs could be slow. Consider:
   - Adding a similarity threshold filter in SQL
   - Using approximate nearest neighbors (ANN) index
   - Caching results

# Add RAG for retrieval of relevant resume points that align with job description points
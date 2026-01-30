"""
GLiNER Entity Extraction Service

FastAPI service for extracting structured entities from job descriptions.
"""
import os
import logging
from typing import List, Dict, Any, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from gliner import GLiNER

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default labels based on SRS requirements
DEFAULT_LABELS = [
    "programming_language",
    "framework",
    "tool",
    "platform",
    "skill",
    "experience_years",
    "education_degree",
    "certification",
    "responsibility",
    "benefit",
    "salary",
    "location",
    "company_size",
    "industry"
]

# Global model instance
model: Optional[GLiNER] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load model on startup, cleanup on shutdown."""
    global model
    
    model_name = os.getenv("GLINER_MODEL", "urchade/gliner_mediumv2.1")
    logger.info(f"Loading GLiNER model: {model_name}")
    
    try:
        model = GLiNER.from_pretrained(model_name)
        logger.info("GLiNER model loaded successfully")
    except Exception as e:
        logger.error(f"Failed to load GLiNER model: {e}")
        raise
    
    yield
    
    # Cleanup
    logger.info("Shutting down GLiNER service")


app = FastAPI(
    title="GLiNER Entity Extraction Service",
    description="Extract structured entities from job descriptions",
    version="0.1.0",
    lifespan=lifespan
)


class ExtractionRequest(BaseModel):
    text: str = Field(..., description="Text to extract entities from")
    labels: Optional[List[str]] = Field(
        default=None,
        description="Entity labels to extract. Uses default job-related labels if not provided."
    )
    threshold: float = Field(
        default=0.3,
        ge=0.0,
        le=1.0,
        description="Confidence threshold for entity extraction"
    )


class Entity(BaseModel):
    text: str = Field(..., description="Extracted entity text")
    label: str = Field(..., description="Entity label/type")
    score: float = Field(..., description="Confidence score")
    start: int = Field(..., description="Start position in text")
    end: int = Field(..., description="End position in text")


class ExtractionResponse(BaseModel):
    entities: List[Entity] = Field(..., description="Extracted entities")
    labels_used: List[str] = Field(..., description="Labels used for extraction")


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    return {"status": "healthy", "model_loaded": True}


@app.post("/extract", response_model=ExtractionResponse)
async def extract_entities(request: ExtractionRequest):
    """
    Extract entities from text using GLiNER.
    
    Args:
        request: Extraction request with text and optional labels
        
    Returns:
        Extracted entities with confidence scores
    """
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    # Use provided labels or default
    labels = request.labels if request.labels else DEFAULT_LABELS
    
    try:
        # Perform entity extraction
        entities = model.predict_entities(
            request.text,
            labels,
            threshold=request.threshold
        )
        
        # Convert to response format
        extracted_entities = [
            Entity(
                text=ent["text"],
                label=ent["label"],
                score=float(ent["score"]),
                start=ent["start"],
                end=ent["end"]
            )
            for ent in entities
        ]
        
        logger.info(f"Extracted {len(extracted_entities)} entities from text (length: {len(request.text)})")
        
        return ExtractionResponse(
            entities=extracted_entities,
            labels_used=labels
        )
        
    except Exception as e:
        logger.error(f"Entity extraction failed: {e}")
        raise HTTPException(status_code=500, detail=f"Extraction failed: {str(e)}")


@app.get("/labels")
async def get_default_labels():
    """Get the default entity labels."""
    return {"labels": DEFAULT_LABELS}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)

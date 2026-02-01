# JobScout Web Dashboard - FastAPI

A modern, fast web application to view and analyze job matching results with automatic API documentation.

## Features

- 📊 **Overview Statistics** - See total matches, score distribution, and match quality
- 🔍 **Filterable Results** - Filter by minimum score and match status
- 📈 **Visual Scoring** - Color-coded match cards with score breakdowns
- 🔎 **Detailed View** - Click any match to see covered/missing requirements
- 📱 **Responsive Design** - Works on desktop and mobile
- 📚 **Auto API Docs** - Interactive Swagger UI at `/docs`
- ✅ **Type Safety** - Full request/response validation with Pydantic

## Quick Start

### 1. Install Dependencies

```bash
# Install FastAPI and dependencies
uv sync --group web

# Or using pip
pip install fastapi uvicorn pydantic
```

### 2. Run the Web App

```bash
# Navigate to web directory
cd web

# Run the FastAPI app with uvicorn
uv run python app.py

# Or with explicit database URL
DATABASE_URL="postgresql://user:pass@localhost:5432/jobscout" uv run python app.py
```

### 3. Open in Browser

- **Dashboard**: http://localhost:5000
- **API Documentation (Swagger UI)**: http://localhost:5000/docs
- **Alternative API Docs (ReDoc)**: http://localhost:5000/redoc

## API Documentation

FastAPI automatically generates interactive API documentation:

### Interactive Swagger UI

Visit http://localhost:5000/docs to:
- See all available endpoints
- Test API calls directly in the browser
- View request/response schemas
- Download OpenAPI specification

### Available Endpoints

#### GET /

Serves the main dashboard HTML page.

#### GET /api/matches

Get a list of job matches.

**Query Parameters:**
- `min_score` (float, optional): Minimum match score 0-100 (default: 0)
- `status` (string, optional): Match status - "active", "stale", or "all" (default: "active")
- `limit` (integer, optional): Maximum number of results 1-1000 (default: 50)

**Example:**
```bash
curl "http://localhost:5000/api/matches?min_score=70&status=active&limit=10"
```

**Response (Pydantic Models):**
```json
{
  "success": true,
  "count": 10,
  "matches": [
    {
      "match_id": "550e8400-e29b-41d4-a716-446655440000",
      "job_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
      "title": "Senior Python Developer",
      "company": "TechCorp",
      "location": "Remote",
      "is_remote": true,
      "overall_score": 85.5,
      "base_score": 95.0,
      "penalties": 9.5,
      "required_coverage": 0.9,
      "preferred_coverage": 0.8,
      "match_type": "with_preferences",
      "created_at": "2026-02-01T12:00:00",
      "calculated_at": "2026-02-01T12:00:00"
    }
  ]
}
```

#### GET /api/matches/{match_id}

Get detailed information about a specific match.

**Path Parameters:**
- `match_id` (string, required): UUID of the match

**Example:**
```bash
curl "http://localhost:5000/api/matches/550e8400-e29b-41d4-a716-446655440000"
```

**Response:**
```json
{
  "success": true,
  "match": {
    "match_id": "550e8400-e29b-41d4-a716-446655440000",
    "resume_fingerprint": "abc123...",
    "overall_score": 85.5,
    "base_score": 95.0,
    "penalties": 9.5,
    "required_coverage": 0.9,
    "preferred_coverage": 0.8,
    "total_requirements": 10,
    "matched_requirements_count": 8,
    "match_type": "with_preferences",
    "status": "active",
    "penalty_details": {
      "details": [...],
      "total": 9.5,
      "preferences_boost": 15.0
    }
  },
  "job": {
    "job_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
    "title": "Senior Python Developer",
    "company": "TechCorp",
    "location": "Remote",
    "description": "...",
    "salary_min": 80000,
    "salary_max": 120000,
    "currency": "USD",
    "min_years_experience": 5,
    "requires_degree": true
  },
  "requirements": [
    {
      "requirement_id": "uuid-here",
      "evidence_text": "Built Python microservices",
      "evidence_section": "Experience",
      "similarity_score": 0.85,
      "is_covered": true,
      "req_type": "required"
    }
  ]
}
```

#### GET /api/stats

Get overall statistics about matches.

**Example:**
```bash
curl "http://localhost:5000/api/stats"
```

**Response:**
```json
{
  "success": true,
  "stats": {
    "total_matches": 150,
    "active_matches": 142,
    "score_distribution": {
      "excellent": 25,
      "good": 45,
      "average": 52,
      "poor": 20
    }
  }
}
```

## Configuration

### Database Connection

The web app connects to the same database as the main application. Set the `DATABASE_URL` environment variable:

```bash
# Default (from config.yaml)
export DATABASE_URL="postgresql://user:password@localhost:5432/jobscout"

# Or use a different database
export DATABASE_URL="postgresql://user:pass@remote-server:5432/jobscout_prod"
```

### Development Mode

For development, the app runs with auto-reload enabled. This provides:
- Auto-reload on code changes
- Detailed error pages with stack traces

To disable in production:

```python
# In app.py, change the last line:
uvicorn.run(app, host="0.0.0.0", port=5000, reload=False)
```

## Pydantic Models

FastAPI uses Pydantic models for automatic validation and documentation:

### MatchSummary
- `match_id`: UUID string
- `job_id`: UUID string (optional)
- `title`: Job title
- `company`: Company name
- `location`: Location text (optional)
- `is_remote`: Boolean (optional)
- `overall_score`: Float 0-100
- `base_score`: Float 0-100
- `penalties`: Float >= 0
- `required_coverage`: Float 0-1
- `preferred_coverage`: Float 0-1
- `match_type`: String (e.g., "with_preferences")
- `created_at`: ISO timestamp (optional)
- `calculated_at`: ISO timestamp (optional)

### MatchDetail
Extended match information including:
- `resume_fingerprint`: String
- `total_requirements`: Integer
- `matched_requirements_count`: Integer
- `status`: String (e.g., "active", "stale")
- `penalty_details`: Dictionary

### JobDetails
Job posting information:
- `job_id`: UUID (optional)
- `title`, `company`, `location`: Strings (optional)
- `is_remote`: Boolean (optional)
- `description`: String (optional)
- `salary_min`, `salary_max`: Floats (optional)
- `currency`: String (optional)
- `min_years_experience`: Integer (optional)
- `requires_degree`, `security_clearance`: Booleans (optional)
- `job_level`: String (optional)

### RequirementDetail
Requirement match details:
- `requirement_id`: UUID string
- `evidence_text`: String (optional)
- `evidence_section`: String (optional)
- `similarity_score`: Float 0-1
- `is_covered`: Boolean
- `req_type`: String (e.g., "required", "preferred")

## Customization

### Styling

The dashboard uses vanilla CSS with a modern gradient design. To customize:

1. Edit `web/templates/index.html`
2. Modify the `<style>` section
3. Restart the app

### Adding New Endpoints

To add new API endpoints:

1. Edit `web/app.py`
2. Add new Pydantic models if needed
3. Add new route function with type hints
4. Access the interactive docs to test

Example:

```python
from pydantic import BaseModel

class SearchQuery(BaseModel):
    query: str
    filters: Optional[Dict[str, Any]] = None

@app.post("/api/jobs/search", response_model=SearchResponse)
def search_jobs(query: SearchQuery):
    # ... search logic
    return SearchResponse(results=[...])
```

## Deployment

### Production with Uvicorn

For production use Uvicorn directly:

```bash
# Install gunicorn with uvicorn workers
pip install gunicorn

# Run with gunicorn
gunicorn web.app:app -w 4 -k uvicorn.workers.UvicornWorker -b 0.0.0.0:5000
```

### Docker Deployment

Create a `Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

EXPOSE 5000

CMD ["uvicorn", "web.app:app", "--host", "0.0.0.0", "--port", "5000"]
```

### Environment Variables

Set these for production:

```bash
export DATABASE_URL="your-production-db-url"
export UVICORN_WORKERS=4
export UVICORN_HOST="0.0.0.0"
export UVICORN_PORT=5000
```

## Troubleshooting

### "No matches found" message

If you see "No matches found":
1. Make sure you've run the matching pipeline: `python main.py`
2. Check that matches exist in the database
3. Verify the database connection URL is correct
4. Try lowering the minimum score filter

### Database connection errors

If you get database errors:
```bash
# Test the connection
python -c "from sqlalchemy import create_engine; e = create_engine('your-db-url'); e.connect()"

# Verify database has matches
psql $DATABASE_URL -c "SELECT COUNT(*) FROM job_match;"
```

### Port already in use

If port 5000 is taken:
```bash
# Run on a different port
uv run python app.py --port 5001

# Or modify the last line in app.py:
uvicorn.run(app, host="0.0.0.0", port=5001)
```

## FastAPI vs Flask Comparison

**Why FastAPI for this project:**

| Feature | FastAPI | Flask |
|---------|---------|-------|
| **Auto API Docs** | ✅ Built-in Swagger & ReDoc | ❌ Manual setup |
| **Type Validation** | ✅ Pydantic models | ❌ Manual validation |
| **Performance** | ✅ Async support | ❌ Sync only |
| **Modern** | ✅ ASGI standard | ⚠️ WSGI |
| **Editor Support** | ✅ Excellent | ⚠️ Good |
| **Learning Curve** | ⚠️ Steeper | ✅ Simple |
| **Maturity** | ⚠️ Newer (2018) | ✅ Mature |

**When to use FastAPI:**
- Building APIs with documentation needs
- Want automatic validation
- Need async performance
- Building microservices

**When to use Flask:**
- Simple single-page apps
- Team already knows Flask
- Need specific Flask extensions
- Legacy codebase

## Next Steps

Possible enhancements:
- [ ] Export results to CSV/Excel
- [ ] Email alerts for high-scoring matches
- [ ] Compare multiple resumes
- [ ] Filter by company, location, salary
- [ ] Time-series charts for match trends
- [ ] User authentication with FastAPI security
- [ ] WebSocket for real-time updates
- [ ] Background tasks with Celery
- [ ] Caching with Redis

## Resources

- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Pydantic Documentation](https://docs.pydantic.dev/)
- [Uvicorn Documentation](https://www.uvicorn.org/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)

## Support

For issues or questions:
1. Check the main README.md
2. Review the API documentation at `/docs`
3. Check FastAPI docs: https://fastapi.tiangolo.com/
4. Open an issue on GitHub

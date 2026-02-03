import unittest
from database.models import JobPost, JobRequirementUnit, JobRequirementUnitEmbedding

class TestModels(unittest.TestCase):
    
    def test_job_post_instantiation(self):
        job = JobPost(
            title="Software Engineer",
            company="Acme Corp",
            location_text="Remote",
            canonical_fingerprint="hash123"
        )
        self.assertEqual(job.title, "Software Engineer")
        self.assertEqual(job.company, "Acme Corp")
        
    def test_job_requirement_unit_instantiation(self):
        req = JobRequirementUnit(
            req_type="required",
            text="Must have Python",
            tags={"skill": "python"}
        )
        self.assertEqual(req.req_type, "required")
        self.assertEqual(req.text, "Must have Python")
        self.assertEqual(req.tags, {"skill": "python"})

if __name__ == "__main__":
    unittest.main()

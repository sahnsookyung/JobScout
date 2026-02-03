import unittest
from unittest.mock import MagicMock
import json
from etl.orchestrator import JobETLOrchestrator
from core.llm.openai_service import OpenAIService
from database.repository import JobRepository

class TestETLRefactor(unittest.TestCase):
    def setUp(self):
        self.mock_repo = MagicMock(spec=JobRepository)
        self.mock_ai = MagicMock(spec=OpenAIService)
        self.orchestrator = JobETLOrchestrator(repo=self.mock_repo, ai_service=self.mock_ai)

    def test_extract_structured_data_service(self):
        # Test OpenAIService independently
        service = OpenAIService(api_key="dummy")
        service.client = MagicMock()
        
        # Mock response content
        mock_response_content = json.dumps({
            "min_years_experience": 5,
            "requires_degree": True,
            "security_clearance": False,
            "requirements": [
                {
                    "req_type": "required",
                    "text": "Must have Python experience",
                    "skills": ["python"]
                }
            ],
            "thought_process": "Analysis here"
        })
        
        # Setup the mock
        mock_message = MagicMock()
        mock_message.content = mock_response_content
        mock_choice = MagicMock()
        mock_choice.message = mock_message
        mock_response = MagicMock()
        mock_response.choices = [mock_choice]
        
        service.client.chat.completions.create.return_value = mock_response

        # Run extraction
        result = service.extract_structured_data("Sample description", {"properties": {}})

        # Verify structural fields
        self.assertEqual(result['min_years_experience'], 5)
        self.assertEqual(result['requires_degree'], True)
        self.assertEqual(result['thought_process'], "Analysis here")
        
        # Verify call arguments
        call_kwargs = service.client.chat.completions.create.call_args[1]
        self.assertIn('response_format', call_kwargs)
        self.assertEqual(call_kwargs['response_format']['type'], 'json_schema')

    def test_orchestrator_flow(self):
        # Test Orchestrator flow
        job_data = {"title": "Engineer", "company_name": "Tech Corp", "location": "Remote"}
        
        # Setup repo mock returns
        self.mock_repo.get_by_fingerprint.return_value = None
        mock_post = MagicMock()
        mock_post.id = "123"
        self.mock_repo.create_job_post.return_value = mock_post
        
        self.orchestrator.process_incoming_job(job_data, "site_x")
        
        # Verify repo calls
        self.mock_repo.create_job_post.assert_called_once()
        self.mock_repo.get_or_create_source.assert_called_once_with("123", "site_x", job_data)
        self.mock_repo.save_job_content.assert_called_once_with("123", job_data)

if __name__ == '__main__':
    unittest.main()

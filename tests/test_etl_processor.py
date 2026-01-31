import unittest
from unittest.mock import MagicMock, patch
import json
from job_scout_hub.etl.etl import ETLProcessor

class TestETLRefactor(unittest.TestCase):
    def setUp(self):
        self.mock_db = MagicMock()
        self.etl = ETLProcessor(db=self.mock_db, llm_config={"extraction_type": "ollama", "api_key": "dummy"})

    def test_extract_requirements_ollama_schema(self):
        # Mock the OpenAI client
        self.etl.openai_client = MagicMock()
        
        # Mock response content - now includes structural fields
        mock_response_content = json.dumps({
            "min_years_experience": 5,
            "requires_degree": True,
            "security_clearance": False,
            "requirements": [
                {
                    "req_type": "required",
                    "text": "Must have Python experience",
                    "skills": ["python"]
                },
                {
                    "req_type": "responsibility",
                    "text": "Develop backend services",
                    "skills": ["backend"]
                }
            ]
        })
        
        # Setup the mock return value
        mock_message = MagicMock()
        mock_message.content = mock_response_content
        mock_choice = MagicMock()
        mock_choice.message = mock_message
        mock_response = MagicMock()
        mock_response.choices = [mock_choice]
        
        self.etl.openai_client.chat.completions.create.return_value = mock_response

        # Run extraction - now returns a dict
        result = self.etl.extract_requirements_openai("Sample description")

        # Verify structural fields
        self.assertEqual(result['min_years_experience'], 5)
        self.assertEqual(result['requires_degree'], True)
        self.assertEqual(result['security_clearance'], False)
        
        # Verify requirements list
        requirements = result['requirements']
        self.assertEqual(len(requirements), 2)
        self.assertEqual(requirements[0]['req_type'], 'required')
        self.assertEqual(requirements[0]['text'], 'Must have Python experience')
        self.assertEqual(requirements[0]['ordinal'], 0)
        self.assertEqual(requirements[1]['req_type'], 'responsibility')
        self.assertEqual(requirements[1]['ordinal'], 1)

        # Verify call arguments to ensure schema was passed
        call_kwargs = self.etl.openai_client.chat.completions.create.call_args[1]
        self.assertIn('response_format', call_kwargs)
        self.assertEqual(call_kwargs['response_format']['type'], 'json_schema')
        self.assertEqual(call_kwargs['response_format']['json_schema']['name'], 'extraction_response')
        self.assertEqual(call_kwargs['model'], 'qwen3:14b')

    def test_config_override(self):
        # Test that we can override the model via config
        custom_config = {
            "extraction_type": "ollama",
            "extraction_model": "llama3:8b",
            "api_key": "dummy"
        }
        etl = ETLProcessor(db=self.mock_db, llm_config=custom_config)
        self.assertEqual(etl.extraction_model, "llama3:8b")

        # Test fallback when model is missing for ollama
        default_config = {
            "extraction_type": "ollama",
            "api_key": "dummy"
        }
        etl_default = ETLProcessor(db=self.mock_db, llm_config=default_config)
        self.assertEqual(etl_default.extraction_model, "qwen3:14b")

if __name__ == '__main__':
    unittest.main()

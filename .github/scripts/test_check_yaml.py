import unittest
from pathlib import Path
import yaml
from check_yaml import parse

class StrictYamlTest(unittest.TestCase):
    def test_nested_duplicate(self):
        with self.assertRaises(yaml.YAMLError):
            parse('jobs:\n  test:\n    services: {pg: 1}\n    services: {pg: 2}\n')
    def test_duplicate_flow_mapping(self):
        with self.assertRaises(yaml.YAMLError):
            parse('env: {USER: one, USER: two}')
    def test_syntax_error(self):
        with self.assertRaises(yaml.YAMLError):
            parse('env: [')
    def test_workflow_trigger_remains_string(self):
        value = parse('on: {push: {}}\nflag: true')[0]
        self.assertIn('on', value)
        self.assertIs(value['flag'], True)
    def test_cli_database_credentials_match(self):
        workflow = parse((Path(__file__).parents[1] / 'workflows/cli.yml').read_text())[0]
        jobs = workflow['jobs']
        from urllib.parse import urlparse
        checked = set()
        for name, job in jobs.items():
            databases = [service['env'] for service in job.get('services', {}).values()
                         if 'POSTGRES_PASSWORD' in service.get('env', {})]
            environments = [job.get('env', {})] + [step.get('env', {}) for step in job.get('steps', [])]
            for environment in environments:
                for key, url in environment.items():
                    if key not in ('NEUTRON_TEST_DATABASE_URL', 'NEUTRON_E2E_DATABASE_URL'): continue
                    self.assertEqual(len(databases), 1, name)
                    parsed = urlparse(url)
                    service = databases[0]
                    self.assertEqual(parsed.username, service['POSTGRES_USER'], (name,key))
                    self.assertEqual(parsed.password, service['POSTGRES_PASSWORD'], (name,key))
                    self.assertEqual(parsed.path.lstrip('/'), service['POSTGRES_DB'], (name,key))
                    self.assertIn(parsed.hostname, ('localhost','127.0.0.1'))
                    self.assertEqual(parsed.port or 5432,5432)
                    checked.add((name,key))
        self.assertEqual(checked, {('test','NEUTRON_TEST_DATABASE_URL'),
            ('test','NEUTRON_E2E_DATABASE_URL'),('studio-browser','NEUTRON_TEST_DATABASE_URL')})
    def test_valid_scoped_keys(self):
        self.assertEqual(len(parse('a: {pg: 1}\nb: {pg: 2}')), 1)

if __name__ == '__main__':
    unittest.main()

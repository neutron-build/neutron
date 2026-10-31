"""Reject ambiguous YAML before any workflow/schema consumer sees it."""
from pathlib import Path
import sys
import re
import yaml

class StrictLoader(yaml.SafeLoader):
    pass

# GitHub workflows use YAML 1.2 Boolean spellings. Keep the trigger key `on`
# as a string instead of PyYAML's default YAML 1.1 Boolean coercion.
StrictLoader.yaml_implicit_resolvers = {
    key: [rule for rule in rules if rule[0] != 'tag:yaml.org,2002:bool']
    for key, rules in yaml.SafeLoader.yaml_implicit_resolvers.items()
}
StrictLoader.add_implicit_resolver('tag:yaml.org,2002:bool',
    re.compile(r'^(?:true|false)$', re.IGNORECASE), list('tTfF'))

def unique_mapping(loader, node, deep=False):
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in result:
            raise yaml.constructor.ConstructorError(
                'mapping', node.start_mark, f'duplicate key: {key!r}', key_node.start_mark)
        result[key] = loader.construct_object(value_node, deep=deep)
    return result

StrictLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, unique_mapping)

def parse(text):
    return list(yaml.load_all(text, Loader=StrictLoader))

if __name__ == '__main__':
    root = Path(__file__).resolve().parents[2]
    files = sorted((root / '.github/workflows').glob('*.y*ml'))
    if not files:
        sys.exit('No workflows found')
    for path in files:
        try:
            parse(path.read_text())
        except (yaml.YAMLError, ValueError, TypeError) as error:
            sys.exit(f'{path}: {error}')
    print(f'Strict YAML: {len(files)} workflows parsed')

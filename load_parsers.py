import os
import importlib.util
from parsers_registry import get_parser_for_file

PARSERS_DIR = "parsers"

def load_all_parsers():
    parser_map = {}
    for filename in os.listdir(PARSERS_DIR):
        if filename.endswith(".py"):
            path = os.path.join(PARSERS_DIR, filename)
            module_name = filename[:-3]
            spec = importlib.util.spec_from_file_location(module_name, path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            parser_map[module_name] = module.parse
    return parser_map

def get_parser(file_name):
    parser_name = get_parser_for_file(file_name)
    if not parser_name:
        return None
    parser_map = load_all_parsers()
    return parser_map.get(parser_name)

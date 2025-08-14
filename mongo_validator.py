import yaml
from typing import Dict, Any, List
from pymongo import MongoClient

class MongoValidator:
    def __init__(self, connection_string: str = None, database: str = None):
        self.rules = self._load_rules()
        if connection_string and database:
            self.client = MongoClient(connection_string)
            self.db = self.client[database]
        else:
            connection_string = self.rules['database_connection']['uri']
            database = self.rules['database_connection']['database']
            self.client = MongoClient(connection_string)
            self.db = self.client[database]

    def _load_rules(self) -> Dict:
        with open('config.yaml', 'r') as file:
            return yaml.safe_load(file)

    def _get_nested_value(self, doc: Dict, field_path: str) -> Any:
        parts = field_path.split('.')
        value = doc
        for part in parts:
            if not isinstance(value, dict) or part not in value:
                return None
            value = value[part]
        return value

    def check_collection_existence(self, collection_name: str):
        if collection_name not in self.db.list_collection_names():
            raise AssertionError(f"Collection '{collection_name}' does not exist in the database.")

    def validate_expected_count(self, collection_name: str, expected_count: int):
        actual_count = self.db[collection_name].count_documents({})
        if actual_count < expected_count:
            raise AssertionError(
                f"Collection '{collection_name}': Expected at least {expected_count} documents, but found {actual_count}."
            )

    def validate_required_fields(self, collection_name: str, required_fields: List[str]):
        errors = []
        sample_size = 1000
        sample = self.db[collection_name].find().limit(sample_size)
        for doc in sample:
            doc_id = str(doc.get('_id', 'Unknown'))
            for field in required_fields:
                if self._get_nested_value(doc, field) is None:
                    errors.append(f"Document {doc_id}: Missing required field '{field}'.")
        if errors:
            raise AssertionError("\n".join(errors))

    def validate_data_types(self, collection_name: str, data_types: Dict[str, str]):
        errors = []
        sample_size = 1000
        sample = self.db[collection_name].find().limit(sample_size)
        type_mapping = {
            'string': str, 'int': int, 'float': float, 'bool': bool, 'list': list, 'dict': dict
        }
        for doc in sample:
            doc_id = str(doc.get('_id', 'Unknown'))
            for field, expected_type in data_types.items():
                value = self._get_nested_value(doc, field)
                if value is not None and not isinstance(value, type_mapping.get(expected_type)):
                    errors.append(
                        f"Document {doc_id}: Invalid data type for field '{field}'. "
                        f"Expected {expected_type}, but found {type(value).__name__}."
                    )
        if errors:
            raise AssertionError("\n".join(errors))

    def validate_categories(self, collection_name: str, categories: Dict[str, List[Any]]):
        errors = []
        sample_size = 1000
        sample = self.db[collection_name].find().limit(sample_size)
        for doc in sample:
            doc_id = str(doc.get('_id', 'Unknown'))
            for field, allowed_values in categories.items():
                value = self._get_nested_value(doc, field)
                if value is not None and value not in allowed_values:
                    errors.append(
                        f"Document {doc_id}: Invalid category in field '{field}'. "
                        f"Found '{value}', expected one of {allowed_values}."
                    )
        if errors:
            raise AssertionError("\n".join(errors))

    def validate_numeric_ranges(self, collection_name: str, numeric_ranges: Dict[str, Dict[str, float]]):
        errors = []
        sample_size = 1000
        sample = self.db[collection_name].find().limit(sample_size)
        for doc in sample:
            doc_id = str(doc.get('_id', 'Unknown'))
            for field, range_rules in numeric_ranges.items():
                value = self._get_nested_value(doc, field)
                if value is not None:
                    if not isinstance(value, (int, float)):
                        errors.append(f"Document {doc_id}: Field '{field}' is not numeric.")
                        continue
                    min_val = range_rules.get('min', float('-inf'))
                    max_val = range_rules.get('max', float('inf'))
                    if not (min_val <= value <= max_val):
                        errors.append(
                            f"Document {doc_id}: Value in field '{field}' ({value}) is outside the "
                            f"expected range [{min_val}, {max_val}]."
                        )
        if errors:
            raise AssertionError("\n".join(errors))

    def validate_keywords(self, collection_name: str, keywords: Dict[str, List[str]]):
        errors = []
        sample_size = 1000
        sample = self.db[collection_name].find().limit(sample_size)
        for doc in sample:
            doc_id = str(doc.get('_id', 'Unknown'))
            for field, expected_keywords in keywords.items():
                value = self._get_nested_value(doc, field)
                if value is not None:
                    value_str = str(value)
                    for keyword in expected_keywords:
                        if keyword not in value_str:
                            errors.append(
                                f"Document {doc_id}: Expected keyword '{keyword}' not found in field '{field}'."
                            )
        if errors:
            raise AssertionError("\n".join(errors))
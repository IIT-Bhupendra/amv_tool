import yaml
from typing import Dict, Any, List
from pymongo import MongoClient
from datetime import datetime

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
        self.validation_results = []

    def _load_rules(self) -> Dict:
        with open('config.yaml', 'r') as file:
            return yaml.safe_load(file)

    def _get_nested_value(self, doc: Dict, field_path: str) -> Any:
        """Get value from nested field using dot notation"""
        parts = field_path.split('.')
        value = doc
        for part in parts:
            if not isinstance(value, dict) or part not in value:
                return None
            value = value[part]
        return value

    def _validate_categories(self, doc: Dict, doc_id: str, rules: Dict, errors: List[str]):
        """Validate categorical fields"""
        for field, allowed_values in rules.get('categories', {}).items():
            value = self._get_nested_value(doc, field)
            if value is not None and value not in allowed_values:
                errors.append(
                    f"Document {doc_id}: Invalid category in {field}. "
                    f"Found '{{value}}', expected one of {allowed_values}"
                )

    def _validate_numeric_ranges(self, doc: Dict, doc_id: str, rules: Dict, errors: List[str]):
        """Validate numeric ranges"""
        for field, range_rules in rules.get('numeric_ranges', {}).items():
            value = self._get_nested_value(doc, field)
            if value is not None:
                if not isinstance(value, (int, float)):
                    errors.append(f"Document {doc_id}: Field {field} is not numeric")
                    continue
                
                if value < range_rules.get('min', float('-inf')):
                    errors.append(
                        f"Document {doc_id}: Value in {field} ({{value}}) is below "
                        f"minimum {range_rules['min']}"
                    )
                if value > range_rules.get('max', float('inf')):
                    errors.append(
                        f"Document {doc_id}: Value in {field} ({{value}}) is above "
                        f"maximum {range_rules['max']}"
                    )

    def _validate_keywords(self, doc: Dict, doc_id: str, rules: Dict, errors: List[str]):
        """Validate keyword presence"""
        for field, expected_keywords in rules.get('keywords', {}).items():
            value = self._get_nested_value(doc, field)
            if value is not None:
                value_str = str(value)
                for keyword in expected_keywords:
                    if keyword not in value_str:
                        errors.append(
                            f"Document {doc_id}: Expected keyword '{{keyword}}' "
                            f"not found in field {field}"
                        )

    def _validate_field_existence(self, doc: Dict, field_path: str) -> bool:
        """Check if a field exists in document, handling nested fields"""
        parts = field_path.split('.')
        current = doc
        for part in parts:
            if not isinstance(current, dict) or part not in current:
                return False
            current = current[part]
        return True

    def _validate_data_type(self, value: Any, expected_type: str) -> bool:
        """Validate data type of a field"""
        type_mapping = {
            'string': str,
            'int': int,
            'float': float,
            'bool': bool,
            'list': list,
            'dict': dict
        }
        return isinstance(value, type_mapping.get(expected_type))

    def validate_collection(self, collection_name: str, rules: Dict) -> Dict[str, Any]:
        """Validate a single collection against all rules"""
        collection = self.db[collection_name]
        errors = []
        
        # Check collection existence
        if collection_name not in self.db.list_collection_names():
            errors.append(f"Collection {collection_name} does not exist")
            return {"collection": collection_name, "status": "Failed", "errors": errors}
        
        # Check document count
        actual_count = collection.count_documents({})
        if actual_count < rules.get('expected_count', 0):
            errors.append(f"Expected {rules['expected_count']} documents, found {actual_count}")
        
        # Sample documents for detailed validation
        sample_size = min(1000, actual_count)
        sample = collection.find().limit(sample_size)
        
        for doc in sample:
            doc_id = str(doc.get('_id', 'Unknown'))
            
            # Validate required fields and data types
            for field in rules.get('required_fields', []):
                if not self._validate_field_existence(doc, field):
                    errors.append(f"Document {doc_id}: Missing required field {field}")
            
            # Validate data types
            for field, expected_type in rules.get('data_types', {}).items():
                value = self._get_nested_value(doc, field)
                if value is not None and not self._validate_data_type(value, expected_type):
                    errors.append(f"Document {doc_id}: Invalid type for {field}")
            
            # Validate categories, ranges, and keywords
            self._validate_categories(doc, doc_id, rules, errors)
            self._validate_numeric_ranges(doc, doc_id, rules, errors)
            self._validate_keywords(doc, doc_id, rules, errors)
        
        return {
            "collection": collection_name,
            "status": "Failed" if errors else "Passed",
            "errors": errors,
            "sample_size": sample_size,
            "total_documents": actual_count
        }

    def run_validations(self) -> Dict[str, Any]:
        """Run all validations and generate report"""
        start_time = datetime.now()
        results = []
        
        for collection_name, rules in self.rules['collections'].items():
            print(f"Validating collection: {collection_name}")
            result = self.validate_collection(collection_name, rules)
            results.append(result)
        
        summary = {
            "execution_time": str(datetime.now() - start_time),
            "total_collections": len(results),
            "passed_collections": sum(1 for r in results if r['status'] == 'Passed'),
            "failed_collections": sum(1 for r in results if r['status'] == 'Failed'),
            "results": results
        }
        
        return summary

if __name__ == "__main__":
    validator = MongoValidator()
    results = validator.run_validations()
    print(f"\nValidation Results:")
    print(f"Execution Time: {results['execution_time']}")
    for result in results['results']:
        print(f"\nCollection: {result['collection']}")
        print(f"Status: {result['status']}")
        if result['errors']:
            print("Errors:")
            for error in result['errors']:
                print(f"  - {error}")

import pytest
from mongo_validator import MongoValidator
import json
from datetime import datetime

def test_mongo_validations():
    """
    Pytest function to run validations and generate HTML report
    """
    validator = MongoValidator()
    
    results = validator.run_validations()
    
    # Save detailed results to JSON file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f'validation_results_{timestamp}.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    # Assert for pytest to generate report
    failed_collections = [r for r in results['results'] if r['status'] == 'Failed']
    if failed_collections:
        error_messages = []
        for collection in failed_collections:
            error_messages.extend(collection['errors'])
        pytest.fail("\n".join(error_messages))

if __name__ == "__main__":
    pytest.main([__file__, '--html=validation_report.html', '--self-contained-html'])
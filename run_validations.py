import pytest
from mongo_validator import MongoValidator

# Create a single validator instance to be used by all tests
validator = MongoValidator()
rules = validator.rules

# A list to hold all the test cases
test_cases = []

# Generate test cases for each collection and rule
for collection_name, collection_rules in rules.get('collections', {}).items():
    # Test for collection existence
    test_cases.append(
        pytest.param(
            collection_name,
            'check_collection_existence',
            (),
            id=f"test_{collection_name}_existence"
        )
    )

    # Test for expected count
    if 'expected_count' in collection_rules:
        test_cases.append(
            pytest.param(
                collection_name,
                'validate_expected_count',
                (collection_rules['expected_count'],),
                id=f"test_{collection_name}_expected_count"
            )
        )

    # Test for required fields
    if 'required_fields' in collection_rules:
        test_cases.append(
            pytest.param(
                collection_name,
                'validate_required_fields',
                (collection_rules['required_fields'],),
                id=f"test_{collection_name}_required_fields"
            )
        )

    # Test for data types
    if 'data_types' in collection_rules:
        test_cases.append(
            pytest.param(
                collection_name,
                'validate_data_types',
                (collection_rules['data_types'],),
                id=f"test_{collection_name}_data_types"
            )
        )

    # Test for categories
    if 'categories' in collection_rules:
        test_cases.append(
            pytest.param(
                collection_name,
                'validate_categories',
                (collection_rules['categories'],),
                id=f"test_{collection_name}_categories"
            )
        )

    # Test for numeric ranges
    if 'numeric_ranges' in collection_rules:
        test_cases.append(
            pytest.param(
                collection_name,
                'validate_numeric_ranges',
                (collection_rules['numeric_ranges'],),
                id=f"test_{collection_name}_numeric_ranges"
            )
        )

    # Test for keywords
    if 'keywords' in collection_rules:
        test_cases.append(
            pytest.param(
                collection_name,
                'validate_keywords',
                (collection_rules['keywords'],),
                id=f"test_{collection_name}_keywords"
            )
        )

# The main test function, parametrized with all the test cases
@pytest.mark.parametrize("collection_name,method_name,args", test_cases)
def test_validation(collection_name, method_name, args):
    """Dynamically runs all validation tests."""
    validation_method = getattr(validator, method_name)
    validation_method(collection_name, *args)

if __name__ == "__main__":
    pytest.main(['-v', __file__, '--html=validation_report.html', '--self-contained-html'])
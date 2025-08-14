Build a tool
- to perform consisitency checks on mongo collection

Consistency checks:
- check whether collection exists in database
- check whether collection is loaded with expected records/documents
- checks all the required fields are present and not-null
- check data types of all fields (may be nested fields within arrays or dict or arrays of dict, etc)
- check whether categorical fields have unintended category values
- check whether numerical fields having out of expected range values
- check whether certain keyworks like $, % are present, if expected in the fields like currency_amts, or percentages

Be careful
- collections are massive
- contains approximately 20Million records

- We've 8 collections to validate
WE need to create a centralized way to run/execute the tool
so, that it runs all the fields in sequence/series.
And also it should generate html reports using pytest-html

current process we are following is completely manual and runs once monthly.
let's automate this completely.

In order to keep this tool reusable by anyone
I want to keep all the conditions to check within a collection in a single yaml file
Someone - will specify conditions to check for each collection separately in this single file
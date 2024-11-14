CREATE OR REPLACE {{ language_alias }} SET SCRIPT "AAF_QUERY_HANDLER_UDF"(...)
EMITS  (outputs VARCHAR(2000000)) AS

{{ script_content }}

/
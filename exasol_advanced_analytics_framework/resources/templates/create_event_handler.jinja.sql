CREATE OR REPLACE {{ language_alias }} SCALAR SCRIPT "AAF_EVENT_HANDLER_UDF"(...)
EMITS  (outputs VARCHAR(2000000)) AS

{{ script_content }}

/
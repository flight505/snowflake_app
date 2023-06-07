import snowflake.connector

account = "qpgirsc-cr13281"
user = "FLIGHT505"
password = ""
warehouse = "COMPUTE_WH"
database = "Nordic_HDM_and_Nopho_database"
schema = "PUBLIC"
role = "ACCOUNTADMIN"

# Establish a connection
ctx = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema,
    role=role
)

query_id_columns = f"""
SELECT TABLE_NAME, COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = '{schema}' AND COLUMN_NAME LIKE '%ID%';
"""

cursor = ctx.cursor()
cursor.execute(query_id_columns)
rows = cursor.fetchall()

id_columns = {}
for row in rows:
    table_name = row[0]
    column_name = row[1]
    if table_name not in id_columns:
        id_columns[table_name] = []
    id_columns[table_name].append(column_name)

relationships = []
for table, columns in id_columns.items():
    for column in columns:
        for other_table, other_columns in id_columns.items():
            if other_table != table and column in other_columns:
                relationships.append((table, column, other_table))

# Generate Mermaid syntax
mermaid_syntax = "graph TD\n"
for rel in relationships:
    mermaid_syntax += f"{rel[0]}-->|{rel[1]}|{rel[2]}\n"

print(mermaid_syntax)

cursor.close()
ctx.close()
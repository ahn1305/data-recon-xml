import xml.etree.ElementTree as ET
import pandas as pd
import re

# -------------------------------
# CONFIG
# -------------------------------
XML_PATH = "ds_job.xml"
EXCEL_PATH = "lineage.xlsx"
OUTPUT_PATH = "final_recon_output.xlsx"

EDGES_SHEET = "edges"

# -------------------------------
# STEP 1: PARSE XML
# -------------------------------
tree = ET.parse(XML_PATH)
root = tree.getroot()

xml_rows = []

for col in root.findall(".//Collection[@Name='Columns']/SubRecord"):
    col_name = None
    source_col = None
    derivation = None
    table_def = None

    for prop in col.findall("Property"):
        name = prop.attrib.get("Name")

        if name == "Name":
            col_name = prop.text
        elif name == "SourceColumn":
            source_col = prop.text
        elif name == "Derivation":
            derivation = prop.text
        elif name == "TableDef":
            table_def = prop.text

    if col_name:
        xml_rows.append({
            "column_name": col_name.strip(),
            "source_column": source_col,
            "derivation": derivation,
            "target_table": table_def
        })

df_xml = pd.DataFrame(xml_rows)

# Clean source_column (extract stage.col)
def clean_source(x):
    if pd.isna(x):
        return None
    return x.strip()

df_xml["source_column"] = df_xml["source_column"].apply(clean_source)

# -------------------------------
# STEP 2: LOAD EXCEL (EDGES)
# -------------------------------
df_edges = pd.read_excel(EXCEL_PATH, sheet_name=EDGES_SHEET)

# Normalize column names
df_edges.columns = [c.strip().lower() for c in df_edges.columns]

# Clean relevant fields
df_edges["source stage.col"] = df_edges["source stage.col"].astype(str).str.strip()
df_edges["target col"] = df_edges["target col"].astype(str).str.strip()

# -------------------------------
# STEP 3: NORMALIZE KEYS
# -------------------------------
# Extract column name from stage.col
def extract_col(x):
    if "." in x:
        return x.split(".")[-1]
    return x

df_edges["column_name"] = df_edges["target col"].apply(extract_col)
df_edges["source_column_clean"] = df_edges["source stage.col"]

# -------------------------------
# STEP 4: RECONCILIATION
# -------------------------------
final_rows = []

for _, row in df_edges.iterrows():
    col = row["column_name"]
    src = row["source_column_clean"]
    job = row.get("job_file", "unknown")

    # Try match in XML
    match = df_xml[
        (df_xml["column_name"] == col)
    ]

    if not match.empty:
        target_table = match.iloc[0]["target_table"]

        comment = "MATCHED"
        if row.get("confidence", "").lower() != "high":
            comment += " | Low confidence in Excel"

    else:
        target_table = None
        comment = "MISSING IN XML"

    # Extra validation
    if pd.notna(row.get("resolution")):
        comment += f" | Resolution: {row['resolution']}"

    final_rows.append({
        "source_file": job,
        "column_name": col,
        "target_table": target_table,
        "comments": comment
    })

df_final = pd.DataFrame(final_rows)

# -------------------------------
# STEP 5: AGGREGATION (dedupe)
# -------------------------------
df_final = df_final.groupby(
    ["source_file", "column_name", "target_table"],
    as_index=False
).agg({
    "comments": lambda x: " | ".join(set(x))
})

# -------------------------------
# STEP 6: OUTPUT
# -------------------------------
df_final.to_excel(OUTPUT_PATH, index=False)

print("✅ Final Recon Output Generated:", OUTPUT_PATH)

import xml.etree.ElementTree as ET
import pandas as pd
import os

# -------------------------------
# CONFIG
# -------------------------------
XML_PATH = "../jCidDW036_3215_CID_ACCT_PROD_Extract.xml"
EXCEL_PATH = "../dataviz1.xlsx"
OUTPUT_PATH = "final_recon_output.xlsx"

EDGES_SHEET = "Edges"

JOB_NAME = os.path.basename(XML_PATH)  # match with excel job_file

# -------------------------------
# STEP 1: PARSE XML (ONLY TRUE SOURCE COLS)
# -------------------------------
tree = ET.parse(XML_PATH)
root = tree.getroot()

xml_source_cols = set()

def is_real_source(col):
    """
    Filter out intermediate lineage
    """
    if not col:
        return False

    col = col.strip()

    # ❌ remove intermediate links
    if col.startswith("To_") or col.startswith("From_"):
        return False

    # ❌ remove transformer/internal refs
    if "Trfm" in col or "Lookup" in col:
        return False

    return True


for col in root.findall(".//Collection[@Name='Columns']/SubRecord"):
    for prop in col.findall("Property"):
        if prop.attrib.get("Name") == "SourceColumn":
            source_col = prop.text

            if is_real_source(source_col):
                xml_source_cols.add(source_col.strip())

df_xml = pd.DataFrame({"source_stage_col": list(xml_source_cols)})

print(f"✅ XML TRUE SOURCE columns: {len(df_xml)}")

# -------------------------------
# STEP 2: LOAD EXCEL + FILTER JOB
# -------------------------------
df_edges = pd.read_excel(EXCEL_PATH, sheet_name=EDGES_SHEET)
df_edges.columns = [c.strip().lower() for c in df_edges.columns]

# normalize job file column
df_edges["job_file"] = df_edges["job_file"].astype(str).str.strip()

# ✅ FILTER ONLY THIS JOB
df_edges = df_edges[df_edges["job_file"].str.contains(JOB_NAME, case=False, na=False)]

print(f"✅ Excel rows after job filter: {len(df_edges)}")

# clean source column
df_edges["source stage.col"] = df_edges["source stage.col"].astype(str).str.strip()

# remove intermediate lineage from excel too
df_edges = df_edges[df_edges["source stage.col"].apply(is_real_source)]

excel_source_cols = set(df_edges["source stage.col"])

df_excel = pd.DataFrame({"source_stage_col": list(excel_source_cols)})

print(f"✅ Excel TRUE SOURCE columns: {len(df_excel)}")

# -------------------------------
# STEP 3: NORMALIZE (IMPORTANT)
# -------------------------------
def normalize(col):
    if "." in col:
        return col.split(".")[-1].lower()
    return col.lower()

xml_set = set(normalize(x) for x in df_xml["source_stage_col"])
excel_set = set(normalize(x) for x in df_excel["source_stage_col"])

# -------------------------------
# STEP 4: COMPARE
# -------------------------------
matched = xml_set & excel_set
missing_in_excel = xml_set - excel_set
missing_in_xml = excel_set - xml_set

# -------------------------------
# STEP 5: OUTPUT
# -------------------------------
final_rows = []

# MATCHED
for col in matched:
    final_rows.append({
        "source_file": JOB_NAME,
        "column_name": col,
        "target_table": "NA",
        "comments": "MATCHED"
    })

# XML but not in Excel
for col in missing_in_excel:
    final_rows.append({
        "source_file": JOB_NAME,
        "column_name": col,
        "target_table": "NA",
        "comments": "❌ Missing in Excel"
    })

# Excel but not in XML
for col in missing_in_xml:
    final_rows.append({
        "source_file": JOB_NAME,
        "column_name": col,
        "target_table": "NA",
        "comments": "⚠️ Missing in XML"
    })

df_final = pd.DataFrame(final_rows)

df_final.to_excel(OUTPUT_PATH, index=False)

# -------------------------------
# SUMMARY
# -------------------------------
print("\n📊 FINAL SUMMARY")
print("Matched:", len(matched))
print("Missing in Excel:", len(missing_in_excel))
print("Missing in XML:", len(missing_in_xml))

print("\n✅ Output:", OUTPUT_PATH)

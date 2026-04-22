import xml.etree.ElementTree as ET
import pandas as pd

# -------------------------------
# CONFIG
# -------------------------------
XML_PATH = "../jCidDW036_3215_CID_ACCT_PROD_Extract.xml"
EXCEL_PATH = "../dataviz1.xlsx"
OUTPUT_PATH = "final_recon_output.xlsx"

EDGES_SHEET = "Edges"

# -------------------------------
# STEP 1: PARSE XML (SOURCE COLUMNS)
# -------------------------------
tree = ET.parse(XML_PATH)
root = tree.getroot()

xml_source_cols = set()

for col in root.findall(".//Collection[@Name='Columns']/SubRecord"):
    source_col = None

    for prop in col.findall("Property"):
        if prop.attrib.get("Name") == "SourceColumn":
            source_col = prop.text

    if source_col:
        xml_source_cols.add(source_col.strip())

df_xml = pd.DataFrame({"source_stage_col": list(xml_source_cols)})

print(f"✅ Total XML source columns: {len(df_xml)}")

# -------------------------------
# STEP 2: LOAD EXCEL
# -------------------------------
df_edges = pd.read_excel(EXCEL_PATH, sheet_name=EDGES_SHEET)
df_edges.columns = [c.strip().lower() for c in df_edges.columns]

df_edges["source stage.col"] = df_edges["source stage.col"].astype(str).str.strip()

excel_source_cols = set(df_edges["source stage.col"])

df_excel = pd.DataFrame({"source_stage_col": list(excel_source_cols)})

print(f"✅ Total Excel source columns: {len(df_excel)}")

# -------------------------------
# STEP 3: COMPARISON
# -------------------------------
xml_set = set(df_xml["source_stage_col"])
excel_set = set(df_excel["source_stage_col"])

matched = xml_set & excel_set
missing_in_excel = xml_set - excel_set
missing_in_xml = excel_set - xml_set

# -------------------------------
# STEP 4: BUILD OUTPUT
# -------------------------------
final_rows = []

# MATCHED
for col in matched:
    final_rows.append({
        "source_file": XML_PATH.split("/")[-1],
        "column_name": col,
        "target_table": "NA",
        "comments": "MATCHED in Excel"
    })

# XML but NOT in Excel
for col in missing_in_excel:
    final_rows.append({
        "source_file": XML_PATH.split("/")[-1],
        "column_name": col,
        "target_table": "NA",
        "comments": "❌ Missing in Excel"
    })

# Excel but NOT in XML
for col in missing_in_xml:
    final_rows.append({
        "source_file": XML_PATH.split("/")[-1],
        "column_name": col,
        "target_table": "NA",
        "comments": "⚠️ Missing in XML"
    })

df_final = pd.DataFrame(final_rows)

# -------------------------------
# STEP 5: SAVE OUTPUT
# -------------------------------
df_final.to_excel(OUTPUT_PATH, index=False)

print("\n📊 SUMMARY")
print("Matched:", len(matched))
print("Missing in Excel:", len(missing_in_excel))
print("Missing in XML:", len(missing_in_xml))

print("\n✅ Output saved:", OUTPUT_PATH)

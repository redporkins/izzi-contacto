import pandas as pd
from pathlib import Path
import re
import json

# ---------- PATHS ----------
hibot_in_path = Path('CSV/filtered_hibot_export.csv')
pakoa_in_path = Path('CSV/filtered_sql_sales_export_CANCELADO_NOT_DONE.csv')  # <-- adjust name/path
tiktok_in_path = Path('CSV/filtered_tiktok_export.csv')
# pakoa_in_path = Path('scs_db01_contacts.csv')  # <-- adjust name/path
out_path = Path('CSV/format_Auto.csv')

# hibot_in_path = Path('hibot_conversations.csv')
# pakoa_in_path = Path('scs_db01_contacts.csv')  # <-- adjust name/path
# out_path = Path('Consultas/Lead_Lists/Clean/ViciDial/format_225.csv')


# # ---------- HIBOT (your original logic) ----------

def get_last_tag(tags_value):
    """
    Extracts the last tag description from a JSON-like string.
    Returns empty string if no valid tag is found.
    """
    if pd.isna(tags_value):
        return ''

    try:
        tags_list = json.loads(tags_value)

        if isinstance(tags_list, list) and len(tags_list) > 0:
            last_tag = tags_list[-1]
            return last_tag.get('description', '')
    except (json.JSONDecodeError, TypeError):
        pass

    return ''


# df = pd.read_csv(hibot_in_path, on_bad_lines='warn')
df = pd.read_csv(hibot_in_path, engine="python")


# required_cols = ['contact_account', contact_name', 'typing',
#                  'tags', 'agentName', 'campaignName', 'typeChannel']

required_cols = ['contact_account', 'contact_name', 'typing',
                 'tags', 'agentName', 'campaignName', 'typeChannel']


for col in required_cols:
    if col not in df.columns:
        raise KeyError(f"Missing column: {col}")

print(f"Total de conversaciones (todas): {len(df)}")
print(df['typing'].value_counts())

# WhatsApp only
df = df[df['typeChannel'] == 'WhatsApp']

# Keep only one row per phone
df = df.drop_duplicates(subset=['contact_account'], keep='first').reset_index(drop=True)

# Clean phone numbers
df['contact_account'] = (
    df['contact_account']
    .astype(str)
    .str.replace(r'\D', '', regex=True)
    .str[-10:]
)
df = df[df['contact_account'].str.len() == 10]

print(f"Total de conversaciones por WhatsApp: {len(df)}")

# Exclude campaignName
df = df[~df['campaignName'].isin([
    'Reclutamiento MTY',
    'Reclutamiento CDMX',
    'RH RECLUTAMIENTO CDMX OFFLINE',
    'RH Reclutamiento CDMX PRESENCIAL'
])]
print(f"Total fuera de reclutamiento: {len(df)}")

# typing filter
df['typing'] = df['typing'].astype(str)
print("typing (antes de filtrar):")
print(df['typing'].value_counts())

df = df[df['typing'].isin(['Transferencia', 'Inactividad', 'Gestión finalizada', 'nan'])]
print("typing (después de filtrar):")
print(df['typing'].value_counts())

# Exclude tags containing certain strings
df['last_tag'] = df['tags'].apply(get_last_tag)
df[['tags', 'last_tag']].head()

print("typing (antes de filtrar tags):")
print(df['typing'].value_counts())


# df = df[~df['tags'].str.contains('tt|ingresada|sc', case=False, na=False)]
df = df[~df['last_tag'].str.contains('tt|ingresada|ingresar|sc', case=False, na=False)]
print("typing (después de filtrar tags):")
print(df['typing'].value_counts())


# # ---------- COMMON OUTPUT STRUCTURE ----------

output_cols = [
    'Vendor lead code', 'Source Code', 'List ID', 'Phone Code', 'Phone Number',
    'Title', 'First Name', 'Middle Initial', 'Last Name',
    'Address Line 1', 'Address Line 2', 'Address Line 3',
    'City', 'State', 'Province', 'Postal Code', 'Country',
    'Gender', 'DOB', 'Alternate Phone Number', 'E-mail',
    'Security Phrase', 'Comments', 'Rank', 'Owner'
]

# Base DF for HiBot leads
hibot_out = pd.DataFrame('', index=df.index, columns=output_cols)

hibot_out['Phone Number'] = df['contact_account']
hibot_out['First Name'] = df['contact_name']
# hibot_out['Address Line 1'] = (
#     "typing: " + df['typing'].astype(str) +
#     " | tags: " + df['tags'].astype(str)
# )
hibot_out['Address Line 1'] = (
    "typing: " + df['typing'].astype(str) +
    " | last_tag: " + df['last_tag'].astype(str)
)
hibot_out['Address Line 2'] = df['agentName']
hibot_out['Address Line 3'] = df['campaignName']

# hibot_out['Phone Code'] = '52'
hibot_out['Country'] = 'Mexico'
hibot_out['Owner'] = '205'

# We keep typing as an extra column for sorting
hibot_out['typing'] = df['typing']


# ---------- PAKOA: CANCELLED / DENIED ORDERS ----------

# This CSV has fields like:
# NoContrato,FechaDeCreacionPakoa,Comentarios,ComentariosCancelacion,Nombre,
# IdConversacion,Sipre,DeleoMuni,CodigoPostal,Colonia,Costo,
# Telefono,Telefono2,TelefonoAtiende,FechaDeInstalacion,FechaCreacionOC,
# EstadoOrden,EstatusConfirmacionPakoa

pakoa = pd.read_csv(pakoa_in_path)

# Minimal columns we need
pakoa_required = [
    'NoContrato', 'Nombre', 'IdConversacion', 'Sipre', 'DeleoMuni',
    'CodigoPostal', 'Colonia', 'Telefono', 'Telefono2',
    'FechaDeInstalacion', 'EstadoOrden', 'Costo'
]
for col in pakoa_required:
    if col not in pakoa.columns:
        raise KeyError(f"Missing column in Pakoa file: {col}")

print(f"Total filas Pakoa (original): {len(pakoa)}")

# Convert Telefono + Telefono2 into separate rows
phone_cols = ['Telefono', 'Telefono2']
id_vars = [c for c in pakoa.columns if c not in phone_cols]

# Melt: from wide to long
pakoa_long = pakoa.melt(
    id_vars=id_vars,
    value_vars=phone_cols,
    var_name='phone_col',
    value_name='raw_phone'
)

# Clean and keep valid phones 
pakoa_long['raw_phone'] = pakoa_long['raw_phone'].astype(str)
print(f"\npakoa_long['raw_phone']:\n{pakoa_long['raw_phone']}")

# Normalize for filtering only (keep original raw_phone for output)
_phone_norm = (
    pakoa_long['raw_phone']
    .astype(str)
    .str.strip()
    .str.replace(r'\.0$', '', regex=True)
)

# Remove obvious nulls / "nan" / sentinel numbers
pakoa_long = pakoa_long[
    ~_phone_norm.isin(['', 'nan', 'None', 'NULL', '0', '123456789', '1234567890'])
]

# Save the phone as provided in the row, but drop a trailing ".0" that comes from floats
pakoa_long['Phone Number'] = (
    pakoa_long['raw_phone']
    .astype(str)
    .str.replace(r'\.0$', '', regex=True)
    .str[-10:]  # keep only the last 10 characters
)

# Keep one row per phone number
pakoa_long = pakoa_long.drop_duplicates(subset=['Phone Number']).reset_index(drop=True)


# Build output DF for Pakoa rows, same structure
pakoa_out = pd.DataFrame('', index=pakoa_long.index, columns=output_cols)

# # --- Mapping requested fields into VICIdial fields ---

# Phone Number, code, country
# pakoa_out['Phone Number'] = pakoa_long['raw_phone']
pakoa_out['Phone Number'] = pakoa_long['Phone Number']
# pakoa_out['Phone Code'] = '52'
pakoa_out['Country'] = 'Mexico'

# # Name
pakoa_out['First Name'] = pakoa_long['Nombre']  # full name

# # Vendor lead code = NoContrato
# pakoa_out['Vendor lead code'] = pakoa_long['NoContrato'].astype(str)

# # City / Postal / Colonia
pakoa_out['City'] = pakoa_long['DeleoMuni']
pakoa_out['Postal Code'] = pakoa_long['CodigoPostal'].astype(str)

# Put EstadoOrden + Costo (and mark as Pakoa cancelled lead) in Address Line 1
pakoa_out['Address Line 1'] = (
    "Pakoa - EstadoOrden: " + pakoa_long['EstadoOrden'].astype(str) +
    " | Costo: " + pakoa_long['Costo'].astype(str)
)

# Sipre + NoContrato in Address Line 2
pakoa_out['Address Line 2'] = (
    "NoContrato: " + pakoa_long['NoContrato'].astype(str) +
    " | Sipre: " + pakoa_long['Sipre'].astype(str)
)

# IdConversacion + Colonia in Address Line 3
pakoa_out['Address Line 3'] = (
    "IdConversacion: " + pakoa_long['IdConversacion'].astype(str) +
    " | Colonia: " + pakoa_long['Colonia'].astype(str)
)

# FechaInstalacion in Comments
pakoa_out['Middle Initial'] = (
    "FechaInstalacion: " + pakoa_long['FechaDeInstalacion'].astype(str)
)

# Same owner as other out-of-leads
pakoa_out['Owner'] = '205'

# typing label for sorting. I assume these are high-intent leads, so "Transferencia".
# (If you prefer them at the end, change this to 'nan'.)
pakoa_out['typing'] = 'Cancelación'

# ---------- TikTok: Forms ----------

tiktok = pd.read_csv(tiktok_in_path)

tiktok_required = ['Phone number', 'Name', 'Lead ID', 'Form ID', 'Creation time',
                   'Campaign ID', 'Campaign name', 'Ad group ID', 'Ad group name', 'Ad ID', 'Ad name']
for col in tiktok_required:
    if col not in tiktok.columns:
        raise KeyError(f"Missing column in TikTok file: {col}")

print(f"Total filas TikTok (original): {len(tiktok)}")

# Clean phone numbers -> last 10 digits
tiktok['Phone Number'] = (
    tiktok['Phone number']
    .astype(str)
    .str.replace(r'\D', '', regex=True)
    .str[-10:]
)

# Keep only valid 10-digit MX phones
tiktok = tiktok[tiktok['Phone Number'].str.len() == 10].copy()

# Keep one row per phone (keep the most recent form)
tiktok['Creation time'] = pd.to_datetime(tiktok['Creation time'], errors='coerce')
tiktok = tiktok.sort_values('Creation time', ascending=False)
tiktok = tiktok.drop_duplicates(subset=['Phone Number'], keep='first').reset_index(drop=True)

# Build output DF
tiktok_out = pd.DataFrame('', index=tiktok.index, columns=output_cols)

tiktok_out['Phone Number'] = tiktok['Phone Number']
tiktok_out['Country'] = 'Mexico'
tiktok_out['First Name'] = tiktok['Name'].astype(str)

# Put form + campaign metadata into address lines (use what your team finds most useful)
tiktok_out['Address Line 1'] = (
    "TikTok Form | Created: " + tiktok['Creation time'].astype(str)
)

tiktok_out['Address Line 2'] = (
    "Campaign: " + tiktok['Campaign name'].astype(str) +
    " | AdGroup: " + tiktok['Ad group name'].astype(str)
)

tiktok_out['Address Line 3'] = (
    "Ad: " + tiktok['Ad name'].astype(str) +
    " | LeadID: " + tiktok['Lead ID'].astype(str)
)

tiktok_out['Owner'] = '205'

# Typing label for sorting priority
tiktok_out['typing'] = 'TikTok Form'

# ---------- COMBINE & SORT ----------

tipificacion_order = ["TikTok Form", "Cancelación", "Inactividad", "Transferencia", "Gestión finalizada", "nan"]

all_out = pd.concat([hibot_out, pakoa_out, tiktok_out], ignore_index=True)

# List ID for ViciDial
all_out['List ID'] = '215'

# Deduplicate by phone across sources (keep highest priority after sorting)
all_out['typing'] = all_out['typing'].astype(str)
all_out['typing'] = pd.Categorical(all_out['typing'], categories=tipificacion_order, ordered=True)

# Sort by priority first
all_out = all_out.sort_values('typing')

# Then drop duplicates keeping the first (highest priority)
all_out = all_out.drop_duplicates(subset=['Phone Number'], keep='first').reset_index(drop=True)

# Save final CSV
all_out.to_csv(out_path, index=False, encoding='utf-8-sig')

print(f"Archivo generado correctamente: {out_path}")
print(f"Total filas exportadas (HiBot + Pakoa + TikTok): {len(all_out)}")

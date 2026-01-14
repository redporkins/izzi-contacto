import pandas as pd
from pathlib import Path

# Input and output paths
in_path = Path('.csv')
out_path = Path('buyers1.csv')

# Read your original CSV
df = pd.read_csv(in_path)

# Ensure columns exist
required_cols = ['NumeroDeContacto', 'NombreCompleto', 'Tipificacion', 'Etiquetas', 'Agente', 'Campania']
for col in required_cols:
    if col not in df.columns:
        raise KeyError(f"Missing column: {col}")

# Clean and standardize phone numbers (keep only digits, last 10)
df['NumeroDeContacto'] = (
    df['NumeroDeContacto']
    .astype(str)
    .str.replace(r'\D', '', regex=True)   # remove non-digits
    .str[-10:]                            # take last 10 digits
)

# Drop rows with missing or invalid numbers
df = df[df['NumeroDeContacto'].str.len() == 10]

# Keep only one row per unique phone number (keep the first occurrence)
df = df.drop_duplicates(subset=['NumeroDeContacto'], keep='first').reset_index(drop=True)


# Define the target column structure
output_cols = [
    'Vendor lead code', 'Source Code', 'List ID', 'Phone Code', 'Phone Number', 'Title', 'First Name',
    'Middle Initial', 'Last Name', 'Address Line 1', 'Address Line 2', 'Address Line 3', 'City', 'State',
    'Province', 'Postal Code', 'Country', 'Gender', 'DOB', 'Alternate Phone Number', 'E-mail',
    'Security Phrase', 'Comments', 'Rank', 'Owner'
]

# Create an empty output DataFrame with all required columns
out_df = pd.DataFrame('', index=df.index, columns=output_cols)

# Map your data into the appropriate columns
out_df['Phone Number'] = df['NumeroDeContacto']
out_df['First Name'] = df['NombreCompleto']  # Keep full name as-is
out_df['Address Line 1'] = (
    "Tipificación: " + df['Tipificacion'].astype(str) +
    " | Etiquetas: " + df['Etiquetas'].astype(str)
)
out_df['Address Line 2'] = df['Agente']
out_df['Address Line 3'] = df['Campania']

# Default values
out_df['Phone Code'] = '52'   # Mexico
out_df['Country'] = 'Mexico'

# Order data frame by TIPIFICACIÓN on the following order: "Inactividad", "Transferencia", "Gestión Finalizada"
tipificacion_order = ["Inactividad", "Transferencia", "Gestión Finalizada"]
out_df['Tipificacion'] = df['Tipificacion']
out_df['Tipificacion'] = pd.Categorical(out_df['Tipificacion'], categories=tipificacion_order, ordered=True)
out_df = out_df.sort_values('Tipificacion')

# Save final CSV
out_df.to_csv(out_path, index=False, encoding='utf-8-sig')

print(f"✅ Archivo generado correctamente: {out_path}")
print(f"Total de filas exportadas (únicos): {len(out_df)}")

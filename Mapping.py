# -*- coding: utf-8 -*-
"""
Pipeline avec allocation sur DESTINATION et respect du format FR des montants.

- input.csv (sep=';') avec montants français (virgule)
- mapping.xlsx :
    * "Mapping_Destination": Source, Target, Pourcentage allocation
    * "Mapping Entities":    Source, Target
    * "Mapping Functionnal Area": Source, Target
- Si une colonne requise est absente dans l'input, elle est créée avec "NA".
- DESTINATION : merge + duplication selon allocations ; AMOUNT ajusté (ou conservé si 100%).
- ENTITY / FUNCTIONAL_AREA : mapping direct (remplacement).
- Ordre des colonnes pris depuis "Template Output.xlsx".
"""

from pathlib import Path
import numpy as np
import pandas as pd
import time


# ----------------------------
# Début du timer
# ----------------------------
start_time = time.perf_counter()


# ----------------------------
# Paramètres / Chemins
# ----------------------------

def load_config(config_file: Path) -> dict:
    """Charge un fichier CSV de config à 2 colonnes: variable;value"""
    cfg = pd.read_csv(config_file, sep=";", dtype=str)
    if not {"variable", "value"}.issubset(cfg.columns):
        raise KeyError("Le fichier de config doit contenir les colonnes 'variable' et 'value'")
    return dict(zip(cfg["variable"].str.strip(), cfg["value"].str.strip()))


# ----------------------------
# 1) Lecture config
# ----------------------------
CONFIG_FILE = Path("config.csv")
config = load_config(CONFIG_FILE)

# Variables paramétrées depuis le fichier
INPUT_CSV       = Path(config["INPUT_CSV"])
MAPPING_XLSX    = Path(config["MAPPING_XLSX"])
OUTPUT_TEMPLATE = Path(config["OUTPUT_TEMPLATE"])
OUTPUT_XLSX     = Path(config["OUTPUT_XLSX"])
SHEET_DESTINATION     = config["SHEET_DESTINATION"]
SHEET_ENTITY          = config["SHEET_ENTITY"]
SHEET_FUNCTIONAL_AREA = config["SHEET_FUNCTIONAL_AREA"]
CURRENCY_DEFAULT      = config.get("CURRENCY_DEFAULT", "EUR")  # valeur par défaut EUR



# INPUT_CSV       = Path("input.csv")
# MAPPING_XLSX    = Path("mapping.xlsx")
# OUTPUT_TEMPLATE = Path("Template Output.xlsx")
# OUTPUT_XLSX     = Path("output.xlsx")

# # Onglets
# SHEET_DESTINATION     = "Mapping_Destination"
# SHEET_ENTITY          = "Mapping Entities"
# SHEET_FUNCTIONAL_AREA = "Mapping Functionnal Area"

# # Valeurs constantes
# CURRENCY_DEFAULT = "EUR"

# Colonnes requises côté input
REQUIRED_INPUT_COLS = [
    "SCENARIO", "PERIOD", "ENTITY", "FUNCTIONAL_AREA", "ACCOUNT",
    "DESTINATION", "CUSTOMER", "PRODUCT", "BUSINESS_TYPE",
    "UOM", "IOM", "TERRITORY", "CHANEL", "AMOUNT"
]

# ----------------------------
# Helpers format FR
# ----------------------------
def parse_amount_fr_to_float(x) -> float:
    """'15 000,2' -> 15000.2 ; 'NA'/'': 0.0"""
    s = str(x).strip()
    if s == "" or s.upper() == "NA":
        return 0.0
    s = s.replace("\u00A0", "").replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0

def format_amount_float_to_fr(val: float) -> str:
    """15000.2 -> '15000,2' ; 150.0 -> '150' ; pas de séparateurs de milliers."""
    if val is None or abs(val) < 1e-12:
        return "0"
    if abs(val - int(val)) < 1e-12:
        return str(int(val))
    s = f"{val:.6f}".rstrip("0").rstrip(".")
    return s.replace(".", ",")

def to_ratio(v) -> float:
    """
    Convertit une valeur d'allocation hétérogène en ratio (1.0 = 100%).
    Accepte '100%', '100', '1', '40', '0,4', '40,5%', etc.
    Règle:
      - si contient '%': float(sans '%')/100
      - sinon: float(v); si > 1 => /100 (ex: '40' -> 0.4), sinon garder (ex: '1' -> 1.0)
    """
    s = str(v).strip().replace("\u00A0", "").replace(" ", "").replace(",", ".")
    if s == "":
        return 1.0
    if s.endswith("%"):
        s = s[:-1]
        try:
            return float(s) / 100.0
        except ValueError:
            return 1.0
    try:
        val = float(s)
    except ValueError:
        return 1.0
    return val/100.0 if val > 1.0 else val

# ----------------------------
# Fonctions utilitaires
# ----------------------------
def ensure_required_columns(df: pd.DataFrame, required_cols: list, fill_value: str = "NA") -> pd.DataFrame:
    for col in required_cols:
        if col not in df.columns:
            df[col] = fill_value
        else:
            df[col] = df[col].fillna(fill_value)
    return df

def build_map_dict(mapping_xlsx: Path, sheet_name: str) -> dict:
    df_map = pd.read_excel(mapping_xlsx, sheet_name=sheet_name, dtype=str)
    required = {"Source", "Target"}
    if not required.issubset(df_map.columns):
        raise KeyError(f"Onglet '{sheet_name}' doit contenir 'Source' et 'Target'")
    return dict(
        zip(
            df_map["Source"].astype(str).str.strip(),
            df_map["Target"].astype(str).str.strip()
        )
    )

def load_dest_mapping_df(mapping_xlsx: Path, sheet_name: str) -> pd.DataFrame:
    """Retourne un DF normalisé: [Source_norm, Target, pct] avec pct en ratio."""
    dfm = pd.read_excel(mapping_xlsx, sheet_name=sheet_name, dtype=str)
    if "Source" not in dfm.columns or "Target" not in dfm.columns:
        raise KeyError(f"Onglet '{sheet_name}' doit contenir 'Source' et 'Target'")

    if "Pourcentage allocation" not in dfm.columns:
        dfm["Pourcentage allocation"] = "100"

    dfm = dfm.assign(
        Source_norm=dfm["Source"].astype(str).str.strip(),
        pct=dfm["Pourcentage allocation"].apply(to_ratio),
        Target=dfm["Target"].astype(str).str.strip(),
    )[["Source_norm", "Target", "pct"]]
    return dfm

def apply_destination_with_allocation(df_in: pd.DataFrame, df_map: pd.DataFrame) -> pd.DataFrame:
    """
    Applique le mapping DESTINATION par merge (duplication auto si plusieurs cibles).
    - Si pas de mapping: Target = DESTINATION d'origine, pct = 1.0
    - Si pct == 1.0 : AMOUNT conservé tel quel (chaîne FR).
    - Si pct != 1.0 : AMOUNT recalculé et reformaté FR.
    """
    work = df_in.copy()
    work["DEST_norm"] = work["DESTINATION"].astype(str).str.strip()

    merged = work.merge(
        df_map,
        left_on="DEST_norm",
        right_on="Source_norm",
        how="left",
        suffixes=("", "_m")
    )

    # Lignes sans mapping -> garder destination et 100%
    merged["Target"] = merged["Target"].fillna(merged["DESTINATION"])
    merged["pct"] = merged["pct"].astype(float).fillna(1.0)

    # AMOUNT sortant
    amt_float = merged["AMOUNT"].apply(parse_amount_fr_to_float)
    is_100 = (merged["pct"] - 1.0).abs() < 1e-12
    amt_calc = amt_float * merged["pct"]
    amt_calc_str = amt_calc.apply(format_amount_float_to_fr)

    merged["AMOUNT"] = np.where(is_100, merged["AMOUNT"], amt_calc_str)
    merged["DESTINATION"] = merged["Target"]

    # Nettoyage colonnes techniques
    merged = merged.drop(columns=[c for c in ["DEST_norm", "Source_norm", "Target", "pct"] if c in merged.columns])
    return merged

# ----------------------------
# 1) Lecture du CSV (préserve 'NA')
# ----------------------------
df = pd.read_csv(INPUT_CSV, sep=";", dtype=str, keep_default_na=False)
if "PERIOD" not in df.columns:
    raise KeyError("La colonne 'PERIOD' est absente de input.csv.")

# Normaliser PERIOD sur 2 caractères
df["PERIOD"] = df["PERIOD"].astype(str).str.strip().str.zfill(2)

# Ajouter/compléter les colonnes requises
df = ensure_required_columns(df, REQUIRED_INPUT_COLS, fill_value="NA")

# ----------------------------
# 2) Chargement des mappings
# ----------------------------
df_destmap  = load_dest_mapping_df(MAPPING_XLSX, SHEET_DESTINATION)
entity_map  = build_map_dict(MAPPING_XLSX, SHEET_ENTITY)
func_map    = build_map_dict(MAPPING_XLSX, SHEET_FUNCTIONAL_AREA)

# ----------------------------
# 3) Application des mappings
# ----------------------------
df = apply_destination_with_allocation(df, df_destmap)               # DEST + AMOUNT
df["ENTITY"] = df["ENTITY"].astype(str).str.strip().map(entity_map).fillna(df["ENTITY"])
df["FUNCTIONAL_AREA"] = (
    df["FUNCTIONAL_AREA"].astype(str).str.strip().map(func_map).fillna(df["FUNCTIONAL_AREA"])
)

# ----------------------------
# 4) Charger le template (ordre colonnes)
# ----------------------------
template_cols = list(pd.read_excel(OUTPUT_TEMPLATE, dtype=str, nrows=0).columns)
if not template_cols:
    raise ValueError(f"Aucune colonne détectée dans le template '{OUTPUT_TEMPLATE}'.")

# ----------------------------
# 5) Construire le DataFrame final selon template
# ----------------------------
assignments = {
    "SCENARIO":        df["SCENARIO"],
    "PERIOD":          df["PERIOD"],
    "ENTITY":          df["ENTITY"],
    "FUNCTIONAL_AREA": df["FUNCTIONAL_AREA"],
    "ACCOUNT":         df["ACCOUNT"],
    "CURRENCY":        pd.Series([CURRENCY_DEFAULT]*len(df), index=df.index),
    "DESTINATION":     df["DESTINATION"],
    "CUSTOMER":        df["CUSTOMER"],
    "PRODUCT":         df["PRODUCT"],
    "BUSINESS_TYPE":   df["BUSINESS_TYPE"],
    "UOM":             df["UOM"],
    "IOM":             df["IOM"],
    "TERRITORY":       df["TERRITORY"],
    "CHANEL":          df["CHANEL"],
    "AMOUNT":          df["AMOUNT"],  # déjà FR
}

output_df = pd.DataFrame(index=df.index, columns=template_cols)
for col in template_cols:
    output_df[col] = assignments.get(col, "")

# ✅ Ajout de la colonne SOMME_AMOUNT uniquement sur la première ligne
amount_float = output_df["AMOUNT"].apply(parse_amount_fr_to_float)
total_amount = amount_float.sum()

output_df["SOMME_AMOUNT"] = ""  # colonne vide
if not output_df.empty:
    output_df.loc[0, "SOMME_AMOUNT"] = format_amount_float_to_fr(total_amount)



# ----------------------------
# 6) Écriture CSV
# ----------------------------
output_df.to_csv(OUTPUT_XLSX.with_suffix(".csv"), sep=";", index=False, encoding="utf-8-sig")

print(f"✅ Terminé. Fichier écrit : {OUTPUT_XLSX.with_suffix('.csv').resolve()}")


# ----------------------------
# 6) Écriture Excel
# ----------------------------
# with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
#     output_df.to_excel(writer, index=False, sheet_name="Output")


# ----------------------------
# 7) Comparaison des totaux Input vs Output
# ----------------------------
# Calcul du total Input
df_input = pd.read_csv(INPUT_CSV, sep=";", dtype=str, keep_default_na=False)
total_input = df_input["AMOUNT"].apply(parse_amount_fr_to_float).sum()

# Calcul du total Output
total_output = output_df["AMOUNT"].apply(parse_amount_fr_to_float).sum()
# Calcul du delta entre input et output
delta = total_input - total_output

# DataFrame de comparaison avec delta (format FR)
compare_df = pd.DataFrame([{
    "Total_Amount_Input":  format_amount_float_to_fr(total_input),
    "Total_Amount_Output": format_amount_float_to_fr(total_output),
    "Delta_Input_minus_Output": format_amount_float_to_fr(delta)  # Input - Output
}])

# Écriture dans un nouveau fichier CSV
compare_file = OUTPUT_XLSX.with_name("compare_amounts.csv")
compare_df.to_csv(compare_file, sep=";", index=False, encoding="utf-8-sig")

print(f"📊 Fichier de comparaison écrit : {compare_file.resolve()}")




# ----------------------------
# Fin du timer
# ----------------------------
end_time = time.perf_counter()
elapsed = end_time - start_time

print(f"✅ Terminé. Fichier écrit : {OUTPUT_XLSX.resolve()}")
print(f"⏱️ Temps d'exécution : {elapsed:.2f} secondes")
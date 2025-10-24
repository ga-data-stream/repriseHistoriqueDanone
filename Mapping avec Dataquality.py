# -*- coding: utf-8 -*-
"""
Pipeline avec contrôle Master Data + log qualité, allocation sur DESTINATION
et format FR des montants.

- input.csv (sep=';') montants FR (virgule)
- mapping.xlsx :
    * "Mapping_Destination": Source, Target, Pourcentage allocation
    * "Mapping Entities":    Source, Target
    * "Mapping Functionnal Area": Source, Target
- master data (csv/xlsx) : colonne DESTINATION (configurable)
- Si une colonne requise est absente dans l'input, elle est créée avec "NA".
- DESTINATION : merge + duplication selon allocations ; AMOUNT ajusté (ou conservé si 100%).
- ENTITY / FUNCTIONAL_AREA : mapping direct (remplacement).
- Vérif Master Data : on garde seulement les cibles présentes ; on log les cibles absentes
  dans QualitycheckFile.csv (une ligne par cible rejetée, avec les colonnes d’input + FAILED_TARGET).
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
    d = dict(zip(cfg["variable"].str.strip(), cfg["value"].str.strip()))
    return d

# CONFIG_FILE = Path("config.csv")
CONFIG_FILE = Path("input v2/config.csv")
config = load_config(CONFIG_FILE)
# print(config)
# Variables paramétrées depuis le fichier
INPUT_CSV            = Path(config["INPUT_CSV"])
MAPPING_XLSX         = Path(config["MAPPING_XLSX"])
OUTPUT_TEMPLATE      = Path(config["OUTPUT_TEMPLATE"])
OUTPUT_XLSX          = Path(config["OUTPUT_XLSX"])
SHEET_DESTINATION    = config["SHEET_DESTINATION"]
SHEET_ENTITY         = config["SHEET_ENTITY"]
SHEET_FUNCTIONAL_AREA= config["SHEET_FUNCTIONAL_AREA"]
CURRENCY_DEFAULT     = config.get("CURRENCY_DEFAULT", "EUR")

# Master Data (flexible csv/xlsx)
MASTER_DATA_FILE     = Path(config["MASTER_DATA_FILE"])               # ex: MasterData.csv ou MasterData.xlsx
MASTER_SHEET         = config.get("MASTER_SHEET", "") or None         # si xlsx et multiple onglets
MASTER_DEST_COL_NAME = config.get("MASTER_DEST_COL_NAME", "DESTINATION")

# Fichier log qualité
QUALITY_LOG_CSV      = Path(config.get("QUALITY_LOG_CSV", "QualitycheckFile.csv"))

# Colonnes requises côté input
REQUIRED_INPUT_COLS = [
    "SCENARIO", "PERIOD", "ENTITY", "FUNCTIONAL_AREA", "ACCOUNT",
    "DESTINATION", "CUSTOMER", "PRODUCT", "BUSINESS_TYPE",
    "UOM", "IOM", "TERRITORY", "CHANEL", "AMOUNT"
]

# ----------------------------
# Helpers format FR / utils
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
    Convertit une valeur d'allocation en ratio (1.0 = 100%).
    Accepte '100%', '100', '1', '40', '0,4', '40,5%', etc.
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
    """Retourne DF normalisé: [Source_norm, Target, pct] avec pct en ratio."""
    dfm = pd.read_excel(mapping_xlsx, sheet_name=sheet_name, dtype=str)
    if "Source" not in dfm.columns or "Target" not in dfm.columns:
        raise KeyError(f"Onglet '{sheet_name}' doit contenir 'Source' et 'Target'")
    if "Pourcentage allocation" not in dfm.columns:
        dfm["Pourcentage allocation"] = "100"

    dfm = dfm.assign(
        Source_norm=dfm["Source"].astype(str).str.strip(),
        Target=dfm["Target"].astype(str).str.strip(),
        pct=dfm["Pourcentage allocation"].apply(to_ratio),
    )[["Source_norm", "Target", "pct"]]
    return dfm

def _find_case_insensitive_col(df: pd.DataFrame, wanted: str) -> str:
    """Trouve une colonne par nom insensible à la casse ; lève si absente."""
    wanted_lower = wanted.lower()
    for c in df.columns:
        if str(c).lower() == wanted_lower:
            return c
    raise KeyError(f"Colonne '{wanted}' introuvable dans le Master Data.")

def load_master_destinations(path: Path, dest_col_name: str, sheet: str | None) -> set:
    """Charge le Master Data (csv/xlsx), renvoie l'ensemble des destinations 'valides'."""
    suffix = path.suffix.lower()
    if suffix in [".csv", ".txt"]:
        dfm = pd.read_csv(path, sep=";", dtype=str, keep_default_na=False)
    elif suffix in [".xlsx", ".xls"]:
        dfm = pd.read_excel(path, sheet_name=sheet if sheet else 0, dtype=str)
    else:
        raise ValueError(f"Format Master Data non supporté: {suffix}")

    col = _find_case_insensitive_col(dfm, dest_col_name)
    vals = dfm[col].astype(str).str.strip()
    return set(vals.tolist())

def apply_destination_with_allocation(df_in: pd.DataFrame, df_map: pd.DataFrame) -> pd.DataFrame:
    """
    Applique le mapping DESTINATION avec duplication pour allocations.
    Conserve des colonnes techniques pour contrôle Master Data ensuite :
      - __ROW_ID : identifiant de la ligne d'input d'origine
      - SOURCE_DESTINATION : destination d'origine
      - TARGET_APPLIED : cible après mapping (ou destination d'origine si pas de mapping)
      - ALLOCATION_RATIO : ratio appliqué
    """
    work = df_in.copy()
    work["DEST_norm"] = work["DESTINATION"].astype(str).str.strip()

    merged = work.merge(
        df_map, left_on="DEST_norm", right_on="Source_norm",
        how="left", suffixes=("", "_m")
    )

    # Sans mapping -> garder destination d'origine et 100%
    merged["TARGET_APPLIED"]  = merged["Target"].fillna(merged["DESTINATION"]).astype(str).str.strip()
    merged["ALLOCATION_RATIO"] = merged["pct"].astype(float).fillna(1.0)

    # Calcul amount (FR)
    amt_float   = merged["AMOUNT"].apply(parse_amount_fr_to_float)
    is_100      = (merged["ALLOCATION_RATIO"] - 1.0).abs() < 1e-12
    amt_calc    = amt_float * merged["ALLOCATION_RATIO"]
    amt_calc_fr = amt_calc.apply(format_amount_float_to_fr)
    merged["AMOUNT"]      = np.where(is_100, merged["AMOUNT"], amt_calc_fr)
    merged["SOURCE_DESTINATION"] = merged["DESTINATION"]
    merged["DESTINATION"] = merged["TARGET_APPLIED"]

    # Nettoyage
    drop_cols = [c for c in ["DEST_norm","Source_norm","Target","pct"] if c in merged.columns]
    merged.drop(columns=drop_cols, inplace=True)
    return merged

def validate_against_master_and_log(df_mapped: pd.DataFrame,
                                    master_set: set,
                                    original_input: pd.DataFrame,
                                    log_path: Path) -> pd.DataFrame:
    """
    Garder seulement les lignes dont DESTINATION (après mapping) ∈ Master Data.
    Logger dans log_path les lignes REJETÉES en copiant STRICTEMENT la ligne d'INPUT
    (non mappée) + FAILED_TARGET (cible invalide) + REASON.
    Gère les cas 1->n : une ligne d'input peut générer plusieurs FAILED_TARGET.
    """
    # Normalisation pour test d'appartenance
    dest_norm = df_mapped["DESTINATION"].astype(str).str.strip()
    is_valid = dest_norm.isin(master_set)

    rejected = df_mapped.loc[~is_valid, ["__ROW_ID", "DESTINATION"]].copy()
    rejected.rename(columns={"DESTINATION": "FAILED_TARGET"}, inplace=True)  # cible invalide post-mapping
    kept = df_mapped.loc[is_valid].copy()

    # ⚙️ Sécurité : supprimer d'éventuels doublons de colonnes dans l'input
    original_input = original_input.loc[:, ~original_input.columns.duplicated()]

    # Colonnes de l'input à restituer telles quelles (sans __ROW_ID)
    cols_in = [c for c in original_input.columns if c != "__ROW_ID"]

    if not rejected.empty:
        # 🔁 Dupliquer la ligne d'input pour CHAQUE cible rejetée (1->n)
        rej_log = rejected.merge(
            original_input[["__ROW_ID"] + cols_in],
            on="__ROW_ID", how="left"
        )

        # Ordonner et enrichir
        rej_log = rej_log[cols_in + ["FAILED_TARGET"]].copy()
        rej_log["REASON"] = "Destination absente du Master Data"

        rej_log.to_csv(log_path, sep=";", index=False, encoding="utf-8-sig")
        print(f"⚠️ Lignes rejetées loguées dans : {log_path.resolve()} (rows={len(rej_log)})")
    else:
        # Écrire un fichier vide avec l'entête attendue
        pd.DataFrame(columns=cols_in + ["FAILED_TARGET", "REASON"])\
          .to_csv(log_path, sep=";", index=False, encoding="utf-8-sig")
        print(f"✅ Aucun rejet Master Data. Fichier log vide : {log_path.resolve()}")

    return kept

# ----------------------------
# 1) Lecture du CSV (préserve 'NA')
# ----------------------------
df = pd.read_csv(INPUT_CSV, sep=";", dtype=str, keep_default_na=False)

if "PERIOD" not in df.columns:
    raise KeyError("La colonne 'PERIOD' est absente de input.csv.")

# Normaliser PERIOD sur 2 caractères
df["PERIOD"] = df["PERIOD"].astype(str).str.strip().str.zfill(2)

# Ajouter/compléter les colonnes requises & tracer l'ID d'origine
df = ensure_required_columns(df, REQUIRED_INPUT_COLS, fill_value="NA")
df["__ROW_ID"] = np.arange(len(df), dtype=int)

# Copie de référence de l'input (après normalisation/complétion)
df_input_ref = df.copy()

# ----------------------------
# 2) Chargement des mappings
# ----------------------------
df_destmap  = load_dest_mapping_df(MAPPING_XLSX, SHEET_DESTINATION)
entity_map  = build_map_dict(MAPPING_XLSX, SHEET_ENTITY)
func_map    = build_map_dict(MAPPING_XLSX, SHEET_FUNCTIONAL_AREA)

# ----------------------------
# 3) Application du mapping DEST + allocation
# ----------------------------
df = apply_destination_with_allocation(df, df_destmap)

# ----------------------------
# 4) Contrôle Master Data (rejets ciblés + log)
# ----------------------------
master_set = load_master_destinations(MASTER_DATA_FILE, MASTER_DEST_COL_NAME, MASTER_SHEET)
df = validate_against_master_and_log(df, master_set, df_input_ref, QUALITY_LOG_CSV)

# ----------------------------
# 5) Mappings simples ENTITY / FUNCTIONAL_AREA
# ----------------------------
df["ENTITY"] = df["ENTITY"].astype(str).str.strip().map(entity_map).fillna(df["ENTITY"])
df["FUNCTIONAL_AREA"] = (
    df["FUNCTIONAL_AREA"].astype(str).str.strip().map(func_map).fillna(df["FUNCTIONAL_AREA"])
)

# ----------------------------
# 6) Charger le template (ordre colonnes)
# ----------------------------
template_cols = list(pd.read_excel(OUTPUT_TEMPLATE, dtype=str, nrows=0).columns)
if not template_cols:
    raise ValueError(f"Aucune colonne détectée dans le template '{OUTPUT_TEMPLATE}'.")

# ----------------------------
# 7) Construire le DataFrame final selon template
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
output_df["SOMME_AMOUNT"] = ""
if not output_df.empty:
    output_df.loc[0, "SOMME_AMOUNT"] = format_amount_float_to_fr(total_amount)

# ----------------------------
# 8) Écriture CSV (output) + comparatif totaux
# ----------------------------
out_csv = OUTPUT_XLSX.with_suffix(".csv")
output_df.to_csv(out_csv, sep=";", index=False, encoding="utf-8-sig")
print(f"✅ Terminé. Fichier écrit : {out_csv.resolve()}")

# Comparaison Input vs Output (après rejets Master Data)
df_input_raw = pd.read_csv(INPUT_CSV, sep=";", dtype=str, keep_default_na=False)
total_input  = df_input_raw["AMOUNT"].apply(parse_amount_fr_to_float).sum()
total_output = output_df["AMOUNT"].apply(parse_amount_fr_to_float).sum()
# Calcul du delta entre input et output
delta = total_input - total_output

# DataFrame de comparaison avec delta (format FR)
compare_df = pd.DataFrame([{
    "Total_Amount_Input":  format_amount_float_to_fr(total_input),
    "Total_Amount_Output": format_amount_float_to_fr(total_output),
    "Delta_Input_minus_Output": format_amount_float_to_fr(delta)  # Input - Output
}])

# Écriture du fichier de comparaison
compare_file = OUTPUT_XLSX.with_name("compare_amounts.csv")
compare_df.to_csv(compare_file, sep=";", index=False, encoding="utf-8-sig")
print(f"📊 Fichier de comparaison écrit : {compare_file.resolve()}")


# ----------------------------
# Fin du timer
# ----------------------------
elapsed = time.perf_counter() - start_time
print(f"⏱️ Temps d'exécution : {elapsed:.2f} secondes")

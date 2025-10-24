# -*- coding: utf-8 -*-
"""
Duplication des lignes d'un fichier CSV pour atteindre ~1 million de lignes.
"""

import pandas as pd
from pathlib import Path

# ----------------------------
# Paramètres / Chemins
# ----------------------------
INPUT_FILE = Path("input.csv")       # fichier d'entrée existant
OUTPUT_FILE = Path("input1M.csv")    # fichier de sortie
TARGET_ROWS = 1_000_000              # nombre de lignes souhaitées

# ----------------------------
# Lecture du fichier existant
# ----------------------------
df = pd.read_csv(INPUT_FILE, sep=";")  # adapte le séparateur si besoin
n = len(df)

if n == 0:
    raise ValueError("⚠️ Le fichier input.csv est vide, rien à dupliquer.")

# ----------------------------
# Calcul du facteur de duplication
# ----------------------------
repeat_factor = TARGET_ROWS // n + 1  # combien de fois dupliquer
df_big = pd.concat([df] * repeat_factor, ignore_index=True)

# Garder seulement 1M de lignes pile
df_big = df_big.iloc[:TARGET_ROWS]

# ----------------------------
# Sauvegarde dans le nouveau fichier
# ----------------------------
df_big.to_csv(OUTPUT_FILE, sep=";", index=False)

print(f"✅ Nouveau fichier généré : {OUTPUT_FILE} ({len(df_big)} lignes)")

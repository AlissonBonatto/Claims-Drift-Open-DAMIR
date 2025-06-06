import pandas as pd
import warnings
warnings.simplefilter('ignore')

region_mapping = {
    5: "Régions et Départements d'outre-mer",
    11: "Ile-de-France",
    24: "Centre-Val-de-Loire",
    27: "Bourgogne-Franche-Comté",
    28: "Normandie",
    32: "Hauts-de-France",
    44: "Grand Est",
    52: "Pays de la Loire",
    53: "Bretagne",
    75: "Nouvelle-Aquitaine",
    76: "Occitanie",
    84: "Auvergne-Rhône-Alpes",
    93: "Provence-Alpes-Côte d'Azur et Corse"
}

age_mapping = {
    0:  "0 à 19 ans",
    20: "20 à 39 ans",
    30: "20 à 39 ans",
    40: "40 à 59 ans",
    50: "40 à 59 ans",
    60: "60+",
    70: "60+",
    80: "60+"
}

sexe_mapping = {
    1: "Hommes",
    2: "Femmes",

}



print(df_grouped)

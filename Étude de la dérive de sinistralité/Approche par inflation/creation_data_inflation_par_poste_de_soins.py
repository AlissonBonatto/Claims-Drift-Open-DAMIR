'''Ce code utilise uniquement les codes actes classés selon la méthode présentée dans l’étude bibliographique.'''
import pandas as pd
import os
from utils.code_acte import Poste_soins, dic 


main_paths = {
    "2019": "/raid/datasets/allianzsante/A2019",
    "2020": "/raid/datasets/allianzsante/A2020",
    "2021": "/raid/datasets/allianzsante/A2021",
    "2022": "/raid/datasets/allianzsante/A2022",
    "2023": "/raid/datasets/allianzsante/A2023",
    "2024": "/raid/datasets/allianzsante/A2024"
}
all_data = []
for year, path in main_paths.items():
    print(f"Traitement des fichiers pour {year}...")
    for file in os.listdir(path):
        if file.endswith(".csv"):
            file_path = os.path.join(path, file)
            try:
                df = pd.read_csv(file_path,sep = ';', usecols=['PRS_REM_TAU','FLX_ANN_MOI','AGE_BEN_SNDS','PRS_DEP_MNT','PRS_ACT_QTE','PRS_NAT', "FLT_ACT_NBR", 'FLT_REM_MNT','FLT_PAI_MNT', 'FLT_ACT_QTE'])
                df["ANNEE"] = int(year)
                df = df[(df['PRS_REM_TAU'] <= 100) & (df['FLT_REM_MNT'] >= 0) & (df['FLT_PAI_MNT'] > 0) & (df['FLT_ACT_QTE'].notna())]
                df["RAC"] = df['FLT_PAI_MNT']-df['FLT_REM_MNT']
                all_data.append(df)
            except Exception as e:
                print(f"Erreur lecture fichier {file_path}: {e}")


df_total = pd.concat(all_data, ignore_index=True)

prs_nat_to_famille = {}

for famille_name, ids in Poste_soins.items():
    for id_ in ids:
        prs_nat_list = dic.get(id_, [])
        for prs_nat in prs_nat_list:
            prs_nat_to_famille[prs_nat] = famille_name


def get_famille(prs_nat):
    return prs_nat_to_famille.get(prs_nat, None)

# 3. Ajouter la colonne dans df_total
df_total['FAMILLE_ACTE'] = df_total['PRS_NAT'].apply(get_famille)


# Agrégation annuelle par famille
df_aggr = df_total.groupby(["ANNEE", "FAMILLE_ACTE"], as_index=False).agg({
    "FLT_ACT_QTE": "sum",
    "FLT_PAI_MNT": "sum",
    "RAC": "sum"
})
df_aggr["COUT_MOYEN_FR"] = df_aggr["FLT_PAI_MNT"] / df_aggr["FLT_ACT_QTE"]
df_aggr["COUT_MOYEN_RAC"] = df_aggr["RAC"] / df_aggr["FLT_ACT_QTE"]

df_aggr.to_csv(r'/Étude de la dérive de sinistralité/Approche par inflation/DBs/inflation_par_poste_de_soins.csv')

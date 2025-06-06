import pandas as pd

# Chargement des données
df_aggr = pd.read_csv(r'/raid/home/allianzsante/fadli_oth/Desktop/Getting Started/data_cleaning/df_aggr.csv')

annees = sorted(df_aggr["ANNEE"].unique())
for i in range(len(annees) - 1):
    annee1 = annees[i]
    annee2 = annees[i + 1]

    df1 = df_aggr[df_aggr["ANNEE"] == annee1][["FAMILLE_ACTE", "COUT_MOYEN_FR", "COUT_MOYEN_RAC", "FLT_ACT_QTE"]]
    df2 = df_aggr[df_aggr["ANNEE"] == annee2][["FAMILLE_ACTE", "COUT_MOYEN_FR", "COUT_MOYEN_RAC"]]

    df_merge = pd.merge(df1, df2, on="FAMILLE_ACTE", how="inner", suffixes=(f"_{annee1}", f"_{annee2}"))

    df_merge["EFFET_INFLATION_FR"] = (df_merge[f"COUT_MOYEN_FR_{annee2}"] - df_merge[f"COUT_MOYEN_FR_{annee1}"]) * df_merge[f"FLT_ACT_QTE"]
    df_merge["EFFET_INFLATION_RAC"] = (df_merge[f"COUT_MOYEN_RAC_{annee2}"] - df_merge[f"COUT_MOYEN_RAC_{annee1}"]) * df_merge[f"FLT_ACT_QTE"]

    total_fr = (df_merge[f"COUT_MOYEN_FR_{annee1}"] * df_merge[f"FLT_ACT_QTE"]).sum()
    total_rac = (df_merge[f"COUT_MOYEN_RAC_{annee1}"] * df_merge[f"FLT_ACT_QTE"]).sum()

    inflation_fr = df_merge["EFFET_INFLATION_FR"].sum() / total_fr
    inflation_rac = df_merge["EFFET_INFLATION_RAC"].sum() / total_rac

    print(f"Inflation FR entre {annee1} et {annee2} : {inflation_fr:.2%}")
    print(f"Inflation RAC entre {annee1} et {annee2} : {inflation_rac:.2%}\n")

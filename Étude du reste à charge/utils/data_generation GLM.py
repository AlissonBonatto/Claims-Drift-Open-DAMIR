import os
import pandas as pd
import polars as pl


def get_yearly_data(year):
    # Définir le dossier principal contenant les sous-dossiers
    main_path = f"/raid/datasets/allianzsante/A{year}"

    # Liste pour stocker les résultats
    all_data = []
    
    k = 0

    for file in os.listdir(main_path):
        if file.endswith(".csv"):
            file_path = os.path.join(main_path, file)
            use_cols = ['SOI_ANN','SOI_MOI', "PRS_REM_TAU", "BEN_SEX_COD", "AGE_BEN_SNDS", "FLT_PAI_MNT", "FLT_REM_MNT", 'ASU_NAT',
                        'BEN_CMU_TOP', 'BEN_QLT_COD', 'BEN_RES_REG', 'CPT_ENV_TYP', 'PRS_NAT']
            data = pl.read_csv(file_path, separator=';', columns=use_cols, ignore_errors=True)
                    
            data = data.filter(
                (data['SOI_ANN'] >= 2019) & # On ne garde que les années à partie de 2019
                (data['PRS_REM_TAU'] <= 100) & # Le taux de remboursement doit être inférieur ou égal à 100 %
                (data['PRS_REM_TAU'] >= 0) & # Le taux de remboursement doit être supérieur ou égal à 0 %
                (data['FLT_REM_MNT'] >= 0) & #Le montant remboursé doit être supérieur à 0, car sinon, il s'agit de participations forfaitaires ou de régularisations qui concernent la sécurité sociale et non pas les mutuelles tels que celle d'Allianz
                (data['FLT_PAI_MNT'] > 0) & # Un montant de prestation doit être supérieur à 0 (à valider)
                (data['BEN_RES_REG'] != 99) # région doit être connue
            )
            data = data.with_columns(
                (data['FLT_PAI_MNT'] - data['FLT_REM_MNT']).alias("RAC")
            )
            # Stocker les données
            all_data.append(data)
        k += 1
        print(k)

    all_data = [df.with_columns([pl.col(c).cast(pl.Float64) for c in df.columns]) for df in all_data]

    # Concaténer tous les fichiers
    df_final = pl.concat(all_data)

    # Agréger les résultats par mois
    result = df_final.group_by(['SOI_ANN','SOI_MOI', 'BEN_SEX_COD', 'AGE_BEN_SNDS', 'BEN_RES_REG']).agg(
        pl.sum('RAC').alias('RAC'),
        pl.sum('FLT_PAI_MNT').alias('FLT_PAI_MNT'),
        pl.sum('FLT_REM_MNT').alias('FLT_REM_MNT'),
    ).sort(['SOI_ANN', 'SOI_MOI'])

    return result


if __name__ == "__main__":
    print("start")
    dfs = []
    for year in range(2019, 2025):
        df = get_yearly_data(year)
        dfs.append(df.to_pandas())
        print(f"Data for {year} processed.")
    # Concaténer les résultats de chaque année
    df_final = pd.concat(dfs)
    # Agréger les résultats par mois
    result = df_final.groupby(['SOI_ANN', 'SOI_MOI', 'BEN_SEX_COD', 'AGE_BEN_SNDS', 'BEN_RES_REG'], as_index=False).agg({
        'RAC': 'sum',
        'FLT_PAI_MNT': 'sum',
        'FLT_REM_MNT': 'sum'
    }).sort_values(['SOI_ANN', 'SOI_MOI'])
    output_path = "/raid/datasets/allianzsante/Étude du reste à charge/DBs"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)  # Ensure the directory exists
    result.to_csv(os.path.join(output_path, 'RAC_nouveau_GLM.csv'), index=False)



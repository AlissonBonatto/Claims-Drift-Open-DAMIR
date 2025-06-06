# Ce module sera utilisé pour prétraiter les données du jeu de données Allianz Sante pour les années 2020 à 2024

import time as time
import polars as pl

year = '2023'

DATA = []
months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
start_time = time.time()
for month in months:
    DATA.append(pl.read_csv("./datasets/allianzsante/A{}/A{}{}.csv".format(year, year, month), separator=';', ignore_errors=True))
    print("A{}{}.csv a été chargé".format(year, month))
end_time = time.time()
print("Le temps nécessaire pour le chargement des données est : {}s".format(end_time-start_time))
initial_sizes = {months[i] : data.shape[0] for i, data in enumerate(DATA)}
print("Les tailles initiales des données sont : {}".format(initial_sizes))
## Prétraitement des données

# On enlève toutes les assurances qui ont un code différent de 10 (code correspondant à la maladie)

DATA = [data.filter(pl.col('ASU_NAT') == 10) for data in DATA]
print("Toutes les assurances ayant un code différent de 10 ont été supprimées")

# Filtrage des enveloppes pour ne garder que les enveloppes de type 1, 2, 3, 9, 98 (soins de ville + hospitalisation + hors Ondam)

DATA = [data.filter(pl.col('CPT_ENV_TYP').is_in([1, 2, 3,])) for data in DATA]
print("Toutes les enveloppes qui ne sont pas de type 1, 2, 3 ont été supprimées")

# Filtrage des types de remboursement pour ne prendre en compte que les prestations de référence (0) et les valeurs inconnues (99)

DATA = [data.filter(pl.col('PRS_REM_TYP').is_in([0, 99])) for data in DATA]
print("Tous les types de remboursement qui ne sont pas 0 ou 99 ont été supprimés")

# Filtrage et suppression des lignes avec une valeur de remboursement négative

DATA = [data.filter(pl.col('FLT_REM_MNT') >= 0) for data in DATA]
print("Toutes les lignes avec une valeur de remboursement négative ont été supprimées")

# Filtrage et suppression des lignes avec un taux de remboursement supérieur à 100%

DATA = [data.filter(pl.col('PRS_REM_TAU') <= 100) for data in DATA]
print("Toutes les lignes avec un taux de remboursement supérieur à 100% ont été supprimées")

# Filtrage et suppression des lignes avec un taux de remboursement négatif

DATA = [data.filter(pl.col('PRS_REM_TAU') >= 0) for data in DATA]
print("Toutes les lignes avec un taux de remboursement négatif ont été supprimées")

# Filtrage et suppression des lignes qui contiennent des sexes, âges, codes d'acte, régions inconnus

DATA = [data.filter((pl.col('BEN_SEX_COD').is_in([1, 2])) & (pl.col('AGE_BEN_SNDS') != 99) & (pl.col('PRS_NAT') != 9999) & (pl.col('BEN_RES_REG') != 99)) for data in DATA]
print("Toutes les lignes avec sexe, lieu, code d'acte, âge inconnus ont été supprimées")

# Remplacement de la colonne BEN_QLT_COD qualité du bénéficiaire par les valeurs tq: Si les adhérents de la ligne présentent une qualité de bénéficiaires égale à « 2 » (conjoint et assimilé) ou « 4 » (autre ayant-droit) avec un âge supérieur à 19 ans, la qualité est remplacée par « 1 » (assuré) ; si la qualité est égale à « 4 » (autre ayant-droit) avec un âge inférieur à 19 ans, la qualité est remplacée par « 3 » (enfant); si la qualité est égale à « 9 » (valeur inconnue), la ligne est supprimée

DATA = [data.with_columns(
    pl.when(pl.col('BEN_QLT_COD') == 2).then(1).otherwise(pl.col('BEN_QLT_COD'))
) for data in DATA]

DATA = [data.with_columns(
    pl.when((pl.col('BEN_QLT_COD') == 4) & (pl.col('AGE_BEN_SNDS') != 0)).then(1).otherwise(pl.col('BEN_QLT_COD'))
) for data in DATA]

DATA = [data.with_columns(
    pl.when((pl.col('BEN_QLT_COD') == 4) & (pl.col('AGE_BEN_SNDS') == 0)).then(3).otherwise(pl.col('BEN_QLT_COD'))
) for data in DATA]

DATA = [data.filter(pl.col('BEN_QLT_COD') != 9) for data in DATA]
print("La colonne BEN_QLT_COD a été remplacée et les lignes avec une qualité de bénéficiaire égale à 9 ont été supprimées")

sum = 0
sum_refund = 0
for i, data in enumerate(DATA):
    sum += data.select(pl.col('FLT_PAI_MNT')).sum().get_column('FLT_PAI_MNT').item()
    sum_refund += data.select(pl.col('FLT_REM_MNT')).sum().get_column('FLT_REM_MNT').item()

print("Le montant total des paiements est : {}".format(sum))
print("Le montant total des remboursements est : {}".format(sum_refund))

end_time = time.time()
print("Le temps nécessaire pour le prétraitement des données est : {}s".format(end_time-start_time))

# Il faut que j'ajoute encore quelques étapes de prétraitement pour les données de 2024

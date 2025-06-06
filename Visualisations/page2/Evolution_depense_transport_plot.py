import pandas as pd 
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import polars as pl
import time


# Codes des transports
transport_codes = [
    4203,  # SUPPLEMENT TRANSPORT PERSONNE MOBILITE REDUITE
    4204,  # FORFAIT TRANSPORT URGENCE EXTRAMUROS CPAM MEUSE
    4205,  # FORFAIT TRANSPORT URGENCE INTRAMUROS CPAM MEUSE
    4206,  # PRESTATION FIN DE GARDE AMBULANCE
    4207,  # FORFAIT TRANSPORT D'URGENCE EXPERIMENTATION CPAM AUDE
    4208,  # FORFAIT D'URGENCE SUR APPEL DU SAMU EXPERIMENTATION CPAM BOUCHES-DU-RHONE
    4209,  # COMPLEMENT TRANSPORTS D'URGENCE
    4211,  # SERVICES MOBILES D'URGENCE ET DE REANIMATION (SMUR)
    4212,  # AMBULANCES AGREEES
    4213,  # VEHICULES SANITAIRES LEGERS (VSL)
    4215,  # VEHICULES PERSONNELS
    4216,  # TRANSPORT REEDUCATION PROFESSIONNEL
    4219,  # AUTRES MODES DE TRANSPORT
    4221,  # AMBULANCE AGREEE DE GARDE
    4222,  # INDEMNITE DE GARDE AMBULANCIERE
    4225,  # FORFAIT TRANSPORT PARTAGE PAR 2 PERSONNES
    4226,  # FORFAIT TRANSPORT PARTAGE PAR 3 PERSONNES
    4236,  # AVION EVACUATION SANITAIRE
    4237,  # AVION TRANSPORT SANITAIRE
    4238,  # BATEAU EVACUATION SANITAIRE
    4239,  # BATEAU TRANSPORT SANITAIRE
    4240,  # TRAIN EVACUATION SANITAIRE
    4241,  # TRAIN TRANSPORT SANITAIRE
    9901   # TRANSPORT POUR PERSONNE ACCOMPAGNANTE (MILITAIRES)
]

taxis_codes = [
4210,  # Taxi tarif A
4214,  # TAXIS
4217,  # Taxi tarif B
4218,  # Taxi tarif C
4220,  # Taxi tarif D
4229,  # Taxi tarif F
]


# Initialisation du DataFrame
data = {
    'annee': [],
    'taxis': [],
    'transports': []
}

df = pd.DataFrame(data)

# Chargement des données et aggrégation du montant payé par année
for year in range(2020, 2025):
    DATA = []
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    start_time = time.time()
    for month in months:
        DATA.append(pl.read_csv("../../../datasets/allianzsante/A{}/A{}{}.csv".format(year, year, month), separator=';', ignore_errors=True, columns=['FLT_PAI_MNT', 'PRS_NAT'])) # replace with the correct path to your CSV files before running
        print("A{}{}.csv has been loaded".format(year, month))
    end_time = time.time()
    print("The time needed for the loading of the data of the {} is : {}s".format(year, end_time-start_time))
    # Initialisation des variables
    taxis = 0
    transports = 0

    # Calcul des dépenses pour les taxis
    for data in DATA: 
        taxis += data.filter(pl.col('PRS_NAT').is_in(taxis_codes)).select(pl.sum('FLT_PAI_MNT')).to_pandas().values[0][0]

    # Calcul des dépenses pour les autres transports
    for data in DATA:
        transports += data.filter(pl.col('PRS_NAT').is_in(transport_codes)).select(pl.sum('FLT_PAI_MNT')).to_pandas().values[0][0]

    # Ajout des données dans le DataFrame df
    df.loc[len(df)] = [year, taxis, transports]

df['annee'] = df['annee'].astype(int)
df['taxis'] = df['taxis'].astype(int)
df['transports'] = df['transports'].astype(int)

# Sauvegarde des données dans un fichier CSV
df.to_csv("./CSVs visualisations/Evolution_depense_transport.csv", index=False)



# Visualisation des données
x = df['annee']
y = df['taxis']
z = df['transports']

# Création du graphique empilé
fig, ax = plt.subplots(figsize=(8, 8))

# Ajustement de la largeur des barres
width = 0.7 

# Dessin des barres empilées
ax.bar(x, z, color='deepskyblue', label="Autres transports", width=width)
ax.bar(x, y, bottom=z, color='lightskyblue', label="Taxis", width=width)

# Ajout des valeurs sur les barres
for i in range(len(x)):
    # Texte sur la partie "Taxis"
    plt.text(x[i], z.values[i] / 2, f'{z.values[i] / 1_000_000_000:.1f}', 
             ha='center', va='center', color='black', fontsize=12, fontweight='bold')

    # Texte sur la partie "Autres transports"
    plt.text(x[i], z.values[i] + y.values[i] / 2, f'{y.values[i] / 1_000_000_000:.1f}', 
             ha='center', va='center', color='black', fontsize=12, fontweight='bold')

# Format des valeurs sur l'axe Y en millions €
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x/1_000_000_000:.1f} Md€'))

# Ajout du titre et des labels
plt.xlabel("Année")
plt.ylabel("Dépenses totales (Md€)")
plt.title("Evolution de la dépense en transport (Md€) entre 2020 et 2024")




# Ajout de la légende dans le coin supérieur gauche
plt.legend(loc='upper left')
plt.savefig("./images_visualisation/Evolution_depense_transport")

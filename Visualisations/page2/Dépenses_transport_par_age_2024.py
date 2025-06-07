import pandas as pd 
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import polars as pl
import time
import seaborn as sns

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
    9901,   # TRANSPORT POUR PERSONNE ACCOMPAGNANTE (MILITAIRES)
    4210,  # Taxi tarif A
    4214,  # TAXIS
    4217,  # Taxi tarif B
    4218,  # Taxi tarif C
    4220,  # Taxi tarif D
    4229,  # Taxi tarif F
]

# Chargement des données
year = 2024

DATA = []
months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

start_time = time.time()
for month in months:
    DATA.append(pl.read_csv("../../../datasets/allianzsante/A{}/A{}{}.csv".format(year, year, month), separator=';', ignore_errors=True, columns=['FLT_PAI_MNT', 'AGE_BEN_SNDS', 'PRS_NAT'])) # replace with the correct path to your CSV files before running
    print("A{}{}.csv has been loaded".format(year, month))
end_time = time.time()
print("The time needed for the loading of the data of the {} is : {}s".format(year, end_time-start_time))


# Calcul des dépenses totales de transport par âge
dépenses_totales_transport_par_mois_par_age = []
for i, month in enumerate(months):
    print("mois : ", month)
    dépenses_totales_transport_par_mois_par_age.append(DATA[i].filter(pl.col('PRS_NAT').is_in(transport_codes)).group_by('AGE_BEN_SNDS').agg(pl.sum('FLT_PAI_MNT').alias('Dépenses totales transport')))
    

dépenses_totales_transport_par_age_2024 = pl.concat(dépenses_totales_transport_par_mois_par_age).group_by('AGE_BEN_SNDS').agg(pl.sum('Dépenses totales transport').alias('Dépenses totales transport'))
dépenses_totales_transport_par_age_2024 = dépenses_totales_transport_par_age_2024.to_pandas()
dépenses_totales_transport_par_age_2024 = dépenses_totales_transport_par_age_2024[dépenses_totales_transport_par_age_2024['AGE_BEN_SNDS'] != 99].sort_values('AGE_BEN_SNDS')
dépenses_totales_transport_par_age_2024
    
dépenses_totales_transport_par_age_2024.rename(columns={'AGE_BEN_SNDS': 'Tranche d\'âge'}, inplace=True)
dépenses_totales_transport_par_age_2024.to_csv("./Visualisations/page2/dépenses_totales_transport_par_age_2024.csv", index=False)

# Visualisation des dépenses totales de transport par âge en 2024
sns.set_theme(style="whitegrid")

plt.figure(figsize=(20, 10))

# Création du barplot avec les valeurs en milliards d'euros
sns.barplot(x='Tranche d\'âge', 
            y='Dépenses totales transport', 
            data=dépenses_totales_transport_par_age_2024, 
            color="royalblue")

plt.title('Dépenses totales de transport par âge en 2024', fontsize=20, fontweight='bold')
plt.xlabel("Tranche d'âge", fontsize=15, fontweight='bold')
plt.ylabel('Dépenses totales de transport (Md €)', fontsize=15, fontweight='bold')

# Changer les labels des tranches d'âge
plt.xticks(ticks=range(0, 8), 
           labels=["0-19", "20-29", "30-39", "40-49", "50-59", "60-69", "70-79", "80+"], 
           rotation=45, fontsize=15)

# Format des valeurs monétaires en millions d'euros sur l'axe Y
plt.gca().yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x/1_000_000_000:.1f} Md€'))
plt.show()



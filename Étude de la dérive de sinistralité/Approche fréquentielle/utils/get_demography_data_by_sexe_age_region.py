import pandas as pd
import warnings
warnings.simplefilter('ignore')



def load_demography_data(year, aux_mapping = {}):


    mapping = {
    "Provence-Alpes-Côte d'Azur": "Provence-Alpes-Côte d'Azur et Corse",
    "Corse" : "Provence-Alpes-Côte d'Azur et Corse", 
    "Île-de-France" : "Ile-de-France", 
    "DOM" : "Régions et Départements d'outre-mer"}

    # Load the CSV file
    df = pd.read_csv(f"../../CSV demographie/données démographique entière fin {year}.csv", sep=";", decimal=' ', header=[0, 1])

    df.columns = pd.MultiIndex.from_tuples([(            'Régions',  ""),
                (           'Ensemble',         '0 à 19 ans'),
                ( 'Ensemble',        '20 à 39 ans'),
                ( 'Ensemble',        '40 à 59 ans'),
                ( 'Ensemble',        '60 à 74 ans'),
                ( 'Ensemble',     '75 ans et plus'),
                ( 'Ensemble',              'Total'),
                (             'Hommes',         '0 à 19 ans'),
                ( 'Hommes',        '20 à 39 ans'),
                ( 'Hommes',        '40 à 59 ans'),
                ('Hommes',        '60 à 74 ans'),
                ('Hommes',     '75 ans et plus'),
                ('Hommes',              'Total'),
                (             'Femmes',         '0 à 19 ans'),
                ('Femmes',        '20 à 39 ans'),
                ('Femmes',        '40 à 59 ans'),
                ('Femmes',        '60 à 74 ans'),
                ('Femmes',     '75 ans et plus'),
                ('Femmes',              'Total')],
            )

    df = df[(df.index <= 20)]

    df = df[[t for t in df.columns if t[0] != "Ensemble" and t[1] != 'Total']]

    df[[t for t in df.columns if t[0] != "Régions"]] = df[[t for t in df.columns if t[0] != "Régions"]].apply(lambda x : x.str.replace(' ', '').astype(float) ,axis=1)

    df[('Hommes', '60+')] = df[('Hommes', '60 à 74 ans')] + df[('Hommes', '75 ans et plus')]

    df[('Femmes', '60+')] = df[('Femmes', '60 à 74 ans')] + df[('Femmes', '75 ans et plus')]

    df[('Hommes', '20 à 59 ans')] = df[('Hommes', '20 à 39 ans')] + df[('Hommes', '40 à 59 ans')]

    df[('Femmes', '20 à 59 ans')] = df[('Femmes', '20 à 39 ans')] + df[('Femmes', '40 à 59 ans')]

    df = df[[t for t in df.columns if t[1] != '60 à 74 ans' and t[1] != '75 ans et plus']]

    columns_to_drop = [('Femmes', '40 à 59 ans'), ('Hommes', '40 à 59 ans'), ('Hommes', '20 à 39 ans'), ('Femmes', '20 à 39 ans')]
    columns_to_drop = [col for col in columns_to_drop if col in df.columns]
    df.drop(columns=columns_to_drop, inplace=True)

    df = df[pd.MultiIndex.from_tuples([
                ('Régions',  ""),
                ('Hommes','0 à 19 ans'),
                ('Hommes','20 à 59 ans'),
                ('Hommes', '60+'),
                ('Femmes','0 à 19 ans'),
                ('Femmes','20 à 59 ans'),
                ('Femmes', '60+')
                ],
            )]

    df = df[((df.index > 18) | (df.index < 13)) & (df.index != 20)].reset_index(drop=True)

    df[('Régions','')] = df[('Régions','')].apply(lambda x : mapping[x] if x in mapping else x)

    df[('Régions','')] = df[('Régions','')].apply(lambda x : aux_mapping[x] if x in aux_mapping else x)


    df = df.groupby(('Régions','')).sum().reset_index()


    # Step 2: Set 'Régions' as index
    df = df.set_index(("Régions", ""))


    # Step 3: Stack the column MultiIndex (go from wide to long)

    df = df.stack(level=0).stack(level=0).reset_index()

    
    df.columns = ["Région",  "Sexe", "Tranche d'âge", "Population"]


    # Convert MultiIndex columns to simple columns
    df.columns = df.columns.get_level_values(0)

    df['Annee'] = year

    df['Population'] = df['Population'] * 0.9 # entre 2019 et 2018, le pourcentage prix en compte par le régime général est de 90 %

    df = df[['Annee', "Région",  "Sexe", "Tranche d'âge", "Population"]]

    return df


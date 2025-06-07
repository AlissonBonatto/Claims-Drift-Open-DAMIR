import os
import polars as pl


def get_yearly_data(year):
    # Define the main folder containing subfolders
    main_path = f"Data/A{year}" #replace with the correct path to your CSV files before running

    # List to store results
    all_data = []
    print("start")
    k = 0

    for file in os.listdir(main_path):
        if file.endswith(".csv"):
            file_path = os.path.join(main_path, file)
            use_cols = ['SOI_ANN', 'SOI_MOI', "PRS_REM_TAU", "BEN_SEX_COD", "AGE_BEN_SNDS", "FLT_PAI_MNT", "FLT_REM_MNT", 'ASU_NAT',
                        'BEN_CMU_TOP', 'BEN_QLT_COD',  'CPT_ENV_TYP']
            
            # Read CSV using Polars
            data = pl.read_csv(file_path, separator=';', columns=use_cols, ignore_errors=True)
            
            # Apply filters
            data = data.filter(
                (data['PRS_REM_TAU'] <= 100) &
                (data['FLT_REM_MNT'] >= 0) &
                (data['FLT_PAI_MNT'] > 0)
            )
            
            
            # Cast all columns to float
            data = data.with_columns([data[col].cast(float) for col in data.columns])

            

            # Compute RAC column
            data = data.with_columns((data['FLT_PAI_MNT'] - data['FLT_REM_MNT']).alias("RAC"))
            
            # Filter years between 2019 and 2024
            data = data.filter(data['SOI_ANN'].is_between(2019, 2024))

            data = data.filter(data['BEN_SEX_COD'] != 9)
            
            # Store data
            all_data.append(data)
        k += 1
        print(k)

    # change the type of SOI_ANN and SOI_MOI to int
    

    # Concatenate all files
    df_final = pl.concat(all_data)

    # Aggregate results by month
    result = df_final.group_by(['SOI_ANN', 'SOI_MOI', 'AGE_BEN_SNDS', 'BEN_SEX_COD']).agg([
        pl.sum('RAC').alias('RAC'),
        pl.sum('FLT_PAI_MNT').alias('FLT_PAI_MNT')
    ])
    
    return result


if __name__ == "__main__":
    Dfs = []
    for year in range(2019, 2025):
        Dfs.append(get_yearly_data(year))
        print(f"Data for {year} processed.")
    
    # Concatenate yearly results
    result = pl.concat(Dfs)

    # Final aggregation
    result = result.group_by(['SOI_ANN', 'SOI_MOI', 'AGE_BEN_SNDS', 'BEN_SEX_COD']).agg([
        pl.sum('RAC').alias('RAC'),
        pl.sum('FLT_PAI_MNT').alias('FLT_PAI_MNT')
    ])

    # Sort the result by the same keys as the grouping
    result = result.sort(['SOI_ANN', 'SOI_MOI', 'AGE_BEN_SNDS', 'BEN_SEX_COD'])

    result = result.to_pandas()

    result['SOI_ANN'] = result['SOI_ANN'].astype(int)
    result['SOI_MOI'] = result['SOI_MOI'].astype(int)
    result['AGE_BEN_SNDS'] = result['AGE_BEN_SNDS'].astype(int)
    result['BEN_SEX_COD'] = result['BEN_SEX_COD'].astype(int)

    result.rename(columns={
        'SOI_ANN': 'Year',
        'SOI_MOI': 'Month',
    }, inplace=True)
    # Save to CSV
    result.to_csv(r"/raid/home/allianzsante/ouarhim_you/Sinistrality_prediction/RAC.csv", index= False)


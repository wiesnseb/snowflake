import pandas as pd

def transform_renewable_electricity(df):
    new_df = pd.DataFrame()

    new_df['ID'] = df['ID']
    new_df['YEAR'] = df['Periods'].str[:4]
    new_df['ENERGY_SOURCES_TECHNIQUES'] = df['EnergySourcesTechniques'].str.strip().map({
        'T001028': 'Total renewable energy sources',
        'E006587': 'Hydropower',
        'E006588': 'Wind energy',
        'E006637': 'Onshore wind energy',
        'E006638': 'Offshore wind energy',
        'E006590': 'Solar photovoltaic',
        'E006566': 'Total biomass',
        'E006661': 'Municipal waste; renewable fraction',
        'E006662': 'Co-firing of biomass in electric plants',
        'E006670': 'Biomass boilers companies',
        'E006583': 'Total biogas',
        'E006673': 'Biogas from landfills',
        'E006674': 'Biogas from sewage water purification',
        'E006586': 'Other biogas',
        'E006675': 'Biogas'
    }).fillna('Unknown')
    new_df['GROSS_PRODUCTION_ELECTRICITY_NORMALIZED'] = pd.to_numeric(df['GrossProductionWithNormalisation_1'], errors='coerce')
    new_df['GROSS_PRODUCTION_OF_ELECTRICITY'] = pd.to_numeric(df['GrossProductionOfElectricity_2'], errors='coerce')
    new_df['NET_PRODUCTION_OF_ELECTRICITY'] = pd.to_numeric(df['NetProductionOfElectricity_3'], errors='coerce')
    new_df['GROSS_PRODUCTION_ELECTRICITY_NORMALIZED_IN_PER_CENT_OF_USE'] = pd.to_numeric(df['GrossProductionWithNormalisation_4'], errors='coerce')
    new_df['GROSS_PRODUCTION_OF_ELECTRICITY_IN_PER_CENT_OF_USE'] = pd.to_numeric(df['GrossProductionOfElectricity_5'], errors='coerce')
    new_df['NET_PRODUCTION_OF_ELECTRICITY_IN_PER_CENT_OF_USE'] = pd.to_numeric(df['NetProductionOfElectricity_6'], errors='coerce')
    new_df['INSTALLATION_INSTALLED_END_OF_YEAR'] = pd.to_numeric(df['InstallationsInstalledEndOfYear_7'], errors='coerce')
    new_df['ELECTRICAL_CAPACITY_END_OF_YEAR'] = pd.to_numeric(df['ElectricalCapacityEndOfYear_8'], errors='coerce')

    return new_df
import pandas as pd
import numpy as np

from plugins.utils.timestamp_manager import timestampManager

timestamp_path = 'data/timestamp.txt'
tsmanager = timestampManager(timestamp_path)


def get_hospital_xlsx(file_path: str) -> pd.DataFrame:
    '''Load and transform hospital data from an Excel file.'''
    df_hospital = pd.read_excel(file_path)

    columns_to_drop = ['PROVCODE', 'BED', 'NAME', 'TYPECODE', 'TOTAL']
    df_hospital_transformed = df_hospital.drop(columns = columns_to_drop)

    df_hospital_transformed['MAINCODE'] = df_hospital_transformed['MAINCODE'].astype(str)

    return df_hospital_transformed


def calculate_budgetyear(df_to_transform: pd.DataFrame, date_col: str, budgetyear_col: str) -> pd.DataFrame:
    '''Calculate budget year based on date column.'''
    def get_budgetyear(date):
        if pd.isnull(date):
            return np.nan
        try:
            if date.month > 10 or (date.month == 10 and date.day >= 1):
                return str(date.year + 543 + 1)
            else:
                return str(date.year + 543)
        except pd.errors.OutOfBoundsDatetime:
            return 'Unknown Year'
        except Exception as e:
            print(f'An unexpected error occurred: {e}')
            return np.nan

    df_to_transform[date_col] = pd.to_datetime(df_to_transform[date_col], errors='coerce')
    df_to_transform[budgetyear_col] = df_to_transform[date_col].apply(get_budgetyear)

    return df_to_transform


def calculate_contractfirstdate(date_col: str) -> pd.DataFrame:
    '''Convert contract first date to Buddhist year format.'''
    try:
        date = pd.to_datetime(date_col)
        if pd.notnull(date) and isinstance(date, pd.Timestamp):
            return date.strftime('%Y-%m-%d').replace(str(date.year), str(date.year + 543))
        else:
            return np.nan
    except pd.errors.OutOfBoundsDatetime:
        return date_col
    except Exception as e:
        return np.nan
    

def location_flag_identifier(row, df_col: pd.DataFrame) -> str:
    '''Identify location flag based on latitude and longitude.'''
    if 'LAT' in df_col.columns and 'LON' in df_col.columns:
        if pd.notnull(row['LAT']) and pd.notnull(row['LON']):
            return '1'
    return '0'

    
def final_transformation(medq_path: str, hospital_path: str, output_path: str) -> None:
    '''Perform final transformation and save to CSV.'''
    df_medq = pd.read_csv(medq_path)
    df_medq_newdata = tsmanager.compare_timestamp(lastexe_df=df_medq, lastexe_col='lastUpdate')

    df_hospital = get_hospital_xlsx(file_path=hospital_path)
    df_transformed = pd.merge(df_medq_newdata, df_hospital, left_on = 'hospitalCode', right_on = 'MAINCODE', how = 'left', indicator = False)

    df_transformed['budgetYear'] = df_transformed['budgetYear'].apply(calculate_budgetyear).astype(str)
    df_transformed['contractFirstDate'] = df_transformed['contractFirstDate'].apply(calculate_contractfirstdate).astype(str)
    df_transformed['LOCATION_FLAG'] = df_transformed.apply(location_flag_identifier, axis=1)

    df_transformed['year'] = df_transformed['year'].apply(lambda x: str(int(x) + 543) if pd.notnull(x) and x != np.nan else np.nan)
    df_transformed['priceUnit'] = df_transformed['priceUnit'].astype(str).replace({'0.0': np.nan, 'NaN': np.nan}, inplace=True)
    df_transformed.rename(columns = {'LAT':'LATITUDE', 'LON':'LONGITUDE'}, inplace = True)

    df_transformed['qty'] = '1'
    df_transformed['unitName'] = 'เครื่อง'

    column_order = [
        '_id',
        'MedQ_ID',
        'budgetYear',
        'contractFirstDate',
        'departmentName',
        'districtName',
        'goodsName',
        'methodName',
        'unspscId',
        'qty',
        'productName',
        'unitName',
        'priceUnit',
        'provinceName',
        'winnerName',
        'year',
        'LATITUDE',
        'LONGITUDE',
        'LOCATION_FLAG',
        'PROVINCE_ID',
        'HEALTH_AREA',
        'inventoryTypeID',
        'lastUpdate'
    ]
    df_ordered = df_transformed(column_order)

    df_ordered.to_csv(output_path)


if __name__ == '__main__':

    medq_path = 'data/medq_data.csv'
    hospital_path = 'data/ref_hospital.xlsx'
    output_path = 'data/medq_data_transformed.csv'

    final_transformation(medq_path=medq_path, hospital_path=hospital_path, output_path=output_path)


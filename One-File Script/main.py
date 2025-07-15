from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import io, json, gspread, gspread_dataframe, math, time, requests, os
import pandas as pd
from tqdm import tqdm
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.discovery import Resource
import numpy as np
from google_auth_oauthlib.flow import InstalledAppFlow
import settings

def get_creds(credentials_file: str, token_file: str, scope:list[str]) -> Credentials:
    creds = None
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, scope)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_file, scope)
            creds = flow.run_local_server(port=0)
        with open(token_file, "w") as token:
            token.write(creds.to_json())   
    return creds

def get_cleaned_data():
  def download_file_from_google_drive(id):
      URL = "https://docs.google.com/uc?export=download"
      session = requests.Session()
      response = session.get(URL, params={'id': id}, stream=True)
      return response.content
  cleaned_data_dict:dict = json.loads(download_file_from_google_drive(settings.METADATA_CLEANED_DATA))
  return cleaned_data_dict

def convert_cleaned_data_to_df(cleaned_data:dict):
    read_spreadsheet = lambda id, gid : f"https://docs.google.com/spreadsheets/d/{id}/export?gid={gid}&format=csv"
    
    converted_column = [
                        # "ID.PROVINCE", 
                        "Province",
                        "Indicator ID", 
                        # "Indicator Code", 
                        "2018", "2019", "2020", "2021", "2022", "2023"]
    df_list = []
    for sps_id, sps_data in tqdm(list(cleaned_data.items()), desc="Converting Google Sheets to DataFrame"):
        worksheet_id = sps_data.get('worksheet_id')
        df = pd.read_csv(read_spreadsheet(sps_id, worksheet_id))
        df = df[converted_column]
        if list(df.columns) != converted_column:
            raise Exception(f"Invalid Column Structure from {sps_data["sheet_name"]}")
        df_list.append(df)
    print(f"\n{len(df_list)} DATA CONVERTED TO PANDAS DATAFRAME")
    
    return df_list

def concatenate_cleaned_data(df_list:list[pd.DataFrame]) -> pd.DataFrame:
    concatenated_df = pd.concat(df_list, ignore_index=True)
    
    print(f"CONCATENATED DATAFRAME SHAPE: {concatenated_df.shape}")
    print(f"{len(concatenated_df.Province.unique())} UNIQUE PROVINCE DETECTED")
    print(f"{len(concatenated_df["Indicator ID"].unique())} UNIQUE INDICATOR DETECTED")
    print(f"{len(concatenated_df[~concatenated_df["2018"].isnull()])} NON-EMPTY VALUE DATA FROM 2018")
    print(f"{len(concatenated_df[~concatenated_df["2019"].isnull()])} NON-EMPTY VALUE DATA FROM 2019")
    print(f"{len(concatenated_df[~concatenated_df["2020"].isnull()])} NON-EMPTY VALUE DATA FROM 2020")
    print(f"{len(concatenated_df[~concatenated_df["2021"].isnull()])} NON-EMPTY VALUE DATA FROM 2021")
    print(f"{len(concatenated_df[~concatenated_df["2022"].isnull()])} NON-EMPTY VALUE DATA FROM 2022")
    print(f"{len(concatenated_df[~concatenated_df["2023"].isnull()])} NON-EMPTY VALUE DATA FROM 2023")
    
    return concatenated_df

def melt_cleaned_data(concatenated_df:pd.DataFrame) -> pd.DataFrame:
    melted_df = pd.melt(concatenated_df, 
                        id_vars=["Province", "Indicator ID"], 
                        value_vars=["2018", "2019", "2020", "2021", "2022", "2023"],
                        var_name="Year",
                        value_name="Value")
    melted_df = melted_df.sort_values(by=["Province", "Indicator ID", "Year"])
    melted_df = melted_df.rename(columns={"Province":"Area",
                                          "Indicator ID":"Indicator Code"})
    
    print(f"MELTING DATA BY YEAR FROM {len(concatenated_df)} DATA to {len(melted_df)} DATA")
    print(f"MELTED DATAFRAME SHAPE: {melted_df.shape}")
    
    return melted_df

def get_master_data(creds, master_worksheet:str): # (ID, GID)
  
    client = gspread.authorize(creds)
    
    master_area_client = client.open_by_key(settings.MASTER_AREA_SPSID)
    master_area_worksheet = master_area_client.worksheet(master_worksheet)
    try:
        master_area_df = gspread_dataframe.get_as_dataframe(worksheet=master_area_worksheet, )
    except HttpError:
        print("Quota Exceeded | Wait a minute . . .")
        master_area_df = gspread_dataframe.get_as_dataframe(worksheet=master_area_worksheet, )
    master_area_df["ID"] = master_area_df["ID"].astype("int64").astype(str).apply(lambda x : x.zfill(2))
    
    master_inc_province_client = client.open_by_key(settings.MASTER_INCOME_PROVINCE_SPSID)
    master_inc_province_worksheet = master_inc_province_client.worksheet(master_worksheet)
    try:
        master_inc_province_df = gspread_dataframe.get_as_dataframe(worksheet=master_inc_province_worksheet)
    except HttpError:
        print("Quota Exceeded | Wait a minute . . .")
        master_inc_province_df = gspread_dataframe.get_as_dataframe(worksheet=master_inc_province_worksheet)
        
    master_year_client = client.open_by_key(settings.MASTER_YEAR_SPSID)
    master_year_worksheet = master_year_client.worksheet(master_worksheet)
    master_year_df = gspread_dataframe.get_as_dataframe(worksheet=master_year_worksheet)
    master_year_df.Year = master_year_df.Year.astype(int).astype(str)
    
    master_indicator_client = client.open_by_key(settings.MASTER_INDICATOR_SPSID)
    master_indicator_worksheet = master_indicator_client.worksheet(master_worksheet)
    master_indicator_df = gspread_dataframe.get_as_dataframe(worksheet=master_indicator_worksheet, )
    
    print(f"ROWS OF AREA MASTER DATA : {len(master_area_df)}")
    print(f"ROWS OF PROVINCE INCOME MASTER DATA : {len(master_inc_province_df)}")
    print(f"ROWS OF YEAR MASTER DATA : {len(master_year_df)}")
    print(f"ROWS OF INDICATOR MASTER DATA : {len(master_indicator_df)}")
    
    return master_area_df, master_inc_province_df, master_year_df, master_indicator_df

def cross_merge_master(master_area:pd.DataFrame,
                       master_year:pd.DataFrame, 
                       master_indicator:pd.DataFrame, 
                       column_to_rename:dict):

    province = master_area[["ID", "AREA_NAME"]].drop_duplicates()
    indicator = master_indicator[["Indicator_Code","Indicator_Name", "Unit"]].drop_duplicates()
    year = master_year.Year.astype(str).drop_duplicates()
    
    print(f"FOUND {len(province)} PROVINCE DATA")
    print(f"FOUND {len(indicator)} INDICATOR DATA")
    print(f"FOUND {len(year)} YEAR DATA")
    
    return province.merge(indicator, how='cross').merge(year, how='cross').rename(columns=column_to_rename)

def validate_data_1(cross_merged_df:pd.DataFrame, melted_df:pd.DataFrame):
    print("Ensuring cleaned data is a subset of master data . . .")
    ensure_province:bool = set(melted_df.Area).issubset(cross_merged_df.Area) 
    ensure_year:bool = set(melted_df.Year).issubset(cross_merged_df.Year) 
    ensure_indicator:bool = set(melted_df["Indicator Code"]).issubset(cross_merged_df["Indicator Code"])
    if not (ensure_province and ensure_indicator and ensure_year):
        raise Exception("There('re/'s) Invalid Data from CLEANED DATA")
    
def left_outer_merge_to_master(cross_merged_df:pd.DataFrame, melted_df:pd.DataFrame):
    result = pd.merge(cross_merged_df,
                      melted_df, 
                      on=["Area", "Indicator Code", "Year"], how="left", )

    return result

def convert_value_dataframe(cleaned_data:pd.DataFrame):
    def preprocess_value(unit, value):
        if isinstance(value, float) and math.isnan(value) : return np.nan
        if value == "-" : return np.nan
        match unit:
            case "%":
                return str(value).replace(",", ".")
            case "Average":
                return str(value).replace(",", ".")
            case "Count":
                try:
                    return int(value)
                except ValueError:
                    return str(value).replace(".", "").replace(",", "")
            case "Rupiah":
                return str(value).replace("", "")
            case _:
                raise Exception(f"Invalid Unit '{unit}'")
    
    
    converted_result = cleaned_data.copy(deep=True)
    converted_result.Value = converted_result.apply(lambda row: preprocess_value(row.Unit, row.Value), axis=1).astype(float)
    
    return converted_result 

def write_data_to_sps(creds, sps_id:str, worksheet_name:str, df:pd.DataFrame):
    client = gspread.authorize(creds)

    spreadsheet = client.open_by_key(sps_id)
    worksheet = spreadsheet.worksheet(worksheet_name)
    worksheet.clear()
    
    area_as_a_text_format = {
        "numberFormat": {
            "type": "TEXT"
        }
    }
    num_rows = len(df) + 1
    column_range = f'A2:A{num_rows}'
    worksheet.format(column_range, area_as_a_text_format)
    
    gspread_dataframe.set_with_dataframe(worksheet=worksheet, dataframe=df)
    border_style = {
        "style": "SOLID",
        "width": 1,
    }

    formatting = {
        "borders": {
          "top": border_style,
          "bottom": border_style,
          "left": border_style,
          "right": border_style
        }
    }
    num_rows = len(df) + 1
    num_cols = len(df.columns)
    range_to_border = f"A1:{gspread.utils.rowcol_to_a1(num_rows, num_cols)}"
    worksheet.format(range_to_border, formatting)
    
    bold_format = {
        "textFormat":{
            "bold":True
        }
    }
    range_to_bold = f"A1:{gspread.utils.rowcol_to_a1(1, len(df.columns))}"
    worksheet.format(range_to_bold, bold_format)
    
    requests = [
        {
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": worksheet.id, # The numeric ID of the worksheet
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": len(df.columns)
                }
            }
        }
    ]

    # Send the batch update request to the spreadsheet
    spreadsheet.batch_update(body={'requests': requests})
    
    return df

def get_dataframe_from_sheet(creds, sps_id:str, worksheet_name:str):
    client = gspread.authorize(creds)

    spreadsheet = client.open_by_key(sps_id)
    worksheet = spreadsheet.worksheet(worksheet_name)
    df = gspread_dataframe.get_as_dataframe(worksheet=worksheet)
    
    return df

def handle_dim_year(creds:Credentials,
                    dim_year:pd.DataFrame,
                    master_year_df:pd.DataFrame,
                    default_null_value:str) -> pd.DataFrame:
    dim_year[["year"]] = dim_year[["year"]].astype(int).astype(str)
    
    # Implementation of SCD TYPE 1
    rename_source_year = master_year_df.fillna(default_null_value).rename(columns={"Year":"year", "Notes":"note"})
    drop_duplicates_source_year = rename_source_year.drop_duplicates(subset=["year"], keep="last").set_index("year")
    dim_year_buskey_as_index = dim_year.set_index("year")
    dim_year_buskey_as_index.update(drop_duplicates_source_year)
    
    # ADD NEW DATA
    new_year = drop_duplicates_source_year[~drop_duplicates_source_year.index.isin(dim_year_buskey_as_index.index)]
    concatenated_dim_year = pd.concat([dim_year_buskey_as_index, new_year])
    
    # WRITE TO SPS
    final_dim_year = concatenated_dim_year.reset_index()
    final_dim_year.insert(0, 'id', range(1, len(final_dim_year)+1))
    write_data_to_sps(creds, settings.WAREHOUSE_DATA_SPS_ID, "dim_year", final_dim_year)
    return final_dim_year

def handle_dim_location(creds:Credentials, 
                        dim_location:pd.DataFrame,
                        master_area_df:pd.DataFrame,
                        master_inc_province_df:pd.DataFrame,
                        default_null_value:str) -> pd.DataFrame:
    
    # HANDLE dim_location
    if not set(master_inc_province_df["Provinsi"]).issubset(set(master_area_df["AREA_NAME"])):
        raise Exception("Invalid Province Name Found")

    dim_location["area_code"] = dim_location["area_code"].astype(int).astype(str).apply(lambda dt: dt.zfill(2))

    # RENAMED MASTER
    renamed_master_inc =  master_inc_province_df.rename(columns={"Provinsi":"area_name",
                                                        "Tingkat_Pendapatan":"income_level_name",
                                                        "ID_Pendapatan":"income_level_code"})
    renamed_master_area = master_area_df.rename(columns={ "ID":"area_code",
                                                "AREA_NAME":"area_name",
                                                "AREA_TYPE":"area_type",
                                                "REGION_GROUP":"region_name",
                                                "ID_REGION":"region_code"
                                                })

    # MERGE AREA
    merged_area =  renamed_master_area.merge(renamed_master_inc,on=["area_name"], how="left")[dim_location.columns]

    # Implementation of SCD TYPE 1
    rename_source_area = merged_area.fillna(default_null_value)
    drop_duplicates_source_area = rename_source_area.drop_duplicates(subset=["area_code"], keep="last").set_index("area_code")
    dim_location_buskey_as_index = dim_location.set_index("area_code")
    dim_location_buskey_as_index.update(drop_duplicates_source_area)

    # ADD NEW DATA
    new_location = drop_duplicates_source_area[~drop_duplicates_source_area.index.isin(dim_location_buskey_as_index.index)]
    concatenated_dim_location = pd.concat([dim_location_buskey_as_index, new_location])

    # WRITE TO SPS
    final_dim_location = concatenated_dim_location.reset_index()
    final_dim_location.insert(0, 'id', range(1, len(final_dim_location)+1))
    write_data_to_sps(creds, settings.WAREHOUSE_DATA_SPS_ID, "dim_location", final_dim_location)
    
    return final_dim_location

def handle_dim_indicator(creds:Credentials, 
                        dim_indicator:pd.DataFrame,
                        master_indicator_df:pd.DataFrame,
                        default_null_value:str) -> pd.DataFrame:
    
    # IMPLEMENTATION OF SCD TYPE 1
    rename_source_indicator = master_indicator_df.fillna(default_null_value).rename(columns={"Indicator_Code":"indicator_code",
                                                                                            "Indicator_Name":"indicator_name",
                                                                                            "Theme":"theme_name",
                                                                                            "Technology":"technology_name",
                                                                                            "Tech_ID":"technology_code",
                                                                                            "Category":"category_name",
                                                                                            "Category_ID":"category_code",
                                                                                            "Unit":"unit",
                                                                                            "Category_ID.1":"new_category_code",
                                                                                            "New_Category":"new_category_name"})[dim_indicator.columns]
    drop_duplicates_source_indicator = rename_source_indicator.drop_duplicates(subset=["indicator_code"], keep="last").set_index("indicator_code")
    dim_indicator_buskey_as_index = dim_indicator.set_index("indicator_code")
    dim_indicator_buskey_as_index.update(drop_duplicates_source_indicator)


    # ADD NEW DATA
    new_indicator = drop_duplicates_source_indicator[~drop_duplicates_source_indicator.index.isin(dim_indicator_buskey_as_index.index)]
    concatenated_dim_indicator = pd.concat([dim_indicator_buskey_as_index, new_indicator])

    # WRITE TO SPS
    final_dim_indicator = concatenated_dim_indicator.reset_index()
    final_dim_indicator.insert(0, 'id', range(1, len(final_dim_indicator)+1))
    write_data_to_sps(creds, settings.WAREHOUSE_DATA_SPS_ID, "dim_indicator", final_dim_indicator)
    
    return final_dim_indicator

def handle_fact_value(creds:Credentials,
                      fact_value:pd.DataFrame,
                      dim_location:pd.DataFrame,
                      dim_indicator:pd.DataFrame,
                      dim_year:pd.DataFrame,) -> pd.DataFrame:
    fact_it_eco_column = [
        "dim_year_id",
        "dim_indicator_id",
        "dim_location_id",
        "value"
    ]
    renamed_source = fact_value.rename(columns={"Area Code":"area_code",
                                                   "Area":"area_name",
                                                   "Indicator Code":"indicator_code",
                                                   "Indicator Name":"indicator_name",
                                                   "Unit":"unit",
                                                   "Year":"year",
                                                   "Value":"value"})


    # FORMAT SOURCE
    renamed_source["area_code"] = renamed_source["area_code"].astype(int).astype(str).apply(lambda x : x.zfill(2))
    renamed_source["year"] = renamed_source["year"].astype(int).astype(str)

    # renamed_source
    enriched_df = pd.merge(renamed_source.drop(columns="area_name"), dim_location, on="area_code").rename(columns={"id":"dim_location_id"})
    enriched_df = pd.merge(enriched_df, dim_year, on="year").rename(columns={"id":"dim_year_id"})
    enriched_df = pd.merge(enriched_df.drop(columns=["indicator_name", "unit"]), dim_indicator, on="indicator_code").rename(columns={"id":"dim_indicator_id"})

    fact_table = enriched_df[fact_it_eco_column]
    write_data_to_sps(creds, settings.WAREHOUSE_DATA_SPS_ID, "fact_it_ecosystem", fact_table)
    
    return fact_table

if __name__ == "__main__":
    # Load settings
    scope = settings.SCOPES
    
    # Get credentials
    print("Loading credentials . . .")
    creds:Credentials = get_creds("credentials.json", "token.json", scope)
    print("Credentials loaded successfully.\n")
    
    # Get all cleaned data
    print("\nLoading cleaned data . . .")
    cleaned_data:dict = get_cleaned_data()
    print(f"Loaded {len(cleaned_data)} cleaned data files.\n")
    
    # Convert cleaned data to DataFrame
    print("Converting cleaned data to DataFrame . . .")
    print("This may take a while . . .")
    df_list:list[pd.DataFrame] = convert_cleaned_data_to_df(cleaned_data)
    print(f"Converted {len(df_list)} DataFrames from cleaned data.\n")
    
    # Concatenate cleaned data
    print("\nConcatenating cleaned data . . .")
    concatenated_df:pd.DataFrame = concatenate_cleaned_data(df_list)
    print(f"Concatenated data shape: {concatenated_df.shape}\n")
    
    # Melt cleaned data
    print("Melting cleaned data . . .")
    melted_df:pd.DataFrame = melt_cleaned_data(concatenated_df)
    print(f"Melted data shape: {melted_df.shape}\n")
    
    # Get master data
    print("Loading master data . . .")
    master_area_df, master_inc_province_df, master_year_df, master_indicator_df = get_master_data(creds, settings.MASTER_WORKSHEET)
    print(f"Master data loaded successfully.\n")
    
    # Cross merge master data
    column_to_rename = {
        "ID":"Area Code",
        "AREA_NAME":"Area",
        "Indicator_Code":"Indicator Code",
        "Indicator_Name":"Indicator Name",
        "Unit":"Unit"
    }
    print("\nCross merging master data . . .")
    cross_merged_df = cross_merge_master(master_area_df, master_year_df, master_indicator_df, column_to_rename)
    print(f"Cross merged data shape: {cross_merged_df.shape}\n")
    
    # Validate data
    print("\nValidating data . . .")
    validate_data_1(cross_merged_df, melted_df)
    print("Data validation passed.\n")
    
    # Left outer merge to master
    print("\nLeft outer merging cleaned data to master data . . .")
    result = left_outer_merge_to_master(cross_merged_df, melted_df)
    print(f"Result shape: {result.shape}\n")
    
    # Convert value dataframe
    print("\nConverting value dataframe . . .")
    converted_result = convert_value_dataframe(result)
    print(f"Converted result shape: {converted_result.shape}\n")
    # Write data to SPS
    print("\nWriting data to SPS . . .")
    print(f"Writing {len(converted_result)} rows of data to {settings.MERGED_DATA_SPS_ID} . . .")
    converted_data_from_sps = write_data_to_sps(creds, settings.MERGED_DATA_SPS_ID, "main",converted_result )
    print(f"Data written to {settings.MERGED_DATA_SPS_ID} successfully.\n")
    
    # Call dimension and fact value
    print("\nHandling dimension and fact value . . .")
    converted_and_merged_data = get_dataframe_from_sheet(creds, settings.MERGED_DATA_SPS_ID, "main")
    dim_location = get_dataframe_from_sheet(creds, settings.WAREHOUSE_DATA_SPS_ID, "dim_location").drop(columns="id")
    dim_indicator = get_dataframe_from_sheet(creds, settings.WAREHOUSE_DATA_SPS_ID, "dim_indicator").drop(columns="id")
    dim_year = get_dataframe_from_sheet(creds, settings.WAREHOUSE_DATA_SPS_ID, "dim_year").drop(columns="id")
    
    # Handle dim_year
    print("\nHandling dim_year . . .")
    final_dim_year = handle_dim_year(creds, dim_year, master_year_df, settings.DEFAULT_NULL_VALUE)
    print(f"Final dim_year shape: {final_dim_year.shape}")
    print(f"Final dim_year columns: {final_dim_year.columns.tolist()}")
    print(f"Final dim_year data: {final_dim_year.head()}")
    print(f"Final dim_year data types: {final_dim_year.dtypes}")
    print(f"Final dim_year null values: {final_dim_year.isnull().sum()}")
    print(f"Final dim_year unique values: {final_dim_year.nunique()}\n")
    
    # Handle dim_location
    print("\nHandling dim_location . . .")
    final_dim_location = handle_dim_location(creds, dim_location, master_area_df, master_inc_province_df, settings.DEFAULT_NULL_VALUE)
    print(f"Final dim_location shape: {final_dim_location.shape}")
    print(f"Final dim_location columns: {final_dim_location.columns.tolist()}")
    print(f"Final dim_location data: {final_dim_location.head()}")
    print(f"Final dim_location data types: {final_dim_location.dtypes}")
    print(f"Final dim_location null values: {final_dim_location.isnull().sum()}")
    print(f"Final dim_location unique values: {final_dim_location.nunique()}\n")
    
    # Handle dim_indicator
    print("\nHandling dim_indicator . . .")
    final_dim_indicator = handle_dim_indicator(creds, dim_indicator, master_indicator_df, settings.DEFAULT_NULL_VALUE)
    print(f"Final dim_indicator shape: {final_dim_indicator.shape}")
    print(f"Final dim_indicator columns: {final_dim_indicator.columns.tolist()}")
    print(f"Final dim_indicator data: {final_dim_indicator.head()}")
    print(f"Final dim_indicator data types: {final_dim_indicator.dtypes}")
    print(f"Final dim_indicator null values: {final_dim_indicator.isnull().sum()}")
    print(f"Final dim_indicator unique values: {final_dim_indicator.nunique()}\n")
    
    # Handle fact_value
    print("\nHandling fact_value . . .")
    final_fact_value = handle_fact_value(creds, converted_and_merged_data, final_dim_location, final_dim_indicator, final_dim_year)
    print(f"Final fact_value shape: {final_fact_value.shape}")
    print(f"Final fact_value columns: {final_fact_value.columns.tolist()}")
    print(f"Final fact_value data: {final_fact_value.head()}")
    print(f"Final fact_value data types: {final_fact_value.dtypes}")
    print(f"Final fact_value null values: {final_fact_value.isnull().sum()}")
    print(f"Final fact_value unique values: {final_fact_value.nunique()}\n")
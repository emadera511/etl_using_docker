import logging 

import pandas as pd 


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def read_table(engine, table_name): 

    try: 
        df = pd.read_sql_query(
            f'select * from "landing_area"."{table_name}"', engine


        )
        print(table_name)
        logger.info("Table read from the landing area!!!!")

        return df 
    except Exception as e: 
        logger.error('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        logger.error(f'Unable to read the data from landing area {e}')

def clean_data(df): 
    try: 

        df['SUPPLIER'] = df["SUPPLIER"].fillna("NO SUPPLIER")
        df["ITEM TYPE"] = df["ITEM TYPE"].fillna("NO ITEM TYPE")
        df["RETAIL SALES"] = df["RETAIL SALES"].fillna(-1)

        logger.info("Cleaned Data")

        return df 

    except Exception as e: 
        logger.error(f"Error Cleaning the data {e}")

def create_schema(df): 

    try: 
        supplier_df = df[['SUPPLIER']]
        supplier_df = supplier_df.drop_duplicates() 
        supplier_df = supplier_df.reset_index(drop=True)
        supplier_df = supplier_df.reset_index(names="SUPPLIER_ID")
        supplier_df["SUPPLIER_ID"] += 1 

        logger.info("Supplier table created")

    except Exception as e: 
        logger.error(f"Error creating table {e}")



    try: 

        item_df = df[["ITEM CODE", "ITEM TYPE", "ITEM DESCRIPTION"]]
        item_df = item_df.drop_duplicates() 
        item_df = item_df.rename(
            columns = {
                "ITEM CODE": "ITEM_CODE", 
                "ITEM TYPE": "ITEM_TYPE", 
                "ITEM DESCRIPTION": "ITEM_DESCRIPTION" 
            }
        )

        

        logger.info("Created Items table")

    except Exception as e: 
        logger.error("Error creating table {e}")

    try: 


        date_df = df[["YEAR", "MONTH"]]
        date_df = date_df.drop_duplicates() 
        date_df = date_df.reset_index(drop=True)
        date_df = date_df.reset_index(names="DATE_ID")
        date_df["DATE_ID"] += 1 

        logger.info("Date table created")

    except Exception as e: 
        logger.error("Error creating table {e}")

    try: 
        
        logger.info("Start Building Fact Table")
        fact_table = ( 
            df.merge(supplier_df, on="SUPPLIER")
            .merge(item_df, left_on="ITEM CODE", right_on="ITEM_CODE")
            .merge(date_df, on=["YEAR", "MONTH"])[
            [
                "ITEM_CODE", 
                "SUPPLIER_ID", 
                "DATE_ID",
                "RETAIL SALES",
                "RETAIL TRANSFERS", 
                "WAREHOUSE SALES"
                
            ] 

            ]
        )

        fact_table = fact_table.drop_duplicates() 

        logger.info("Fact table created")

        return {
            "supplier": supplier_df.to_dict(orient="dict"),
            "itme": item_df.to_dict(orient="dict"), 
            "date": date_df.to_dict(orient="dict"),
            "fact_tabel": fact_table.to_dict(orient="dict")
        }
    except Exception as e: 
        logger.error("Error creating table {e}")

def load_tables_staging(dict, engine): 

    try: 
        for df_name, value_dict in dict.items(): 
            value_df = pd.DataFrame(value_dict)
            logger.info(
                f"Importing {len(value_df)} rows from"
                f"landing_area to staging_area.{df_name}"
            )

            value_df.to_sql( 
                df_name, 
                engine, 
                if_exists='replace',
                index=False,
                schema="staging_area",
            )

            logger.info("!!!!!!!!!!!!!!!!!!")
            logger.info(f"Table {df_name} loaded successfully")

    except Exception as e: 
        logger.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        logger.error(f"Unable to load the data to stage area {e}")
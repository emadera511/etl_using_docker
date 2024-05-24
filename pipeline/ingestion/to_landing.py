import logging 

logging.basicConfig(
    level=logging.INFO, 
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s", 

)

logger = logging.getLogger(__name__)


def load_to_landing(df, engine, table_name): 

    try: 
        df.to_sql(
            table_name, 
            engine, 
            if_exists = 'replace', 
            index = False, 
            schema = "landing_zone"
        )

    except Exception as e: 
        logger.error("!!!!!!!!!!!!!!!!!!!!!!!")
        logger.error(f"Unable to load data: {e}")
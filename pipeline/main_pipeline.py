import os

import pandas as pd
from connection import close_conn, create_conn
from ingestion.to_landing import load_to_landin
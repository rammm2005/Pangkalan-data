import PyPDF2
import pandas as pd
from sqlalchemy import create_engine, text
import os
import re

# File paths
pdf_file_path = os.path.join(os.getcwd(), 'aali.pdf')
excel_file_path = os.path.join(os.getcwd(), 'aali.xlsx')

# Load all sheets in the Excel file
try:
    sheets_dict = pd.read_excel(excel_file_path, sheet_name=None, header=None)  # Load all sheets without headers
    
    # Display the sheet names and the first few rows of each sheet for inspection
    print("Loaded sheets:")
    for sheet_name, sheet_df in sheets_dict.items():
        print(f"\nSheet name: {sheet_name}")
        print(sheet_df.head())  # Display the top rows of each sheet

    # Set 'kode emiten' and 'quartal' directly
    kode_emiten = 'AALI'  # Set the entity code to 'aali' as specified
    quartal = 'Q4'  # Set quartal to Q4 or adjust if needed

    print("Kode Emiten:", kode_emiten)
    print("Quartal:", quartal)

except FileNotFoundError:
    print("Excel file not found.")
    exit(1)
except Exception as e:
    print(f"Error reading Excel file: {e}")
    exit(1)

# Load PDF and extract CALK and Q4 data
def extract_text_from_pages(pdf_reader, pages):
    text = ""
    for page_num in pages:
        page = pdf_reader.pages[page_num - 1]
        text += page.extract_text()
    return text

try:
    with open(pdf_file_path, 'rb') as pdf_file:
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        calk_text = extract_text_from_pages(pdf_reader, list(range(190, 211)))
        q4_text = extract_text_from_pages(pdf_reader, list(range(184, 187)))

except FileNotFoundError:
    print(f"PDF file not found: {pdf_file_path}")
    exit(1)
except Exception as e:
    print(e)
    exit(1)

# Function to parse CALK data into DataFrame
def parse_calk_to_dataframe(text):
    rows = text.split("\n")
    data = []
    for row in rows:
        cells = re.split(r'\s{2,}', row)  # Split by two or more spaces
        data.append(cells)
    return pd.DataFrame(data)

# Parsing CALK text into DataFrame
df_calk = parse_calk_to_dataframe(calk_text)

# Add 'kode emiten' and 'quartal' to the DataFrame
df_calk['kode emiten'] = kode_emiten
df_calk['quartal'] = quartal

# Debugging: Ensure DataFrame is correctly populated
print("CALK DataFrame:")
print(df_calk.head())  # Print the first few rows to check the data

# MySQL configuration
host = 'localhost'
user = 'root'
database = 'pangkalan_data'

mysql_uri = f'mysql+mysqlconnector://{user}@{host}/'
engine = create_engine(mysql_uri)

try:
    # Create the database if it doesn't exist
    with engine.connect() as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS {database}"))
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {database}"))

    # Reconnect to the database after creation
    mysql_uri = mysql_uri + database
    engine = create_engine(mysql_uri)

    # Save CALK DataFrame to MySQL
    df_calk.to_sql('laporan_calk', engine, if_exists='replace', index=False)

    print("CALK data successfully added to MySQL.")
except Exception as e:
    print(f"Error inserting CALK data into MySQL: {e}")

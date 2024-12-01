import PyPDF2
import pandas as pd
from sqlalchemy import create_engine, text
import os
import re

# File paths for the PDF and Excel files
pdf_file_path = os.path.join(os.getcwd(), 'aali.pdf')
excel_file_path = os.path.join(os.getcwd(), 'aali.xlsx')

# Load all sheets from the Excel file without headers
try:
    sheets_dict = pd.read_excel(excel_file_path, sheet_name=None, header=None)  # Load all sheets
    
    # Display the sheet names and the first few rows of each sheet for inspection
    print("Loaded sheets:")
    for sheet_name, sheet_df in sheets_dict.items():
        print(f"\nSheet name: {sheet_name}")
        print(sheet_df.head())  # Display the top rows for each sheet

    # Set 'kode emiten' and 'quartal' directly for future use in the data
    kode_emiten = 'AALI'  # Set the entity code to 'AALI'
    quartal = 'Q4'  # Set quartal to 'Q4'

    print("Kode Emiten:", kode_emiten)
    print("Quartal:", quartal)

except FileNotFoundError:
    print("Excel file not found.")
    exit(1)
except Exception as e:
    print(f"Error reading Excel file: {e}")
    exit(1)

# Function to extract text from specified pages of the PDF
def extract_text_from_pages(pdf_reader, pages):
    text = ""
    for page_num in pages:
        page = pdf_reader.pages[page_num - 1]  # Pages in PyPDF2 are zero-indexed
        text += page.extract_text()
    return text

# Extract CALK and Q4 text from specific pages of the PDF
try:
    with open(pdf_file_path, 'rb') as pdf_file:
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        
        # Extract text from the CALK pages (190-210) and Q4 pages (184-186)
        calk_text = extract_text_from_pages(pdf_reader, list(range(190, 211)))
        q4_text = extract_text_from_pages(pdf_reader, list(range(184, 187)))

except FileNotFoundError:
    print(f"PDF file not found: {pdf_file_path}")
    exit(1)
except Exception as e:
    print(f"Error extracting PDF data: {e}")
    exit(1)

# Function to parse the CALK data into a DataFrame with the required headers
def parse_calk_to_dataframe(text):
    rows = text.split("\n")  # Split text into rows based on newline character
    data = []
    for row in rows:
        # Split each row by two or more spaces to separate the data fields
        cells = re.split(r'\s{2,}', row)
        
        # Check if the row has enough columns and format it properly
        if len(cells) >= 5:  # Ensure there are enough cells for Value, Item, Notes, etc.
            nama = cells[0].strip() if len(cells) > 0 else ""
            no_emiten = kode_emiten  # Fixed value for No Emiten
            quartal_value = quartal  # Fixed value for Quartal
            value = cells[1].strip() if len(cells) > 1 else ""  # Value field
            item = cells[2].strip() if len(cells) > 2 else ""  # Item field
            notes = " ".join(cells[3:]).strip() if len(cells) > 3 else ""  # Notes field
            
            # Add to data list
            data.append([nama, no_emiten, quartal_value, value, item, notes])
    
    # Convert the list of data into a DataFrame with the required columns
    return pd.DataFrame(data, columns=['Nama', 'No Emiten', 'Kuartal', 'Value', 'Item', 'Notes'])

# Parse the extracted CALK text into a DataFrame
df_calk = parse_calk_to_dataframe(calk_text)

# Display the first few rows of the CALK DataFrame for debugging
print("CALK DataFrame:")
print(df_calk.head())

# MySQL database configuration
host = 'localhost'  # MySQL host
user = 'root'  # MySQL user
database = 'pangkalan_data'  # Name of the database

# Create the MySQL URI for connection
mysql_uri = f'mysql+mysqlconnector://{user}@{host}/'
engine = create_engine(mysql_uri)

# Try creating the database and saving the CALK data to MySQL
try:
    # Create the database if it doesn't exist
    with engine.connect() as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS {database}"))  # Drop the database if it exists
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {database}"))  # Create the database

    # Reconnect to the database after creation
    mysql_uri = mysql_uri + database
    engine = create_engine(mysql_uri)

    # Save the CALK DataFrame to MySQL (replace table if it exists)
    df_calk.to_sql('laporan_calk', engine, if_exists='replace', index=False)

    print("CALK data successfully added to MySQL.")

except Exception as e:
    print(f"Error inserting CALK data into MySQL: {e}")

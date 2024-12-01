import os
import re
import pandas as pd
from sqlalchemy import create_engine, text
from PyPDF2 import PdfReader

# === CONFIGURATION === #
PDF_FILE = 'resource/aali.pdf'
EXCEL_FILE = 'resource/aali.xlsx'
DB_HOST = 'localhost'
DB_USER = 'root'
DB_NAME = 'pangkalan_data'
KODE_EMITEN = 'AALI'
KUARTAL = 'Q4'

# === FUNCTIONS === #

def load_excel_sheets(file_path):
    """Load all sheets from an Excel file without headers."""
    try:
        return pd.read_excel(file_path, sheet_name=None, header=None)
    except FileNotFoundError:
        print(f"Excel file not found: {file_path}")
        exit(1)
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        exit(1)

def extract_text_from_pdf(file_path, pages):
    """Extract text from specified pages of a PDF."""
    try:
        with open(file_path, 'rb') as pdf_file:
            pdf_reader = PdfReader(pdf_file)
            text = ""
            for page_num in pages:
                text += pdf_reader.pages[page_num - 1].extract_text()
            return text
    except FileNotFoundError:
        print(f"PDF file not found: {file_path}")
        exit(1)
    except Exception as e:
        print(f"Error extracting PDF data: {e}")
        exit(1)

def parse_text_to_dataframe(text):
    """Parse CALK text into a DataFrame with specific headers."""
    rows = text.split("\n")
    data = []
    for row in rows:
        cells = re.split(r'\s{2,}', row)
        if len(cells) >= 5:
            data.append([
                cells[0].strip(),                # Nama
                KODE_EMITEN,                    # No Emiten (fixed)
                KUARTAL,                        # Kuartal (fixed)
                cells[1].strip(),               # Value
                cells[2].strip(),               # Item
                " ".join(cells[3:]).strip()     # Notes
            ])
    return pd.DataFrame(data, columns=['Nama', 'No Emiten', 'Kuartal', 'Value', 'Item', 'Notes'])

def save_to_mysql(df, table_name, host, user, db_name):
    """Save a DataFrame to a MySQL database."""
    mysql_uri = f'mysql+mysqlconnector://{user}@{host}/'
    engine = create_engine(mysql_uri)
    try:
        with engine.connect() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS {db_name}"))
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))
        
        mysql_uri += db_name
        engine = create_engine(mysql_uri)
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data successfully added to table '{table_name}' in database '{db_name}'.")
    except Exception as e:
        print(f"Error saving data to MySQL: {e}")
        exit(1)

# === MAIN SCRIPT === #
if __name__ == "__main__":
    # Load Excel sheets for inspection
    sheets = load_excel_sheets(EXCEL_FILE)
    print("Loaded Excel sheets:")
    for sheet_name, sheet_df in sheets.items():
        print(f"\nSheet: {sheet_name}\n{sheet_df.head()}")
    
    print(f"\nKode Emiten: {KODE_EMITEN}, Kuartal: {KUARTAL}\n")

    # Extract CALK text from PDF (pages 190-210) and Q4 text (pages 184-186)
    calk_text = extract_text_from_pdf(PDF_FILE, range(190, 211))
    q4_text = extract_text_from_pdf(PDF_FILE, range(184, 187))

    # Parse extracted text into DataFrame
    df_calk = parse_text_to_dataframe(calk_text)
    print("\nCALK DataFrame:")
    print(df_calk.head())

    # Save DataFrame to MySQL database
    save_to_mysql(df_calk, 'laporan_calk', DB_HOST, DB_USER, DB_NAME)

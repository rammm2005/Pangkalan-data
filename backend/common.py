import os
from dotenv import load_dotenv
import re
import pandas as pd
from sqlalchemy import create_engine, text
from PyPDF2 import PdfReader

load_dotenv()

# === CONFIGURATION === #
PDF_FILE = os.getenv('PDF_FILE')
EXCEL_FILE = os.getenv('EXCEL_FILE')
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_NAME = os.getenv('DB_NAME')
TABLE_NAME = os.getenv('TABLE_NAME')

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
            print("Extracted Text from PDF (first 1000 characters):")
            print(text[:1000])  # nge print 1000 kata dlu buar di verifikasi
            return text
    except FileNotFoundError:
        print(f"PDF file not found: {file_path}")
        exit(1)
    except Exception as e:
        print(f"Error extracting PDF data: {e}")
        exit(1)

def extract_metadata_from_pdf(text):
    """Extract 'Nama Emiten' and 'Kode Emiten' from the PDF text."""
    nama_emiten_match = re.search(r"Nama Emiten[:\s]*([A-Za-z0-9\s]+(?:\s*PT\s*[A-Za-z0-9\s]*)?)", text, re.IGNORECASE)
    kode_emiten_match = re.search(r"Kode Emiten[:\s]*(\w+)", text, re.IGNORECASE)
    kuartal_match = re.search(r"Kuartal\s*(I|II|III|IV|V|VI|VII|\d{1,2)", text, re.IGNORECASE)
    notes_match = re.search(r"Catatan\s*([\d]+[a-z]?)", text, re.IGNORECASE)

    if nama_emiten_match:
        print(f"Nama Emiten Found: {nama_emiten_match.group(1)}")
    else:
        print("Nama Emiten not found.")
    
    if kode_emiten_match:
        print(f"Kode Emiten Found: {kode_emiten_match.group(1)}")
    else:
        print("Kode Emiten not found.")
        
    if kuartal_match:
        print(f"Kuartal Found: {kuartal_match.group(1)}")
    else:
        print("Kuartal not found.")

    if notes_match:
        print(f"Notes Found: {notes_match.group(1)}")
    else: 
        print(f"Notes Not Found") 
    
    nama_emiten = nama_emiten_match.group(1) if nama_emiten_match else "Unknown"
    kode_emiten = kode_emiten_match.group(1) if kode_emiten_match else "Unknown"
    kuartal = kuartal_match.group(1) if kuartal_match else "Unknown"
    notes = notes_match.group(1) if notes_match else "Unknown"
    return nama_emiten, kode_emiten, kuartal, notes

def parse_text_to_dataframe(text, nama_emiten, kode_emiten, kuartal, notes):
    """Parse CALK text into a DataFrame with specific headers."""
    rows = text.split("\n")
    data = []
    for row in rows:
        print(f"Row: {row}")  # Debug: lihat setiap baris teks yang diproses
        cells = row.split()
        if len(cells) >= 5:  
            print(f"Parsed Cells: {cells}")  # Debug: lihat hasil pemisahan
            data.append([
                nama_emiten,                   # Nama Emiten (fixed)
                kode_emiten,                   # Kode Emiten (fixed)
                kuartal,                       # Kuartal (fixed)
                cells[0].strip(),              # Value
                cells[1].strip(),              # Item
                notes                          # Notes
            ])
    return pd.DataFrame(data, columns=['Nama Emiten', 'Kode Emiten', 'Kuartal', 'Value', 'Item', 'Notes'])

def save_to_mysql(df, table_name, host, user, db_name):
    """Save a DataFrame to a MySQL database."""
    mysql_uri = f'mysql+mysqlconnector://{user}@{host}/'
    engine = create_engine(mysql_uri)
    try:
        with engine.connect() as conn:
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
        print(f"\nSheet: {sheet_name}\n", sheet_df.head()) 
    
    # Extract text from the PDF and metadata
    pdf_text = extract_text_from_pdf(PDF_FILE, pages=[384 , 385, 386, 387])  # cathing data from the PDF of the page (jadi range halamannnya di check)
    nama_emiten, kode_emiten, kuartal, notes = extract_metadata_from_pdf(pdf_text)
    
    # Parse the extracted text into a DataFrame
    df = parse_text_to_dataframe(pdf_text, nama_emiten, kode_emiten, kuartal, notes)
    print("\nParsed DataFrame:")
    print(df.head())

    save_to_mysql(df, TABLE_NAME, DB_HOST, DB_USER, DB_NAME)

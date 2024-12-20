import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from PyPDF2 import PdfReader
import re

load_dotenv()

# === CONFIGURATION === #
EXCEL_FILE = os.getenv('EXCEL_FILE')
PDF_FILE = os.getenv('PDF_FILE')
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_NAME = 'pangkalan_data'
TABLE_NAME = 'laporan_neraca'

# === FUNCTIONS === #
def load_excel_sheet(file_path, sheet_name):
    """Load a specific sheet from an Excel file."""
    try:
        return pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    except Exception as e:
        print(f"Error reading Excel sheet {sheet_name}: {e}")
        exit(1)

def extract_notes_from_pdf(pdf_file, pages):
    """Extract notes from specified pages of the PDF."""
    try:
        with open(pdf_file, 'rb') as file:
            reader = PdfReader(file)
            notes_dict = {}
            for page_num in pages:
                page_text = reader.pages[page_num - 1].extract_text()
                # Match items with their notes (e.g., "Kas" -> "2a, 2c, 3")
                matches = re.findall(r"(Catatan\s*(\w+)).+?([^\n]+)", page_text, re.IGNORECASE)
                for _, note, item in matches:
                    item = item.strip()
                    if item not in notes_dict:
                        notes_dict[item] = []
                    notes_dict[item].append(note.strip())
            # Combine notes for each item into a single string
            for key in notes_dict:
                notes_dict[key] = ", ".join(set(notes_dict[key]))
            return notes_dict
    except Exception as e:
        print(f"Error extracting notes from PDF: {e}")
        exit(1)

def parse_excel_to_dataframe(excel_file, notes_dict):
    """Parse data from Excel into a DataFrame."""
    # Load specific sheet
    sheet_4220000 = load_excel_sheet(excel_file, '4220000')

    # Fixed values
    nama = "PT Bank Rakyat Indonesia (Persero) Tbk"
    no_emiten = "BBRI"
    kuartal = ["I", "II", "III", "IV"]  # Standard 4 quarters

    # Extract columns for items and nilai
    item_column = sheet_4220000.iloc[3:, 0].reset_index(drop=True)  # Starting from A4
    value_column = sheet_4220000.iloc[3:, 1].reset_index(drop=True)  # Starting from B4

    # Filter rows where nilai is numeric and not empty
    valid_data = []
    for item, value in zip(item_column, value_column):
        if pd.notna(value) and isinstance(value, (int, float)):
            valid_data.append((item, value))

    # Combine into a DataFrame
    data = []
    for i, (item, value) in enumerate(valid_data):
        note = notes_dict.get(item, "")  # Get notes for the item, or empty if none found
        data.append([nama, no_emiten, kuartal[i % 4], value, item, note])

    return pd.DataFrame(data, columns=['nama', 'no_emiten', 'kuartal', 'nilai', 'item', 'notes'])

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
    # Extract notes from the PDF
    notes_dict = extract_notes_from_pdf(PDF_FILE, pages=[384, 385, 386, 387])
    print("Extracted Notes Dictionary:", notes_dict)

    # Parse Excel data into DataFrame
    df = parse_excel_to_dataframe(EXCEL_FILE, notes_dict)
    print("\nParsed DataFrame:")
    print(df.head())

    # Save DataFrame to MySQL
    save_to_mysql(df, TABLE_NAME, DB_HOST, DB_USER, DB_NAME)

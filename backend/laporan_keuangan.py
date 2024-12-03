import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from PyPDF2 import PdfReader
import re
import difflib

# === Load environment variables === #
load_dotenv()

# === CONFIGURATION === #
EXCEL_FILE = os.getenv('EXCEL_FILE')
PDF_FILE = os.getenv('PDF_FILE')
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_NAME = 'pangkalan_data'
TABLE_NAME = 'laporan_keuangan'

# === FUNCTIONS === #
def load_excel_sheet(file_path, sheet_name):
    """Load a specific sheet from an Excel file."""
    try:
        return pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    except Exception as e:
        print(f"Error reading Excel sheet {sheet_name}: {e}")
        exit(1)

def extract_notes_from_pdf(pdf_file, pages):
    """Extract notes and associate them with items from specified pages of the PDF."""
    try:
        with open(pdf_file, 'rb') as file:
            reader = PdfReader(file)
            notes_dict = {}
            for page_num in pages:
                page_text = reader.pages[page_num - 1].extract_text()

                # Regex to detect notes like "2e,2f,2i,4" following items
                matches = re.findall(r"(?P<item>[\w\s]+?)\s(?P<notes>(\d+[a-z]{1,2}(?:,\d+[a-z]{1,2})(?:,\d+)))", page_text, re.IGNORECASE)

                # Add to dictionary
                for match in matches:
                    item = match[0].strip()
                    note = match[1].strip()
                    notes_dict[item] = note

            return notes_dict
    except Exception as e:
        print(f"Error extracting notes from PDF: {e}")
        exit(1)

def clean_item(item):
    """Cleans item text from unwanted characters."""
    item = re.sub(r'\s+', ' ', item).strip()  # Remove excess spaces
    item = re.sub(r'(?<!\S)(\w)\s(?=\w)', r'\1', item)  # Merge spaces between letters
    return item.lower()  # Convert to lowercase

def fuzzy_match_item(excel_item, pdf_item, threshold=0.5):
    """Fuzzy match between Excel item and PDF item."""
    ratio = difflib.SequenceMatcher(None, clean_item(excel_item), clean_item(pdf_item)).ratio()
    print(f"Matching '{excel_item}' with '{pdf_item}' - Cleaned: '{clean_item(excel_item)}' vs '{clean_item(pdf_item)}' - Ratio: {ratio}")
    return ratio > threshold  # Match if ratio exceeds threshold

def parse_excel_to_dataframe(excel_file, notes_dict, report_type='neraca'):
    """Parse data from Excel into a DataFrame based on the report type."""
    grup_lk_map = {
        'neraca': 'laporan_neraca',
        'laba_rugi': 'laporan_labarugi',
        'arus_kas': 'laporan_aruskas'
    }

    grup_lk = grup_lk_map.get(report_type, 'unknown')

    # Load sheet 1000000 to get kode_emiten and nama_emiten
    sheet_1000000 = load_excel_sheet(excel_file, '1000000')
    try:
        nama = sheet_1000000.iloc[5, 1]  # Nama entitas di B6
        no_emiten = sheet_1000000.iloc[7, 1]  # Kode entitas di B8
    except Exception as e:
        print(f"Error extracting entity name and code: {e}")
        exit(1)

    # Mapping file names to quarter
    kuartal_map = {
        'FinancialStatement-2023-I-BBRI.xlsx': 'I',
        'FinancialStatement-2023-II-BBRI.xlsx': 'II',
        'FinancialStatement-2023-III-BBRI.xlsx': 'III',
        'FinancialStatement-2023-Tahunan-BBRI.xlsx': 'IV',
    }
    quartal = kuartal_map.get(os.path.basename(excel_file), 'Unknown')

    # Load the relevant sheet based on report type
    if report_type == 'neraca':
        sheet_4220000 = load_excel_sheet(excel_file, '4220000')
        item_column = sheet_4220000.iloc[3:, 0].reset_index(drop=True)
        value_column = sheet_4220000.iloc[3:, 1].reset_index(drop=True)
    elif report_type == 'laba_rugi':
        sheet_4312000 = load_excel_sheet(excel_file, '4312000')
        item_column = sheet_4312000.iloc[3:, 0].reset_index(drop=True)
        value_column = sheet_4312000.iloc[3:, 1].reset_index(drop=True)
    elif report_type == 'arus_kas':
        sheet_4510000 = load_excel_sheet(excel_file, '4510000')
        item_column = sheet_4510000.iloc[3:, 0].reset_index(drop=True)
        value_column = sheet_4510000.iloc[3:, 1].reset_index(drop=True)
    else:
        raise ValueError(f"Invalid report type: {report_type}")
    
    valid_data = []
    for item, value in zip(item_column, value_column):
        if pd.notna(value) and isinstance(value, (int, float)):
            valid_data.append((item.strip(), value))  # Strip extra spaces from items
    
    # Combine data with notes
    data = []
    for i, (item, value) in enumerate(valid_data):
        matched_note = ""  # Start with no note

        # Check for fuzzy matches in notes_dict
        for pdf_item, note in notes_dict.items():
            if fuzzy_match_item(item, pdf_item):  # Fuzzy matching function (you can define it)
                matched_note = note
                break
        
        # Ensure no duplicate notes
        if matched_note:
            unique_notes = ",".join(sorted(set(matched_note.split(','))))  # Remove duplicate notes
            matched_note = unique_notes

        if matched_note == "":  # Print out items that failed to match any note
            print(f"Failed to match item: {item}")
        
        # Prepare data for DataFrame
        data.append([no_emiten, nama, quartal, grup_lk, item, value, matched_note])

    # Create the DataFrame with proper columns
    return pd.DataFrame(data, columns=['kode_emiten', 'nama_emiten', 'quartal', 'grup_lk', 'item', 'nilai', 'catatan'])

def save_to_mysql(df, table_name, host, user, db_name):
    """Save a DataFrame to a MySQL database."""
    mysql_uri = f'mysql+mysqlconnector://{user}@{host}/'
    engine = create_engine(mysql_uri)
    try:
        with engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))
        
        mysql_uri += db_name
        engine = create_engine(mysql_uri)

        # Create table if it does not exist
        with engine.connect() as conn:
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    kode_emiten VARCHAR(255),
                    nama_emiten VARCHAR(255),
                    quartal VARCHAR(10),
                    grup_lk VARCHAR(50),
                    item VARCHAR(255),
                    nilai BIGINT,
                    catatan TEXT
                );
            """))

        # Insert data into the table
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Data successfully added to table '{table_name}' in database '{db_name}'.")
    except Exception as e:
        print(f"Error saving data to MySQL: {e}")
        exit(1)

# === MAIN SCRIPT === #
if __name__ == "__main__":
    # Extract notes from the PDF (modify page numbers as needed)
    notes_dict = extract_notes_from_pdf(PDF_FILE, pages=[384, 385, 386, 387])
    print("Extracted Notes Dictionary:", notes_dict)

    # Parse Neraca data from Excel
    df_neraca = parse_excel_to_dataframe(EXCEL_FILE, notes_dict, report_type='neraca')
    print("\nParsed Neraca DataFrame:")
    print(df_neraca.head())

    # Parse Laporan Laba Rugi data from Excel
    df_laba_rugi = parse_excel_to_dataframe(EXCEL_FILE, notes_dict, report_type='laba_rugi')
    print("\nParsed Laba Rugi DataFrame:")
    print(df_laba_rugi.head())

    # Parse Laporan Arus Kas data from Excel
    df_arus_kas = parse_excel_to_dataframe(EXCEL_FILE, notes_dict, report_type='arus_kas')
    print("\nParsed Arus Kas DataFrame:")
    print(df_arus_kas.head())

    df_combined = pd.concat([df_neraca, df_laba_rugi, df_arus_kas], ignore_index=True)

    # Save to MySQL
    save_to_mysql(df_combined, TABLE_NAME, DB_HOST, DB_USER, DB_NAME)

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

import re
from PyPDF2 import PdfReader

def extract_notes_from_pdf(pdf_file, pages):
    """Extract notes from specified pages of the PDF."""
    try:
        with open(pdf_file, 'rb') as file:
            reader = PdfReader(file)
            notes_dict = {}
            
            # Loop through the pages specified
            for page_num in pages:
                page_text = reader.pages[page_num - 1].extract_text()
                
                # Log the extracted text to debug the structure
                print(f"Extracted Text from Page {page_num}:\n{page_text}\n")
                
                # Regex to capture items and their corresponding notes
                matches = re.findall(r"([A-Za-z\s]+)\s*[:\n]?\s*([0-9a-zA-Z,]+(?:\s*,\s*[0-9a-zA-Z]+)*)", page_text, re.IGNORECASE)
                
                # Check if matches are found and process them
                if matches:
                    for item, note in matches:
                        item = item.strip()  # Clean up the item name
                        note = note.strip()  # Clean up the note

                        # Log if the note for an item is empty
                        if not note:
                            print(f"Warning: No note found for item '{item}' on page {page_num}")
                        
                        # Add the note for the particular item
                        if item not in notes_dict:
                            notes_dict[item] = []
                        notes_dict[item].append(note)
                else:
                    print(f"No matches found on page {page_num}")
            
            # Join notes into a single string without removing duplicates and ensure the correct format
            for key in notes_dict:
                notes_dict[key] = ",".join(notes_dict[key])

            # Log the notes in a structured manner
            print("\nExtracted Notes Dictionary:")
            for item, notes in notes_dict.items():
                print(f"{item}:")
                print(f"  {notes}\n")
            
            print('peler',notes_dict)
            return notes_dict
    except Exception as e:
        print(f"Error extracting notes from PDF: {e}")
        exit(1)


def parse_excel_to_dataframe(excel_file, notes_dict):
    """Parse data from Excel into a DataFrame, including notes from the extracted PDF."""
    # Load specific sheet
    sheet_1000000 = load_excel_sheet(excel_file, '1000000')
    sheet_4220000 = load_excel_sheet(excel_file, '4220000')
    
    try:
        nama = sheet_1000000.iloc[5, 1]  # Nama entitas di B6
        no_emiten = sheet_1000000.iloc[7, 1]  # Kode entitas di B8
    except Exception as e:
        print(f"Error extracting entity name and code: {e}")
        exit(1)

    # Fixed values
    kuartal = ["I", "II", "III", "IV"]  # Standard 4 quarters

    # Extract columns for items and values
    item_column = sheet_4220000.iloc[3:, 0].reset_index(drop=True)  # Starting from A4
    value_column = sheet_4220000.iloc[3:, 1].reset_index(drop=True)  # Starting from B4

    # Filter rows where values are numeric and not empty
    valid_data = []
    for item, value in zip(item_column, value_column):
        if pd.notna(value) and isinstance(value, (int, float)):
            valid_data.append((item, value))


    # Combine data with notes
    data = []
    for i, (item, value) in enumerate(valid_data):
        # Get notes for the item, if any, otherwise default to an empty string
        note = notes_dict.get(item.strip(), "")  # Clean item to match keys in notes_dict
        print("Data Note \n", note)

        data.append([nama, no_emiten, kuartal[i % 4], value, item, note])
    print("Data", data)
    # Create the DataFrame with proper columns
    return pd.DataFrame(data, columns=['nama', 'no_emiten', 'kuartal', 'value', 'item', 'note'])


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
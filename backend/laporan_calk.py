import os
import re
from dotenv import load_dotenv
from PyPDF2 import PdfReader
import mysql.connector

# Load environment variables
load_dotenv()

PDF_FILE = os.getenv('PDF_FILE')
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
TABLE_NAME = os.getenv('TABLE_NAME_CaLK')

# === EXTRACT TEXT AND ORGANIZE === #
def clean_title(title):
    """Process the title by splitting it into characters and extracting the last number with text."""
    # Split the title into an array of characters
    char_array = list(title.strip())
    
    # Join characters back into a string and find the last number followed by text
    match = re.search(r"(\d+)\.\s*(.*)$", ''.join(char_array))
    
    if match:
        # Extract the last number and its associated text
        number = match.group(1)
        text = match.group(2).strip()
        
        # Return the formatted title with the last number and text
        return f"{number}. {text}"
    
    return None  

def extract_and_organize_text(file_path, start_page, end_page):
    try:
        reader = PdfReader(file_path)
        text = ""
        for i in range(start_page - 1, end_page):  # Adjust for zero-index
            text += reader.pages[i].extract_text()

        # Organize text into title, subtitle, and content
        organized_data = []
        current_title = None
        current_subtitle = None
        current_content = []

        lines = text.split("\n")
        for line in lines:
            if re.match(r"^\d+\.\s", line):  # Matches "1. UMUM"
                if current_title:
                    organized_data.append({
                        "title": clean_title(current_title),
                        "subtitle": current_subtitle,
                        "content": "\n".join(current_content).strip(),
                    })
                current_title = line.strip()
                print("curent title: " + current_title)
                current_subtitle = None
                current_content = []
            elif re.match(r"^[a-z]+\.\s", line):  # Matches "a. Pendirian"
                if current_subtitle:
                    organized_data.append({
                        "title": clean_title(current_title),
                        "subtitle": current_subtitle,
                        "content": "\n".join(current_content).strip(),
                    })
                current_subtitle = line.strip()
                current_content = []
            else:
                current_content.append(line)

        # Save the last entry
        if current_title:
            organized_data.append({
                "title": clean_title(current_title),
                "subtitle": current_subtitle,
                "content": "\n".join(current_content).strip(),
            })
        
        # Log the data for debugging
        print("Organized Data:")
        for entry in organized_data:
            print(entry)
        
        return organized_data
    except Exception as e:
        print(f"Error extracting and organizing text: {e}")
        return None

# === SAVE TO DATABASE === #
def save_to_database(host, user, password, db_name, table_name, data):
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=db_name
        )
        cursor = connection.cursor()

        # Create table if it doesn't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            subtitle VARCHAR(255),
            content LONGTEXT NOT NULL
        );
        """
        cursor.execute(create_table_query)

        # Insert data into table
        insert_query = f"INSERT INTO {table_name} (title, subtitle, content) VALUES (%s, %s, %s)"
        for entry in data:
            cursor.execute(insert_query, (entry['title'], entry['subtitle'], entry['content']))
        connection.commit()

        print("Data has been saved to the database.")
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# === MAIN SCRIPT === #
if __name__ == "__main__":
    if not os.path.exists(PDF_FILE):
        print("PDF file not found. Please check the path in the .env file.")
    else:
        # Extract and organize text
        organized_text = extract_and_organize_text(PDF_FILE, 395, 454)
        if organized_text:
            # Save organized text to the database
            save_to_database(DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, TABLE_NAME, organized_text)

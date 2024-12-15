import os
import re
from dotenv import load_dotenv
from tkinter import filedialog, messagebox
from ttkbootstrap import Style
from ttkbootstrap.constants import PRIMARY
from ttkbootstrap.widgets import Frame, Label, Button, Entry, Progressbar
from PyPDF2 import PdfReader
import mysql.connector

# Load environment variables
load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
TABLE_NAME = os.getenv('TABLE_NAME_CaLK')

# === EXTRACT TEXT AND ORGANIZE === #
def clean_title(title):
    match = re.search(r"(\d+)\.\s*(.*)$", title.strip())
    if match:
        number = match.group(1)
        text = match.group(2).strip()
        return f"{number}. {text}"
    return None

def extract_and_organize_text(file_path, start_page, end_page, progress_callback):
    try:
        reader = PdfReader(file_path)
        total_pages = len(reader.pages)
        relevant_text = ""

        for idx, page in enumerate(reader.pages):
            page_text = page.extract_text()
            if "CATATAN ATAS LAPORAN KEUANGAN" in page_text:
                relevant_text += page_text
            progress_callback((idx + 1) / total_pages * 100)

        organized_data = []
        current_title = None
        current_subtitle = None
        current_content = []

        lines = relevant_text.split("\n")
        for line in lines:
            if re.match(r"^\d+\.\s", line):
                if current_title:
                    organized_data.append({
                        "title": clean_title(current_title),
                        "subtitle": current_subtitle,
                        "content": "\n".join(current_content).strip(),
                    })
                current_title = line.strip()
                current_subtitle = None
                current_content = []
            elif re.match(r"^[a-z]+\.\s", line):
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

        if current_title:
            organized_data.append({
                "title": clean_title(current_title),
                "subtitle": current_subtitle,
                "content": "\n".join(current_content).strip(),
            })

        return organized_data
    except Exception as e:
        messagebox.showerror("Error", f"Error extracting text: {e}")
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

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            subtitle LONGTEXT,
            content LONGTEXT NOT NULL
        );
        """
        cursor.execute(create_table_query)

        insert_query = f"INSERT INTO {table_name} (title, subtitle, content) VALUES (%s, %s, %s)"
        for entry in data:
            cursor.execute(insert_query, (entry['title'], entry['subtitle'], entry['content']))
        connection.commit()

        messagebox.showinfo("Success", "Data has been successfully saved to the database. Processing completed!")
    except mysql.connector.Error as err:
        messagebox.showerror("Database Error", f"Database error: {err}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# === MAIN SCRIPT WITH UI === #
def main():
    def select_file():
        file_path = filedialog.askopenfilename(filetypes=[("PDF Files", "*.pdf")])
        if file_path:
            file_entry.delete(0, 'end')
            file_entry.insert(0, file_path)

    def process_file():
        file_path = file_entry.get()
        try:
            if not file_path:
                messagebox.showwarning("Input Error", "Please select a PDF file.")
                return

            progress_bar['value'] = 0
            progress_message['text'] = "Processing started..."

            def update_progress(value):
                progress_bar['value'] = value
                progress_message['text'] = f"Progress: {int(value)}%"
                root.update_idletasks()

            organized_text = extract_and_organize_text(file_path, None, None, update_progress)
            if organized_text:
                save_to_database(DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, TABLE_NAME, organized_text)
                progress_message['text'] = "Processing finished successfully!"
        except Exception as e:
            messagebox.showerror("Error", f"An unexpected error occurred: {e}")

    # Setup UI
    style = Style(theme="superhero")
    root = style.master
    root.title("PDF Extractor with Style")
    root.geometry("700x500")

    frame = Frame(root, padding=20)
    frame.pack(fill="both", expand=True)

    Label(frame, text="PDF File Path:", font=("Helvetica", 14)).grid(row=0, column=0, sticky="w", pady=10)
    file_entry = Entry(frame, width=50, font=("Helvetica", 12))
    file_entry.grid(row=0, column=1, padx=5, pady=10)
    Button(frame, text="Browse", bootstyle=PRIMARY, command=select_file).grid(row=0, column=2, padx=5, pady=10)

    Button(frame, text="Process", bootstyle=PRIMARY, command=process_file).grid(row=1, columnspan=3, pady=20)

    progress_bar = Progressbar(frame, orient="horizontal", length=400, mode="determinate", bootstyle=PRIMARY)
    progress_bar.grid(row=2, columnspan=3, pady=10)

    progress_message = Label(frame, text="", font=("Helvetica", 12))
    progress_message.grid(row=3, columnspan=3, pady=10)

    root.mainloop()

if __name__ == "__main__":
    main()
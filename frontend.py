import os
import xml.etree.ElementTree as ET
import pandas as pd
import streamlit as st
import zipfile
import io
import shutil
import subprocess
import zipfile

# Funzione gestione errori
def gestisci_errore_parsing(filename, errore):
    st.write(f"Errore nel file {filename}: {errore}. Passo al file successivo.")

# Funzione di esplorazione ricorsiva per il parsing dei dati
def parse_element(element, parsed_data, parent_tag=""):
    for child in element:
        tag_name = f"{parent_tag}/{child.tag.split('}')[-1]}" if parent_tag else child.tag.split('}')[-1]
        
        if list(child):  # Se ha figli, chiamata ricorsiva
            parse_element(child, parsed_data, tag_name)
        else:  # Altrimenti, aggiunge il testo alla struttura dei dati
            parsed_data[tag_name] = child.text

# Funzione per estrarre il contenuto di un file ZIP
def extract_zip(zip_file):
    extracted_folder = "/tmp/extracted"  # Percorso temporaneo per i file estratti

    # Rimuovi la cartella estratta precedente, se esiste
    if os.path.exists(extracted_folder):
        shutil.rmtree(extracted_folder)
    
    # Estrai il nuovo file ZIP
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(extracted_folder)
    return extracted_folder

# Funzione per decodificare e convertire i file .p7m in .xml
def converti_p7m_in_xml(fe_path):
    file = os.listdir(fe_path)
    files_converted = []

    # Scorre tutti i file nella cartella estratta
    for x in range(len(file)):
        full_file_path = os.path.join(fe_path, file[x])
        # Verifica se il file è un .p7m
        if ".p7m" in file[x]:
            xml_output_path = os.path.join(fe_path, f"{file[x]}.xml")
            try:
                # Esegui il comando per convertire il .p7m in .xml
                result = subprocess.run(
                    ['openssl', 'smime', '-verify', '-noverify', '-in', full_file_path, '-inform', 'DER', '-out', xml_output_path],
                    check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
                st.write(f"File {file[x]} convertito in XML.")
                files_converted.append(xml_output_path)
                os.remove(full_file_path)  # Rimuovi il file .p7m originale
            except subprocess.CalledProcessError as e:
                st.write(f"Errore nella conversione del file {file[x]}: {e.stderr.decode()}")
    return files_converted

# Funzione per creare una zip con i file convertiti
def create_zip_with_converted_files(fe_path, converted_files):
    zip_filename = "/tmp/converted_files.zip"
    with zipfile.ZipFile(zip_filename, 'w') as zipf:
        for file_path in converted_files:
            zipf.write(file_path, os.path.relpath(file_path, fe_path))
    return zip_filename



# Funzione per estrarre e parsare il file XML
def parse_xml_file(xml_file_path, includi_dettaglio_linee=True):
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        # Parsing dei dati generali della fattura senza namespace
        header_data = {}
        header = root.find(".//FatturaElettronicaHeader")
        if header is not None:
            parse_element(header, header_data)

        # Parsing di Data e Numero della Fattura nel corpo
        general_data = {}
        dati_generali = root.find(".//FatturaElettronicaBody//DatiGenerali//DatiGeneraliDocumento")
        if dati_generali is not None:
            parse_element(dati_generali, general_data)

        # Parsing dei riepiloghi
        riepilogo_dati = {}
        riepiloghi = root.findall(".//FatturaElettronicaBody//DatiBeniServizi//DatiRiepilogo")
        for riepilogo in riepiloghi:
            parse_element(riepilogo, riepilogo_dati)

        # Parsing delle linee se richiesto
        line_items = []
        descrizioni = []
        lines = root.findall(".//FatturaElettronicaBody//DettaglioLinee")
        for line in lines:
            line_data = {}
            parse_element(line, line_data)
            if "Descrizione" in line_data:
                descrizioni.append(line_data["Descrizione"])
            if includi_dettaglio_linee:
                line_items.append(line_data)

        # Organizzare i dati in modo che ogni fattura sia una riga e le linee siano separate
        all_data = []
        combined_data = {**header_data, **general_data, **riepilogo_dati}

        if not includi_dettaglio_linee and descrizioni:
            combined_data["Descrizione"] = " | ".join(descrizioni)
            all_data.append(combined_data)
        elif line_items:
            first_line_data = line_items[0]
            combined_data = {**combined_data, **first_line_data}
            all_data.append(combined_data)

            for line_data in line_items[1:]:
                line_row = {**{key: None for key in combined_data.keys()}, **line_data}
                all_data.append(line_row)
        else:
            all_data.append(combined_data)

        return all_data

    except ET.ParseError as e:
        return []

# Funzione per iterare su più file e compilare un unico DataFrame
def process_all_files(xml_folder_path, includi_dettaglio_linee=True):
    all_data_combined = []

    # Ciclo su tutti i file nella cartella specificata
    xml_files = []
    for root, dirs, files in os.walk(xml_folder_path):
        for file in files:
            if file.endswith('.xml'):
                xml_files.append(os.path.join(root, file))

    if not xml_files:
        return []

    for xml_file_path in xml_files:
        try:
            file_data = parse_xml_file(xml_file_path, includi_dettaglio_linee)
            all_data_combined.extend(file_data)
        except ET.ParseError as e:
            gestisci_errore_parsing(xml_file_path, e)  # Chiamata alla funzione di gestione errori

    # Creazione del DataFrame combinato con tutti i dati
    all_data_df = pd.DataFrame(all_data_combined)
    return all_data_df

# Funzione per selezionare le colonne da esportare
def seleziona_colonne(df, colonne_default):
    colonne_validi = [col for col in colonne_default if col in df.columns]
    
    colonne_selezionate = st.multiselect(
        "Seleziona le colonne da visualizzare",
        options=df.columns.tolist(),
        default=colonne_validi  # Imposta le colonne valide come predefinite
    )
    return colonne_selezionate

# Funzione per esportare i dati come file Excel e creare un bottone di download
def esporta_excel(df, colonne_esistenti):
    if not df.empty:
        # Creazione di un buffer in memoria (senza salvarlo su disco)
        output = io.BytesIO()
        
        # Scrittura dei dati nel buffer
        df[colonne_esistenti].to_excel(output, index=False)
        
        # Necessario per il download del file
        output.seek(0)
        
        # Creazione del bottone di download
        st.download_button(
            label="Scarica i dati in Excel",
            data=output,
            file_name="fattura_dati_combinati_selezionati.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        st.success(f"Il file Excel è pronto per il download.")
    else:
        st.warning("Non ci sono dati da esportare.")

# Elenco delle colonne di default
colonne_default = [
    "CedentePrestatore/DatiAnagrafici/IdFiscaleIVA/IdPaese",
    "CedentePrestatore/DatiAnagrafici/IdFiscaleIVA/IdCodice",
    "CedentePrestatore/DatiAnagrafici/Anagrafica/Denominazione",
    "CedentePrestatore/DatiAnagrafici/RegimeFiscale",
    "CedentePrestatore/Sede/Indirizzo",
    "CedentePrestatore/Sede/NumeroCivico",
    "CedentePrestatore/Sede/CAP",
    "CedentePrestatore/Sede/Comune",
    "TipoDocumento",
    "Data",
    "Numero",
    "ImportoTotaleDocumento",
    "AliquotaIVA",
    "ImponibileImporto",
    "Imposta",
    "Descrizione",
    "PrezzoTotale"
]

# Interfaccia utente con Streamlit
st.title("Analisi XML Fatture Elettroniche")

# Carica un nuovo file ZIP per l'elaborazione
uploaded_file = st.file_uploader("Carica il file ZIP contenente i file XML", type=["zip"], key="file_uploader")

# Variabile per memorizzare i dati
all_data_df = None

# Reset dei dati quando viene caricato un nuovo file
if uploaded_file is not None:
    # Reset dei dati precedenti
    all_data_df = None

    # Estrazione file ZIP
    extracted_folder = extract_zip(uploaded_file)
    
    # Converti i file .p7m in .xml
    converti_p7m_in_xml(extracted_folder)

    # Creare una nuova cartella ZIP con i file convertiti
    zip_filename = create_zip_with_converted_files(extracted_folder)

    # Aggiungi un pulsante per scaricare il file ZIP con i file convertiti
    with open(zip_filename, "rb") as f:
        st.download_button(
            label="Scarica i file convertiti",
            data=f,
            file_name="converted_files.zip",
           mime="application/zip"
        )

    st.success('Conversione completata e file .zip pronto per il download!')
else:
    st.warning("Non ci sono file .p7m da convertire.")


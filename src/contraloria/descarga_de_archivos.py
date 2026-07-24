import warnings

warnings.filterwarnings("ignore")  # Suppress warnings as in the original code

from datetime import datetime as dt
from re import search
from urllib.parse import quote
import io


from pandas import read_excel, options, to_datetime, concat, DataFrame, read_csv

options.mode.chained_assignment = None  # Disable chained assignment warnings in pandas

from requests import post, get
from bs4 import BeautifulSoup


class Contraloria:
    """
    Provides methods to extract and process payroll data from the Contraloría de Panamá website:
    https://www.contraloria.gob.pa/CGR.PLANILLAGOB.UI/Formas

    The site allows downloading Excel reports for public institutions, filtered by employment status.
    This class manages extraction of dropdown values, downloading and cleaning Excel files,
    and storing results.
    """

    def __init__(self):
        """
        Initializes base URLs and HTTP headers for requests to the Contraloría website.
        """
        self.base_url = "https://www.contraloria.gob.pa/CGR.PLANILLAGOB.UI/Formas"
        self.index_url = f"{self.base_url}/Index"
        self.report_url = f"{self.base_url}/Reporte"
        self.headers = {"User-Agent": "Mozilla/5.0"}
        self.query_date = dt.now()

    def get_query_date(self):
        return self.query_date

    def read_update_date(self):
        """
        Extracts the last update date from the main page of the Contraloría.
        Sets self.update_date with the extracted datetime value.
        """
        pattern = r"Fecha de actualización de los datos:\s+(\d{1,2}/\d{1,2}/\d+\s+\d{1,2}:\d{1,2}:\d{1,2}\s+\w+)"
        response = get(
            self.index_url,
            verify=False,  # keep original behavior
        ).text
        date_match = search(pattern, response)
        self.update_date = dt.strptime(date_match.group(1), "%d/%m/%Y %H:%M:%S %p")

    def get_update_date(self):
        """
        Returns the last update date as a datetime object.
        """
        return self.update_date

    def __retrieve_options(self, selector_id: str) -> list[str]:
        """
        Scrapes the institutions dropdown from the Contraloría index page.
        Returns a list of institution names, excluding placeholders.
        """
        resp = post(
            self.index_url,
            headers=self.headers,
            verify=False,  # keep original behavior
        )
        soup = BeautifulSoup(resp.content, "html.parser")

        # Find the select element that contains the institutions
        select = soup.find("select", {"id": selector_id})
        raw_items = select.text.split("\n") if select else []

        # Filter out empty and placeholder rows
        results = [x for x in raw_items if x and "Seleccione" not in x.strip()]
        return results

    def get_status_list(self):
        """
        Extracts the values from the employment status dropdown on the main page.
        Returns a list of available statuses.
        """
        return self.__retrieve_options("MainContent_ddlEstado")

    def get_institution_list(self) -> list[str]:
        """
        Scrapes the institutions dropdown from the Contraloría index page.
        Returns a list of institution names, excluding placeholders.
        """
        return self.__retrieve_options("MainContent_ddlInstituciones")

    def download_report(self, institution: str, status: str, save_path: str) -> None:
        """
        Downloads an Excel payroll report for a given institution and employment status from the Contraloría site,
        cleans the data, and writes the result to a Parquet file in the staging directory.

        Args:
            institution (str): Institution name as shown by the website.
            status (str): Employment status to filter the report by.
            path (str): Output directory for the Parquet file.

        The URL used is:
        https://www.contraloria.gob.pa/CGR.PLANILLAGOB.UI/Formas/Reporte?&Ne={institution}&N=&A=&C=&E={status}
        """
        query_datetime = dt.now()

        # Build the report URL with institution and status filters
        url = f"{self.report_url}?&Ne={quote(institution)}&N=&A=&C=&E={quote(status)}"

        # Request the file (original code uses verify=False)
        resp = get(url, verify=False)

        # Derive a file name from the HTTP header if available
        dispo = resp.headers.get("content-disposition", "") or resp.headers.get("Content-Disposition", "")
        m = search(r'filename="(.*?).xlsx"', dispo)
        file_name = m.group(1) if m else "reporte_contraloria"

        # Read the Excel content from memory (may fail if server returned HTML)
        bytes_file_obj = io.BytesIO(resp.content)

        # Load all sheets into a list; original logic expects headers in row 4
        sheets = list(read_excel(bytes_file_obj, engine="openpyxl", sheet_name=None).values())

        # Cleans each sheet: drop top 4 rows and set row 3 as header
        def clean_data(df_):
            df = df_.iloc[4:].copy()
            df.columns = df_.iloc[3]
            df.columns = list(map(lambda x: x.strip().lower().replace(" ", "_"), df.columns))
            return df

        # Extract some header cells for date parsing (kept for compatibility)
        # informe = sheets[0].iloc[0, 0]
        fecha_act = sheets[0].iloc[1, 4]
        # institucion_meta = sheets[0].iloc[1, 0]

        # Concatenate all sheets after cleaning
        df = concat(list(map(clean_data, sheets)))

        # Cast numeric columns
        df["salario"] = df["salario"].astype("float")
        df["gasto"] = df["gasto"].astype("float")

        # Parse the update datetime found on the sheet header (keeps original regex/format)
        df["fecha_actualizacion"] = str(
            dt.strptime(
                search(r".*?:\s+(\d{2}/\d{2}/\d{4}\s+\d{1,2}:\d{1,2}:\d{1,2}\s+\w{2})", str(fecha_act)).group(1),
                "%d/%m/%Y %H:%M:%S %p",
            )
        )

        # Add audit/metadata columns
        df["fecha_consulta"] = self.get_update_date()
        df["archivo"] = file_name
        df["Institucion"] = institution

        # Normalize cedula column name if it has an accent
        if "cédula" in df.columns:
            df = df.rename(columns={"cédula": "cedula"})

        # Parse date-like columns to proper types (keeps original formats)
        df["fecha_de_inicio"] = to_datetime(df["fecha_de_inicio"], format="%d/%m/%Y").dt.date
        df["fecha_actualizacion"] = to_datetime(df["fecha_actualizacion"])
        df["fecha_consulta"] = to_datetime(df["fecha_consulta"])

        # Write to Parquet in staging; include estado and a timestamp for uniqueness
        ts = str(self.get_update_date().timestamp()).replace(".", "_")
        out = f"{save_path}/{file_name}_{status}_{ts}.parquet"
        df.to_parquet(out, index=False, use_deprecated_int96_timestamps=True)

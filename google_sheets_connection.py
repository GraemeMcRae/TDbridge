"""
Google Sheets Connection for TDbridge
Provides a simple global connection to the Google Sheets spreadsheet.

This file is functionally identical to google_sheets_connection.py from the
HCF project.  The only difference is the import: it imports `config` from
the TDbridge `config` module rather than from `config_hcf`.

A symlink or copy strategy is used in HCF to keep a single canonical copy;
TDbridge carries its own copy for the same reason (import-path isolation).
"""

import logging

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from config import config

logger = logging.getLogger(config.bot_name)


def _connect_to_sheets() -> gspread.Spreadsheet:
    """Establish connection to Google Sheets.

    Returns:
        gspread.Spreadsheet: The authenticated spreadsheet object.
    """
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            config.google_credentials_file,
            scope,
        )
        client = gspread.authorize(creds)
        spreadsheet = client.open(config.google_spreadsheet_name)
        logger.info(f"Connected to Google Spreadsheet: {config.google_spreadsheet_name}")
        return spreadsheet

    except FileNotFoundError:
        logger.error(f"Credentials file not found: {config.google_credentials_file}")
        raise
    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(f"Spreadsheet not found: {config.google_spreadsheet_name}")
        logger.error("Make sure the sheet has been shared with the service account email")
        raise
    except Exception as e:
        logger.error(f"Failed to connect to Google Sheets: {e}")
        raise


# Global spreadsheet connection — created once at import time.
# table_manager.py imports this module and uses the `sheet` object.
sheet = _connect_to_sheets()

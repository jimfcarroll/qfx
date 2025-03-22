#!/usr/bin/env python3
import argparse
import csv
import datetime
import json
import os
import re
import sys

import config
from db.db import create_or_get
from dataclasses import dataclass

# Module-level database connection
_con = None

@dataclass
class SecurityInfo:
    uniqueid: str
    info_entry: str

@dataclass
class TransactionEntry:
    txn_str: str
    is_invbanktran: bool

# --- Helper Functions ---

def load_account_id_mapping(filename: str) -> dict[str, str]:
    """
    Loads the account_id_mapping section from the account mapping JSON file.
    
    Args:
        filename (str): Path to the JSON mapping file
        
    Returns:
        dict[str, str]: Mapping of account names to account IDs
        
    Raises:
        FileNotFoundError: If the mapping file doesn't exist
        KeyError: If the account_id_mapping section is missing
        json.JSONDecodeError: If the file isn't valid JSON
    """
    if not os.path.isfile(filename):
        raise FileNotFoundError(f"Account mapping file '{filename}' not found.")
    
    with open(filename, "r", encoding="utf-8") as f:
        mapping = json.load(f)
        
    if "account_id_mapping" not in mapping:
        raise KeyError("Required 'account_id_mapping' section not found in mapping file")
        
    return mapping["account_id_mapping"]

def load_missing_cusip_mapping(filename: str) -> list[dict]:
    """
    Loads the missing_cusip_mapping section from the account mapping JSON file.
    
    Args:
        filename (str): Path to the JSON mapping file
        
    Returns:
        list[dict]: List of missing CUSIP mappings with regex patterns
        
    Raises:
        FileNotFoundError: If the mapping file doesn't exist
        KeyError: If missing_cusip_mapping section is missing
        json.JSONDecodeError: If the file isn't valid JSON
    """
    if not os.path.isfile(filename):
        raise FileNotFoundError(f"Account mapping file '{filename}' not found.")
    
    with open(filename, "r", encoding="utf-8") as f:
        mapping = json.load(f)
        
    if "missing_cusip_mapping" not in mapping:
        raise KeyError("Required 'missing_cusip_mapping' section not found in mapping file")
        
    return mapping["missing_cusip_mapping"]

def get_account_id(account_name: str, mapping: dict[str, str]) -> str:
    normalized_name = account_name.strip()
    if normalized_name not in mapping:
        raise KeyError(f"Account name '{normalized_name}' not found in account mapping.")
    return mapping[normalized_name]

def normalize_header(header: str) -> str:
    h = header.strip()
    if h.lower().startswith("price") or h.lower().startswith("amount"):
        h = re.sub(r'\s*\d+$', '', h)
    return h

def parse_date(date_str: str) -> datetime.datetime | None:
    s = date_str.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None

def format_ofx_datetime(dt_obj: datetime.datetime | None) -> str:
    # Returns a string like "YYYYMMDD120000.000[-5:EST]"
    if dt_obj is None:
        dt_obj = datetime.datetime.now()
    return dt_obj.strftime("%Y%m%d") + "120000.000[-5:EST]"

def normalize_currency(value: str, inverseAmount: bool = False) -> str:
    s = value.strip()
    if not s:
        return None
    negative = False
    if s.startswith("(") and s.endswith(")"):
        negative = True
        s = s[1:-1]
    s = s.replace("$", "").replace(",", "").strip()
    if s.startswith("-"):
        negative = True
        s = s.lstrip("-").strip()
    try:
        num = float(s)
    except ValueError:
        return "0.00"
    negative = not negative if inverseAmount else negative
    if negative:
        num = -num
    return f"{num:.2f}"

def normalize_quantity(value : str) -> str:
    return value.strip().replace(",", "")

def compute_price(quantity: str, amount: str) -> str:
    try:
        q = float(quantity)
        if q < 0:
            q = -q
        a = float(amount)
        if a < 0:
            a = -a
        if q != 0:
            return f"{a/q:.9f}"
    except ValueError:
        pass
    return "0.00"

def is_mutual_fund(symbol: str) -> bool:
    """
    Determines if a symbol represents a mutual fund by checking if it exists in the stocks database.
    Returns True if the symbol is not found in the stocks database (indicating it's a mutual fund).
    """
    global _con
    if _con is None:
        _con = create_or_get(config.data_root, config.parquet_subdir, fail_if_not_exists=True)
        _con.init_views()
        
    result = _con.execute(f"""
        SELECT 1 
        FROM stocks_day 
        WHERE ticker = '{symbol}' 
        LIMIT 1
    """).fetchone()
    
    # If symbol is not found in stocks database, consider it a mutual fund
    return result is None

def cash_secid(indent: int) -> str:
    """Return the SECID block for cash transactions with proper indentation."""
    indent_str = " " * indent

    return (
        f"{indent_str}<SECID>\n"
        f"{indent_str}  <UNIQUEID>CASH</UNIQUEID>\n"
        f"{indent_str}  <UNIQUEIDTYPE>CUSIP</UNIQUEIDTYPE>\n"
        f"{indent_str}</SECID>\n"
    )

def row_is_empty(row: list[str]) -> bool:
    """Return True if every field in the row is an empty string."""
    return all(not field.strip() for field in row)

def xml_escape(text: str) -> str:
    """Escape special characters for XML."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;").replace("'", "&apos;")

def is_funding_activity(row: dict[str, str]) -> bool:
    """
    Determine if the activity is a funding activity. These tend to be labeled as "buy" with no CUSIP,
    no symbol, and an amount equal to the negative of the quantity. They are usually accompanied by an 
    eventual reversal when the actual underlying fund is bought and includes another "buy" activity 
    with reversed quantity and positive amount.
    """
    return row["Activity"].strip().lower() == "buy" and \
        row["Description"].strip().lower().endswith("initial") and \
        not row["CUSIP"].strip() and \
        not row["Symbol"].strip() and \
        float(normalize_currency(row["Amount"])) ==  - float(row["Quantity"])

def _make_txn_entry(txn_data: tuple[ str,  SecurityInfo], is_invbanktran: bool) -> tuple[TransactionEntry, SecurityInfo]:
    return TransactionEntry(txn_data[0], is_invbanktran), txn_data[1]

# ---------------------------------------------------------------------
# --- Transaction Generation Functions for buy/sell
# ---
def generate_buysell_secid(row: dict[str, str], indent: int) -> str:
    """
    Generate a SECID block for a BUY or SELL transaction.
    
    Args:
        row (dict[str, str]): Transaction row data
        
    Returns:
        str: Generated SECID XML block
        
    Raises:
        RuntimeError: If missing_cusip_mappings not initialized
        ValueError: If no CUSIP is provided and no matching regex pattern is found
    """
    global _missing_cusip_mappings
    if _missing_cusip_mappings is None:
        raise RuntimeError("missing_cusip_mappings not initialized")
    
    indent_str = " " * indent
    out = ""
    cusip = row["CUSIP"].strip()
    if cusip:
        out += f"{indent_str}<SECID>\n"
        out += f"{indent_str}  <UNIQUEID>{cusip}</UNIQUEID>\n"
        out += f"{indent_str}  <UNIQUEIDTYPE>CUSIP</UNIQUEIDTYPE>\n"
        out += f"{indent_str}</SECID>\n"
    else:
        # otherwise, parse account_mapping.json and load the list of missing_cusip_mappings.
        # then, check if the description matches any of the regexes in the list.
        # if it does, return the SECID block for the corresponding uniqueid.
        # otherwise, return an empty string.
        # Try to match description against regex patterns
        description = row["Description"].strip()
        for mapping in _missing_cusip_mappings:
            if re.search(mapping["description_regex"], description):
                out += f"{indent_str}<SECID>\n"
                out += f"{indent_str}  <UNIQUEID>{mapping['uniqueid']}</UNIQUEID>\n"
                out += f"{indent_str}  <UNIQUEIDTYPE>OTHER</UNIQUEIDTYPE>\n"
                out += f"{indent_str}</SECID>\n"
                break
        else:  # No match found
            raise ValueError(f"No CUSIP provided and no matching pattern found for description: {description}")
    
    return out

def generate_buysell_security_info(row: dict[str, str]) -> SecurityInfo:
    """
    Generate a SecurityInfo object for a transaction.
    """
    symbol = row["Symbol"].strip()
    cusip = row["CUSIP"].strip()

    if cusip:
        if not symbol:
            symbol = cusip
        info_tag = "MFINFO" if is_mutual_fund(symbol) else "STOCKINFO"
        out = f"      <{info_tag}>\n"
        out += "        <SECINFO>\n"
        out += generate_buysell_secid(row, 10)
        out += f"          <SECNAME>{symbol}</SECNAME>\n"
        out += f"          <TICKER>{symbol}</TICKER>\n"
        out += "        </SECINFO>\n"
        out += f"      </{info_tag}>\n"
        return SecurityInfo(cusip, out)
    else:
        global _missing_cusip_mappings
        if _missing_cusip_mappings is None:
            raise RuntimeError("missing_cusip_mappings not initialized")
        for mapping in _missing_cusip_mappings:
            if re.search(mapping["description_regex"], row["Description"].strip()):
                out = f"      <{mapping['info_tag']}>\n"
                out += "        <SECINFO>\n"
                out += generate_buysell_secid(row, 10)
                out += f"          <SECNAME>{mapping['symbol']}</SECNAME>\n"
                out += f"          <TICKER>{mapping['symbol']}</TICKER>\n"
                out += "        </SECINFO>\n"
                out += f"      </{mapping['info_tag']}>\n"
                break
        else:
            raise ValueError(f"No CUSIP provided and no matching pattern found for description: {row['Description'].strip()}")
        return SecurityInfo(mapping["symbol"], out)

def generate_buysell_transaction(row: dict[str, str], sell: bool, fitid: str, date_str: str, amount_default : int = None) -> tuple[str, SecurityInfo]:
    """
    Generate a buy transaction as a BUYSTOCK or BUYMF.
    Settlement date is trade date + 2 days.
    """
    activity = row["Activity"].strip()
    description = row["Description"].strip()
    cusip = row["CUSIP"].strip()
    quantity = normalize_quantity(row["Quantity"])
    price = normalize_currency(row["Price"])
    amount = normalize_currency(row["Amount"])

    if amount is None:
        amount = amount_default

    buysell = "SELL" if sell else "BUY"
    
    # Calculate price from amount if quantity exists
    if quantity and amount is not None:
        calculated_price = compute_price(quantity, amount)
        if price and abs(float(calculated_price) - float(price)) > 0.01:
            print(f"WARNING: Given price {price} differs from calculated price {calculated_price} "
                  f"for {row.get('Symbol', '')} - {description} (FITID: {fitid})")
        price = calculated_price
    # Calculate amount from price if amount missing
    elif quantity and price is not None and not amount:
        try:
            q = float(quantity)
            p = float(price)
            amount = f"{-q * p:.2f}"  # Negative because it's a buy
        except ValueError:
            amount = "0.00"
    else:
        raise ValueError(f"Transaction should have either amount or price but neither is supplied: {row}")
    
    if sell:
        outer = "SELLMF" if is_mutual_fund(row["Symbol"].strip()) else "SELLSTOCK"
    else:
        outer = "BUYMF" if is_mutual_fund(row["Symbol"].strip()) else "BUYSTOCK"

    try:
        dt_trade = datetime.datetime.strptime(date_str, "%Y%m%d")
        dt_settle = (dt_trade + datetime.timedelta(days=2)).strftime("%Y%m%d")
    except:
        dt_settle = date_str
    out =  f"          <{outer}>\n"
    out += f"            <INV{buysell}>\n"
    out +=  "            <INVTRAN>\n"
    out += f"              <FITID>{fitid}</FITID>\n"
    out += f"              <DTTRADE>{date_str}</DTTRADE>\n"
    out += f"              <DTSETTLE>{dt_settle}</DTSETTLE>\n"
    out += f"              <MEMO>{activity}: {xml_escape(description)}</MEMO>\n"
    out +=  "            </INVTRAN>\n"
    out += generate_buysell_secid(row, 12)
    out += f"            <UNITS>{quantity}</UNITS>\n"
    out += f"            <UNITPRICE>{price}</UNITPRICE>\n"
    out += f"            <TOTAL>{amount}</TOTAL>\n"
    out +=  "            <SUBACCTSEC>CASH</SUBACCTSEC>\n"
    out +=  "            <SUBACCTFUND>CASH</SUBACCTFUND>\n"
    out += f"            </INV{buysell}>\n"
    out += f"            <{buysell}TYPE>{buysell}</{buysell}TYPE>\n"
    out += f"          </{outer}>\n"
    return out, generate_buysell_security_info(row)

# ---------------------------------------------------------------------
# --- Transaction Generation Functions for fee transactions
# ---------------------------------------------------------------------
def generate_fee_transaction(row: dict[str, str], fitid: str, date_str: str, inverseAmount: bool = False) -> tuple[str, SecurityInfo]:
    """
    Generate a fee transaction as an INVBANKTRAN.
    """
    stmttrn =   "          <INVBANKTRAN>\n"
    stmttrn +=  "            <STMTTRN>\n"
    stmttrn += f"              <TRNTYPE>FEE</TRNTYPE>\n"
    stmttrn += f"              <DTPOSTED>{date_str}</DTPOSTED>\n"
    stmttrn += f"              <TRNAMT>{normalize_currency(row['Amount'], inverseAmount)}</TRNAMT>\n"
    stmttrn += f"              <FITID>{fitid}</FITID>\n"
    stmttrn += f"              <MEMO>{row['Activity'].strip()}: {xml_escape(row['Description'].strip())}</MEMO>\n"
    stmttrn +=  "              <CURRENCY>\n"
    stmttrn +=  "                <CURRATE>1.0</CURRATE>\n"
    stmttrn +=  "                <CURSYM>USD</CURSYM>\n"
    stmttrn +=  "              </CURRENCY>\n"
    stmttrn +=  "            </STMTTRN>\n"
    stmttrn +=  "            <SUBACCTFUND>CASH</SUBACCTFUND>\n"
    stmttrn +=  "          </INVBANKTRAN>\n"
    return stmttrn, None

# ---------------------------------------------------------------------
# --- Transaction Generation Functions for interest income
# ---------------------------------------------------------------------
# def generate_interest_secinfo() -> SecurityInfo:
#     """
#     Generate a SECID block for interest income.
#     """
#     return SecurityInfo( "WFPBNAE", "      <MFINFO>\n" + \
#                 "        <SECINFO>\n" + \
#                 "          <SECID>\n" + \
#                 "            <UNIQUEID>WFPBNAE</UNIQUEID>\n" + \
#                 "            <UNIQUEIDTYPE>CUSIP</UNIQUEIDTYPE>\n" + \
#                 "          </SECID>\n" + \
#                 "          <SECNAME>Unknown Security WFPBNA</SECNAME>\n" + \
#                 "          <TICKER>WFPBNA</TICKER>\n" + \
#                 "          <UNITPRICE>1</UNITPRICE>\n" + \
#                 "          <MEMO>No information available for security: WFPBNA</MEMO>\n" + \
#                 "        </SECINFO>\n" + \
#                 "        <MFASSETCLASS>\n" + \
#                 "          <PORTION>\n" + \
#                 "            <ASSETCLASS>MONEYMRKT</ASSETCLASS>\n" + \
#                 "            <PERCENT>100</PERCENT>\n" + \
#                 "          </PORTION>\n" + \
#                 "        </MFASSETCLASS>\n" + \
#                 "      </MFINFO>")

def generate_income_transaction(row: dict[str, str], fitid: str, date_str: str, incometype: str) -> tuple[str, SecurityInfo]:
    """
    Generate an income transaction as an INCOME block.
    For interest income (incometype == "INTEREST"), the SECID is set to reference
    the made-up fund (WFPBNAE). For other income types, if a SECID is provided in the CSV,
    it is used.
    """
    activity = row["Activity"].strip()
    description = row["Description"].strip()
    amount = normalize_currency(row["Amount"])
    # For interest income, use a specific time (e.g. 170000.000 without timezone)
    dttrade = date_str + "120000.000[-5:EST]"
    
    invtran =   "            <INVTRAN>\n"
    invtran += f"              <FITID>{fitid}</FITID>\n"
    invtran += f"              <DTTRADE>{dttrade}</DTTRADE>\n"
    # For interest, we drop the CSV memo and use only the description
    # (adjust as needed)
    invtran += f"              <MEMO>{xml_escape(description)}</MEMO>\n"
    invtran +=  "            </INVTRAN>\n"
    
    cusip = row["CUSIP"].strip()
    if cusip:
        secid = (
            "            <SECID>\n"
            f"              <UNIQUEID>{cusip}</UNIQUEID>\n"
            "              <UNIQUEIDTYPE>CUSIP</UNIQUEIDTYPE>\n"
            "            </SECID>\n"
        )
        secid_info = generate_buysell_security_info(row)
    else:
        raise ValueError(f"No CUSIP provided for income transaction: {description}")
    
    income =  "          <INCOME>\n"
    income += invtran
    income += secid
    income += f"            <INCOMETYPE>{incometype}</INCOMETYPE>\n"
    income += f"            <TOTAL>{amount}</TOTAL>\n"
    income +=  "            <SUBACCTSEC>CASH</SUBACCTSEC>\n"
    income +=  "            <SUBACCTFUND>CASH</SUBACCTFUND>\n"
    income +=  "          </INCOME>\n"
    return income, secid_info

def generate_intrest_transaction(row: dict[str, str], fitid: str, date_str: str) -> tuple[str, SecurityInfo]:
    """
    Generate an interest transaction as an INCOME block.
    For interest income (incometype == "INTEREST"), the SECID is set to reference
    the made-up fund (WFPBNAE). For other income types, if a SECID is provided in the CSV,
    it is used.
    """
    amount = normalize_currency(row["Amount"])

    stmttrn =   "          <INVBANKTRAN>\n"
    stmttrn +=  "            <STMTTRN>\n"
    stmttrn += f"              <TRNTYPE>INT</TRNTYPE>\n"
    stmttrn += f"              <DTPOSTED>{date_str}</DTPOSTED>\n"
    stmttrn += f"              <TRNAMT>{normalize_currency(row['Amount'], amount)}</TRNAMT>\n"
    stmttrn += f"              <FITID>{fitid}</FITID>\n"
    stmttrn += f"              <MEMO>{row['Activity'].strip()}: {xml_escape(row['Description'].strip())}</MEMO>\n"
    stmttrn +=  "              <CURRENCY>\n"
    stmttrn +=  "                <CURRATE>1.0</CURRATE>\n"
    stmttrn +=  "                <CURSYM>USD</CURSYM>\n"
    stmttrn +=  "              </CURRENCY>\n"
    stmttrn +=  "            </STMTTRN>\n"
    stmttrn +=  "            <SUBACCTFUND>CASH</SUBACCTFUND>\n"
    stmttrn +=  "          </INVBANKTRAN>\n"
    return stmttrn, None

# ---------------------------------------------------------------------
# --- Transaction Generation Functions for asset transfers
# ---------------------------------------------------------------------
def generate_asset_transfer(row: dict[str, str], fitid: str, date_str: str) -> tuple[str, SecurityInfo]:
    """
    Generate an asset transfer transaction.
    
    - If the CSV row has no CUSIP (i.e. a cash transfer), output a TRANSFER block without a SECID and UNITS.
    - If a CUSIP is present, output a TRANSFER block that includes the SECID and the number of UNITS.
    
    The DTTRADE is formatted with a full OFX datetime (using format_ofx_datetime).
    """
    # Get the full date/time string for this transaction.
    dt_obj = parse_date(row["Date"])
    full_date = format_ofx_datetime(dt_obj) if dt_obj is not None else (date_str + "120000.000[-5:EST]")
    
    activity = row["Activity"].strip()
    description = row["Description"].strip()
    cusip = row["CUSIP"].strip()
    quantity = normalize_quantity(row["Quantity"])  # May be empty for cash transfers.
    amount = normalize_currency(row["Amount"])
    
    # If no CUSIP is provided, assume it's a cash transfer.
    if not cusip:
        out =   "          <TRANSFER>\n"
        out +=  "            <INVTRAN>\n"
        out += f"              <FITID>{fitid}</FITID>\n"
        out += f"              <DTTRADE>{full_date}</DTTRADE>\n"
        out += f"              <MEMO>{activity}: {xml_escape(description)}</MEMO>\n"
        out +=  "            </INVTRAN>\n"
        out += cash_secid(12)
        out +=  "            <SUBACCTSEC>CASH</SUBACCTSEC>\n"
        out += f"            <UNITS>{amount}</UNITS>\n"
        # Determine transfer direction: if amount starts with '-' then it's OUT, else IN.
        transfer_dir = "OUT" if amount.startswith("-") else "IN"
        out += f"            <TFERACTION>{transfer_dir}</TFERACTION>\n"
        out +=  "            <POSTYPE>LONG</POSTYPE>\n"
        out +=  "          </TRANSFER>\n"
        return out, SecurityInfo(
            uniqueid='CASH',
            info_entry="      <OTHERINFO>\n"
                       "        <SECINFO>\n" +
                       cash_secid(10) +
                       "          <SECNAME>Cash Balance</SECNAME>\n"
                       "        </SECINFO>\n"
                       "      </OTHERINFO>")
    else:
        # Asset transfer for funds/stocks.
        out = "  <TRANSFER>\n"
        out += "    <INVTRAN>\n"
        out += f"      <FITID>{fitid}</FITID>\n"
        out += f"      <DTTRADE>{full_date}</DTTRADE>\n"
        out += f"      <MEMO>{activity}: {xml_escape(description)}</MEMO>\n"
        out += "    </INVTRAN>\n"
        out += "    <SECID>\n"
        out += f"      <UNIQUEID>{cusip}</UNIQUEID>\n"
        out += "      <UNIQUEIDTYPE>CUSIP</UNIQUEIDTYPE>\n"
        out += "    </SECID>\n"
        out += "    <SUBACCTSEC>CASH</SUBACCTSEC>\n"
        out += f"    <UNITS>{quantity}</UNITS>\n"
        transfer_dir = "OUT" if quantity.startswith("-") else "IN"
        out += "    <TFERACTION>IN</TFERACTION>\n"
        out += "    <POSTYPE>LONG</POSTYPE>\n"
        out += "  </TRANSFER>\n"
        return out, generate_buysell_security_info(row)

def generate_transaction(row: dict[str, str], counter: int) -> tuple[TransactionEntry, SecurityInfo]:
    """
    Generate a QFX transaction block based on the activity type in the input row.
    
    Args:
        row (dict[str, str]): Dictionary containing transaction data with keys like
            'Date', 'Activity', etc. from the CSV row
        counter (int): Sequential counter used to generate unique FITID
            
    Returns:
        tuple[TransactionEntry, SecurityInfo]: A tuple containing:
            - TransactionEntry: Contains the QFX transaction XML block and a flag indicating
              if it's an INVBANKTRAN type transaction
            - SecurityInfo: Security information for SECLIST section, or None if not applicable
            
    Raises:
        ValueError: If the activity type in the row is not recognized
        
    The function handles these transaction types:
    - Buy/Reinvest transactions (buy, reinvest dividend, rein stc/cap gain)
    - Sell transactions 
    - Asset transfers (with or without CUSIP)
    - Income transactions (dividend, interest, long/short term capital gains)
    - Fee transactions (advisory fees)
    
    For asset transfers without a CUSIP, it's treated as a cash transfer.
    For interest income, a specific made-up fund (WFPBNAE) is used as the security.
    """
    dt_obj = parse_date(row["Date"])
    date_str = dt_obj.strftime("%Y%m%d") if dt_obj else "00000000"
    fitid = f"TXN{date_str}{counter:04d}"
    act_lower = row["Activity"].strip().lower()

    if is_funding_activity(row): # This needs to be done before detecting "buy"s because "funding activity" is labeled as "buy"
        return _make_txn_entry(generate_asset_transfer(row, fitid, date_str), False)
    if act_lower in {"buy", "reinvest dividend", "rein stc gain", "rein cap gain"}:
        return _make_txn_entry(generate_buysell_transaction(row, False, fitid, date_str), False)
    elif act_lower == "sell":
        return _make_txn_entry(generate_buysell_transaction(row, True, fitid, date_str), False)
    elif act_lower in {"asset trf", "ach activity"}:
        return _make_txn_entry(generate_asset_transfer(row, fitid, date_str), False)
    elif act_lower in {"dividend"}:
        return _make_txn_entry(generate_income_transaction(row, fitid, date_str, "DIV"), False)
    elif act_lower in {"interest"}:
        return _make_txn_entry(generate_intrest_transaction(row, fitid, date_str), True)
    elif act_lower in {"lt cap gain"}:
        return _make_txn_entry(generate_income_transaction(row, fitid, date_str, "CGLONG"), False)
    elif act_lower in {"shrt trm gain"}:
        return _make_txn_entry(generate_income_transaction(row, fitid, date_str, "CGSHORT"), False)
    elif act_lower in {"advisory fee"}:
        return _make_txn_entry(generate_fee_transaction(row, fitid, date_str), True)
    elif act_lower in {"journal"}:
        return _make_txn_entry(generate_fee_transaction(row, fitid, date_str), True)
    elif act_lower in {"reinvest dist"}:
        return _make_txn_entry(generate_buysell_transaction(row, False, fitid, date_str, 0.0), False)
    else:
        raise ValueError(f"Unknown activity: {row['Activity']}")


def _output_from_input(input : str) -> str:
    if input.endswith('.csv'):
        return input[:-4] + '.qfx'
    return input + '.qfx'

# --- Main ---

def main() -> None:
    global _missing_cusip_mappings
    
    default_mapping_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "account_mapping.json")
    parser = argparse.ArgumentParser(
        description="Convert a CSV file of transactions to a QFX (OFX) file."
    )
    parser.add_argument("input_csv", help="Path to the input CSV file")
    parser.add_argument("output_qfx", nargs='?', default=None, help="Path to the output QFX file")
    parser.add_argument("--account_mapping", default=default_mapping_path,
                       help=f"Path to the JSON account mapping file (default: {default_mapping_path})")
    args = parser.parse_args()

    if not args.output_qfx:
        args.output_qfx = _output_from_input(args.input_csv)
    
    # Load account mappings
    account_mapping = load_account_id_mapping(args.account_mapping)
    
    # Load missing CUSIP mappings
    _missing_cusip_mappings = load_missing_cusip_mapping(args.account_mapping)
    
    transactions: list[TransactionEntry] = []
    counter = 1
    first_account = None
    min_date = None
    max_date = None
    securities: dict[str, SecurityInfo] = {}  # keyed by CUSIP, value = symbol

    with open(args.input_csv, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.reader(csvfile)
        valid_lines = []
        # Skip lines until we find the header row
        search_for_header = True
        for row in reader:
            if search_for_header:
                if row and row[0].strip() == 'Date' and row[1].strip() == 'Account':
                    raw_fieldnames = row
                    search_for_header = False
            else:
                if row_is_empty(row):
                    break
                valid_lines.append(row)
        if search_for_header:
            raise ValueError("Could not find header row starting with 'Date'")
        
        normalized_fieldnames = [normalize_header(field) for field in raw_fieldnames]
        for row_ in valid_lines:
            row = dict(zip(normalized_fieldnames, row_))
            if first_account is None:
                first_account = row["Account"].strip()
            dt_obj = parse_date(row["Date"])
            if dt_obj:
                if min_date is None or dt_obj < min_date:
                    min_date = dt_obj
                if max_date is None or dt_obj > max_date:
                    max_date = dt_obj
            txn_entry, secid_info = generate_transaction(row, counter)
            if secid_info is not None and secid_info.uniqueid not in securities:
                securities[secid_info.uniqueid] = secid_info
            transactions.append(txn_entry)
            counter += 1

    if first_account is None:
        raise ValueError("No account information found in CSV.")
    account_id = get_account_id(first_account, account_mapping)
    dtstart_str = format_ofx_datetime(min_date)
    dtend_str = format_ofx_datetime(max_date)
    dtasof = datetime.datetime.now().strftime("%Y%m%d")
    dtnow = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    # Preserve CSV order for non-fee transactions; collect fee transactions separately.
    non_fee_txns = [txn.txn_str for txn in transactions if not txn.is_invbanktran]
    invbanktrans = [txn.txn_str for txn in transactions if txn.is_invbanktran] 
    all_txns = non_fee_txns + invbanktrans

    # Build the main QFX file.
    qfx_lines = [
        "OFXHEADER:100",
        "DATA:OFXSGML",
        "VERSION:102",
        "SECURITY:NONE",
        "ENCODING:USASCII",
        "CHARSET:1252",
        "COMPRESSION:NONE",
        "OLDFILEUID:NONE",
        "NEWFILEUID:NONE",
        "",
        "<OFX>",
        "  <SIGNONMSGSRSV1>",
        "    <SONRS>",
        "      <STATUS>",
        "        <CODE>0</CODE>",
        "        <SEVERITY>INFO</SEVERITY>",
        "      </STATUS>",
        f"      <DTSERVER>{dtnow}</DTSERVER>",
        "      <LANGUAGE>ENG</LANGUAGE>",
        # This is a hack since Quicken looks up the institution by BID. Currently 
        # this is the BID for Interactive Brokers.
        "      <FI>",
        "        <ORG>4705</ORG>",
        "      </FI>",
        "      <INTU.BID>4705</INTU.BID>",
        "      <INTU.USERID>U424465</INTU.USERID>",
        "    </SONRS>",
        "  </SIGNONMSGSRSV1>",
        "  <INVSTMTMSGSRSV1>",
        "    <INVSTMTTRNRS>",
        "      <TRNUID>0</TRNUID>",
        "      <STATUS>",
        "        <CODE>0</CODE>",
        "        <SEVERITY>INFO</SEVERITY>",
        "      </STATUS>",
        "      <INVSTMTRS>",
        f"        <DTASOF>{dtasof}</DTASOF>",
        "        <CURDEF>USD</CURDEF>",
        "        <INVACCTFROM>",
        "          <BROKERID>WellsFargo</BROKERID>",
        f"          <ACCTID>{account_id}</ACCTID>",
        "        </INVACCTFROM>",
        "        <INVTRANLIST>",
        f"          <DTSTART>{dtstart_str}</DTSTART>",
        f"          <DTEND>{dtend_str}</DTEND>"
    ]
    for txn in all_txns:
        qfx_lines.append(txn.rstrip())
    qfx_lines.append("        </INVTRANLIST>")
    qfx_lines.append("        <INVBAL>")
    qfx_lines.append("          <AVAILCASH>0.00</AVAILCASH>")
    qfx_lines.append("          <MARGINBALANCE>0.00</MARGINBALANCE>")
    qfx_lines.append("          <SHORTBALANCE>0.00</SHORTBALANCE>")
    qfx_lines.append("        </INVBAL>")
    qfx_lines.append("      </INVSTMTRS>")
    qfx_lines.append("    </INVSTMTTRNRS>")
    qfx_lines.append("  </INVSTMTMSGSRSV1>")
    # Build the SECLIST section with STOCKINFO and MFINFO (if interest was found).
    if securities:
        qfx_lines.extend([
            "  <SECLISTMSGSRSV1>",
            "    <SECLIST>"
        ])
        for _, security_info in securities.items():
            qfx_lines.append("      " +security_info.info_entry.strip())
        qfx_lines.extend([
            "    </SECLIST>",
            "  </SECLISTMSGSRSV1>"
        ])
    qfx_lines.append("</OFX>")
    qfx_content = "\n".join(qfx_lines)
    with open(args.output_qfx, "w", encoding="utf-8") as outfile:
        outfile.write(qfx_content)
    print(f"QFX file has been generated and saved to {args.output_qfx}")

if __name__ == "__main__":
    main()

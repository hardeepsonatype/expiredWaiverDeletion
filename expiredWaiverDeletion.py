import pandas as pd
from datetime import datetime, timezone
import sys
import os # To list directory contents and check paths
import glob # To find files matching a pattern
import requests # Needed for DELETE requests

# --- Configuration ---
# CSV_FILENAME is now determined dynamically below
IQ_SERVER_URL = "http://localhost:8070" # Your Sonatype IQ Server base URL
IQ_USERNAME = "admin"
IQ_PASSWORD = "admin123" # CHANGE THIS if necessary

# --- Constants ---
API_BASE = f"{IQ_SERVER_URL}/api/v2"
# Define the expected date format string for strptime
DATE_FORMAT_STR = '%Y-%m-%dT%H:%M:%SZ' # Format with Z for UTC
CSV_PATTERN = "results-waivers-*.csv" # Pattern to find the CSV file

# --- Scope Type Mapping ---
# Maps scope types from CSV ('Scope Type') to Policy Waiver DELETE API {ownerType}
SCOPE_TYPE_MAP = {
    "root_organization": "organization", # Map root_organization to organization for API path
    "organization": "organization",
    "application": "application",
    "repository": "repository",
    "repository_container": "repository_container",
    # Add other mappings if you encounter different scope types in your CSV
}

# --- Helper Function for API Calls ---
def make_api_request(method, endpoint, auth, params=None):
    """Makes an authenticated API request to Sonatype IQ."""
    url = f"{API_BASE}/{endpoint}"
    try:
        response = requests.request(method, url, auth=auth, params=params, timeout=30)
        response.raise_for_status()
        if response.status_code == 204 and method == "DELETE": return True
        if response.status_code == 200 and not response.text: return []
        return response.json()
    except requests.exceptions.Timeout:
        print(f"ERROR: Timeout connecting to {url}", file=sys.stderr)
        return None
    except requests.exceptions.ConnectionError:
        print(f"ERROR: Could not connect to {url}. Is IQ Server running?", file=sys.stderr)
        return None
    except requests.exceptions.HTTPError as e:
        print(f"ERROR: HTTP Error {response.status_code} calling {method} {url}: {e}", file=sys.stderr)
        return None
    except requests.exceptions.RequestException as e:
        print(f"ERROR: An unexpected error occurred during the request to {url}: {e}", file=sys.stderr)
        return None
    except requests.exceptions.JSONDecodeError:
         print(f"ERROR: Could not decode JSON response from {method} {url}", file=sys.stderr)
         return None

# --- Function to parse date string robustly ---
def parse_waiver_datetime(datetime_str):
    """Parses waiver datetime string expecting UTC ('Z' suffix)."""
    if pd.isna(datetime_str) or not isinstance(datetime_str, str): return None
    try:
        if datetime_str.endswith('Z'):
             dt_naive = datetime.strptime(datetime_str[:-1], '%Y-%m-%dT%H:%M:%S')
             return dt_naive.replace(tzinfo=timezone.utc)
        else: # Try parsing with offset if no Z
             dt_with_offset = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S%z')
             return dt_with_offset
    except ValueError: # Try parsing with microseconds
        try:
            if datetime_str.endswith('Z'):
                dt_naive = datetime.strptime(datetime_str[:-1], '%Y-%m-%dT%H:%M:%S.%f')
                return dt_naive.replace(tzinfo=timezone.utc)
            else: return None # Fallback for other formats if needed
        except ValueError: return None # Return None if all parsing fails

# --- Main Execution ---
if __name__ == "__main__":
    print("Starting script to FIND and DELETE expired Sonatype IQ waivers...")
    print(f"Searching for waiver CSV file matching pattern '{CSV_PATTERN}' in the current directory...")

    # Find CSV file matching the pattern in the script's directory
    script_dir = os.path.dirname(os.path.abspath(__file__)) # Get directory where script is running
    # Use glob.glob directly if running from the target dir, or join path:
    # search_path = os.path.join(script_dir, CSV_PATTERN)
    # For simplicity assuming script is run from the directory with the CSV:
    matching_files = glob.glob(CSV_PATTERN)

    if len(matching_files) == 0:
        print(f"ERROR: No CSV file found matching pattern '{CSV_PATTERN}' in the current directory.", file=sys.stderr)
        print("Please place the exported waiver CSV file here.", file=sys.stderr)
        sys.exit(1)
    elif len(matching_files) > 1:
        print(f"ERROR: Found multiple CSV files matching pattern '{CSV_PATTERN}':", file=sys.stderr)
        for f in matching_files:
            print(f"  - {f}", file=sys.stderr)
        print("Please ensure only one waiver export CSV file is present in the directory.", file=sys.stderr)
        sys.exit(1)
    else:
        # Exactly one file found
        csv_file_to_process = matching_files[0]
        print(f"Found waiver CSV file: '{csv_file_to_process}'")

    print(f"Connecting to IQ Server: {IQ_SERVER_URL}")
    print("-" * 30)

    # Load the CSV file
    try:
        df_waivers = pd.read_csv(csv_file_to_process)
        print(f"Successfully loaded '{csv_file_to_process}'. Found {len(df_waivers)} total waivers.")
    except Exception as e:
        print(f"ERROR: Failed to read the CSV file '{csv_file_to_process}': {e}", file=sys.stderr)
        sys.exit(1)

    # Clean column names
    df_waivers.columns = df_waivers.columns.str.strip()
    col_waiver_id = 'Waiver Id'
    col_expiry = 'Expiration Date'
    col_owner_type = 'Scope Type'
    col_owner_id = 'Scope Id'
    col_component = 'Component Name'

    # Verify essential columns exist
    required_cols_list = [col_waiver_id, col_expiry, col_owner_type, col_owner_id]
    missing_cols = [col for col in required_cols_list if col not in df_waivers.columns]
    if missing_cols:
        print(f"ERROR: The CSV file '{csv_file_to_process}' is missing essential columns: {', '.join(missing_cols)}", file=sys.stderr)
        sys.exit(1)

    expired_waivers_to_delete = []

    # Get current time in UTC
    current_utc_time = datetime.now(timezone.utc)
    print(f"Current System UTC time: {current_utc_time.isoformat()}")
    print(f"Comparing waiver '{col_expiry}' against this time.")
    print("-" * 30)

    # Identify Expired Waivers
    print("Identifying expired waivers from CSV data...")
    for index, row in df_waivers.iterrows():
        expiry_date_str = row[col_expiry]
        waiver_id = row[col_waiver_id]
        if pd.isna(expiry_date_str): continue
        expiry_time = parse_waiver_datetime(str(expiry_date_str))
        if expiry_time is None:
            print(f"WARN: Could not parse Expiration Date '{expiry_date_str}' for Waiver ID {waiver_id}. Skipping.", file=sys.stderr)
            continue
        if expiry_time < current_utc_time:
            print(f"  -> MARKED FOR DELETION: ID {waiver_id} (Expired on: {expiry_date_str})")
            expired_waivers_to_delete.append({
                "Waiver Id": waiver_id,
                "Scope Type": row[col_owner_type],
                "Scope Id": row[col_owner_id],
                "Expiration Date": expiry_date_str,
                "Component Name": row.get(col_component, 'N/A')
            })

    print("-" * 30)
    print("\nExpired Waiver Identification Complete.")
    print("-" * 30)

    # --- Deletion Section ---
    if not expired_waivers_to_delete:
        print("RESULT: No expired waivers were found in the CSV file to delete.")
    else:
        print(f"RESULT: Found {len(expired_waivers_to_delete)} expired waivers to potentially delete:")
        print("-" * 30)
        for waiver in expired_waivers_to_delete:
            print(f"  - Waiver ID: {waiver['Waiver Id']}")
            print(f"    Scope    : {waiver['Scope Type']} / {waiver['Scope Id']}")
            print(f"    Expired  : {waiver['Expiration Date']}")
            print(f"    Component: {waiver['Component Name']}")
            print("-" * 10)

        print("\n" + "="*40)
        print("WARNING: Proceeding will attempt to permanently delete these waivers via API.")
        print("Ensure credentials and scope mapping are correct.")
        print("="*40)
        confirm = input("Type 'DELETE' to confirm deletion, or anything else to cancel: ")

        if confirm.strip().upper() == "DELETE":
            print("\nProceeding with deletion...")
            auth = (IQ_USERNAME, IQ_PASSWORD)
            deleted_count = 0
            failed_count = 0
            for waiver in expired_waivers_to_delete:
                waiver_id = waiver['Waiver Id']
                original_scope_type = waiver['Scope Type']
                owner_id = waiver['Scope Id']
                delete_scope_type = SCOPE_TYPE_MAP.get(original_scope_type)

                if not delete_scope_type:
                    print(f"  - ERROR: Unknown Scope Type '{original_scope_type}' for Waiver ID {waiver_id}. Skipping.", file=sys.stderr)
                    failed_count += 1
                    continue
                if pd.isna(owner_id) or not owner_id:
                    print(f"  - ERROR: Missing Scope Id for Waiver ID {waiver_id}. Skipping.", file=sys.stderr)
                    failed_count += 1
                    continue

                endpoint = f"policyWaivers/{delete_scope_type}/{owner_id}/{waiver_id}"
                print(f"Attempting to delete waiver via: DELETE {API_BASE}/{endpoint}")
                success = make_api_request("DELETE", endpoint, auth)

                if success:
                    print(f"  - Successfully deleted waiver ID: {waiver_id}")
                    deleted_count += 1
                else:
                    print(f"  - Failed to delete waiver ID: {waiver_id}")
                    failed_count += 1

            print("-" * 30)
            print(f"Deletion complete. Successfully deleted: {deleted_count}, Failed: {failed_count}")
        else:
            print("\nDeletion cancelled by user.")

    print("\nScript finished.")
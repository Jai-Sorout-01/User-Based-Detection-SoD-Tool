import pandas as pd
import os
from datetime import datetime

# File Paths
BASE_PATH = r"D:\User Based Detection with Bulk Uploader"
USER_DATA_FILE = os.path.join(BASE_PATH, "Users 1117 plant 19-09-2025.xlsx")
ROLES_AUTH_FILE = os.path.join(BASE_PATH, "Roles and Authorisation_Parivartan_V1.0.xlsx")
RISK_FILE = os.path.join(BASE_PATH, "SAP delivered standard GRC Risks_Consolidated.xlsx")
OUTPUT_FILE = os.path.join(BASE_PATH, f"User_Conflict_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")

# Helper: normalize colnames
def normalize_colname(col):
    return str(col).strip().lower().replace(" ", "_").replace("-", "_")

def load_user_data():
    """Load user data from Input sheet"""
    print("Loading User Data file:", USER_DATA_FILE)
    user_data = pd.read_excel(USER_DATA_FILE, sheet_name="Input")
    user_data.columns = [normalize_colname(c) for c in user_data.columns]
    print("User data loaded:", user_data.shape)
    return user_data

def load_role_tcode_mapping():
    """Load role-tcode mapping from multiple sheets"""
    print("Loading Role-Tcode mapping from:", ROLES_AUTH_FILE)
    possible_sheets = ["Role Tcode Mapping", "Role Tcode Mapping_New", "User Mapping"]
    role_tcode_data = []
    excel_file = pd.ExcelFile(ROLES_AUTH_FILE)
    available_sheets = excel_file.sheet_names
    print("Available sheets in Roles file:", available_sheets)

    for sheet_name in possible_sheets:
        if sheet_name in available_sheets:
            print(f"Reading sheet: {sheet_name}")
            df = pd.read_excel(ROLES_AUTH_FILE, sheet_name=sheet_name)
            df.columns = [normalize_colname(c) for c in df.columns]

            role_cols = [col for col in df.columns if 'role' in col]
            tcode_cols = [col for col in df.columns if 'tcode' in col or 't_code' in col]

            if role_cols and tcode_cols:
                mapping_df = df[[role_cols[0], tcode_cols[0]]].dropna()
                mapping_df.columns = ['role', 'tcode']
                role_tcode_data.append(mapping_df)
                print(f"Extracted {len(mapping_df)} role-tcode mappings from {sheet_name}")

    if role_tcode_data:
        combined_mapping = pd.concat(role_tcode_data, ignore_index=True)
        expanded_mapping = []
        for _, row in combined_mapping.iterrows():
            role = str(row['role']).strip().upper()
            tcodes = str(row['tcode']).strip()
            if tcodes and tcodes not in ['nan', 'NaN', '']:
                for tcode in tcodes.split(','):
                    tcode = tcode.strip().upper()
                    if tcode:
                        expanded_mapping.append({'role': role, 'tcode': tcode})
        return pd.DataFrame(expanded_mapping).drop_duplicates()

    return pd.DataFrame()

def load_user_role_assignments():
    """Extract user-role assignments from user data"""
    user_data = load_user_data()
    role_columns = [col for col in user_data.columns if 'role' in col]
    user_role_assignments = []

    if role_columns:
        for _, row in user_data.iterrows():
            user_id = str(row.get('user_id', '')).strip()
            user_name = str(row.get('user_name', '')).strip()
            for role_col in role_columns:
                role = str(row.get(role_col, '')).strip().upper()
                if role and role not in ['nan', 'NaN', '']:
                    user_role_assignments.append({
                        'user_id': user_id,
                        'user_name': user_name,
                        'role': role
                    })

    return pd.DataFrame(user_role_assignments).drop_duplicates()

def create_user_tcode_mapping():
    user_roles = load_user_role_assignments()
    role_tcodes = load_role_tcode_mapping()
    if user_roles.empty or role_tcodes.empty:
        print("⚠️ No user-role or role-tcode mapping found.")
        return pd.DataFrame()
    user_tcodes = user_roles.merge(role_tcodes, on='role', how='inner')
    print(f"Created user-tcode mapping with {len(user_tcodes)} records")
    return user_tcodes

def load_risk_data():
    """Load risk and function mapping data"""
    print("Loading Risk data from:", RISK_FILE)
    
    # Load Function T-Code Mapping
    function_map = pd.read_excel(RISK_FILE, sheet_name="Function T-Code Mapping")
    function_map.columns = [normalize_colname(c) for c in function_map.columns]
    
    # Explicit column detection based on your sheet
    func_id_col = next((c for c in function_map.columns if c in ["function_id", "functionid"]), None)
    tcode_col = next((c for c in function_map.columns if "action" in c and ("tcode" in c or "t_codes" in c)), None)
    
    if not func_id_col:
        raise ValueError("Function ID column not found in Function T-Code Mapping sheet")
    if not tcode_col:
        raise ValueError("Action (T-Codes/Apps/Services) column not found in Function T-Code Mapping sheet")
    
    function_map = function_map[[func_id_col, tcode_col]].rename(
        columns={func_id_col: "function_id", tcode_col: "tcode"}
    )
    
    # Expand comma-separated tcodes
    expanded_functions = []
    for _, row in function_map.iterrows():
        function_id = str(row['function_id']).strip().upper()
        tcodes = str(row['tcode']).strip()
        if tcodes and tcodes.lower() not in ['nan', '']:
            for tcode in tcodes.split(','):
                tcode = tcode.strip().upper()
                if tcode:
                    expanded_functions.append({"function_id": function_id, "tcode": tcode})
    
    function_map_expanded = pd.DataFrame(expanded_functions).drop_duplicates()
    print(f"Function->Tcode expanded rows: {len(function_map_expanded)}")
    
    # Load Risk Function Mapping
    risk_pairs = pd.read_excel(RISK_FILE, sheet_name="Risk Function Mapping")
    risk_pairs.columns = [normalize_colname(c) for c in risk_pairs.columns]
    
    conflict_cols = [c for c in risk_pairs.columns if "conflicting_function" in c]
    if not conflict_cols:
        raise ValueError("No conflicting_function columns found in Risk Function Mapping sheet")
    
    risk_function_pairs = []
    for _, row in risk_pairs.iterrows():
        risk_id = str(row.get("access_risk_id", "")).strip().upper()
        functions = []
        for col in conflict_cols:
            val = row[col]
            if pd.notna(val):
                val = str(val).strip().upper()
                if " - " in val:
                    val = val.split(" - ")[0].strip()
                functions.append(val)
        
        functions = sorted(set(functions))
        for i in range(len(functions)):
            for j in range(i + 1, len(functions)):
                risk_function_pairs.append({
                    "risk_id": risk_id,
                    "function_1": functions[i],
                    "function_2": functions[j]
                })
    
    risk_pairs_df = pd.DataFrame(risk_function_pairs).drop_duplicates()
    print(f"Risk function pairs: {len(risk_pairs_df)}")
    
    # Add tcode info
    func_tcodes = function_map_expanded.groupby("function_id")["tcode"].apply(
        lambda x: ", ".join(sorted(set(x)))
    ).reset_index()
    
    risk_pairs_df = risk_pairs_df.merge(
        func_tcodes, left_on="function_1", right_on="function_id", how="left"
    ).rename(columns={"tcode": "tcode1"}).drop(columns=["function_id"])
    
    risk_pairs_df = risk_pairs_df.merge(
        func_tcodes, left_on="function_2", right_on="function_id", how="left"
    ).rename(columns={"tcode": "tcode2"}).drop(columns=["function_id"])
    
    return function_map_expanded, risk_pairs_df


def analyze_conflicts():
    user_tcodes = create_user_tcode_mapping()
    if user_tcodes.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    function_map, risk_pairs = load_risk_data()
    user_functions = user_tcodes.merge(function_map, on="tcode", how="inner")

    conflicts = user_functions.merge(risk_pairs, left_on="function_id", right_on="function_1", how="inner")
    conflicts = conflicts.merge(user_functions, left_on=["function_2", "user_id"], right_on=["function_id", "user_id"], how="inner", suffixes=("_f1", "_f2"))

    final_conflicts = conflicts[["user_id", "user_name_f1", "role_f1", "role_f2", "risk_id", "function_1", "function_2", "tcode1", "tcode2", "tcode_f1", "tcode_f2"]].drop_duplicates()
    return final_conflicts.rename(columns={"user_name_f1": "user_name", "role_f1": "role_1", "role_f2": "role_2", "tcode_f1": "user_tcode_f1", "tcode_f2": "user_tcode_f2"}), user_tcodes, function_map, risk_pairs

def create_user_summary(conflicts_df, user_tcodes_df):
    summary = []
    for user_id in user_tcodes_df['user_id'].unique():
        user_data = user_tcodes_df[user_tcodes_df['user_id'] == user_id]
        user_name = user_data['user_name'].iloc[0] if not user_data.empty else ""
        roles = list(user_data['role'].unique())
        tcodes = list(user_data['tcode'].unique())
        conflict_count = len(conflicts_df[conflicts_df['user_id'] == user_id])
        summary.append({
            "user_id": user_id,
            "user_name": user_name,
            "roles": ", ".join(roles),
            "total_roles": len(roles),
            "total_tcodes": len(tcodes),
            "conflict_count": conflict_count,
            "risk_status": "HIGH RISK" if conflict_count > 0 else "SAFE"
        })
    return pd.DataFrame(summary)

def main():
    try:
        print("Starting User-Based SoD Conflict Analysis...")
        conflicts_df, user_tcodes_df, function_map_df, risk_pairs_df = analyze_conflicts()
        if user_tcodes_df.empty:
            print("❌ No data to process. Check input files.")
            return

        user_summary_df = create_user_summary(conflicts_df, user_tcodes_df)

        with pd.ExcelWriter(OUTPUT_FILE) as writer:
            user_summary_df.to_excel(writer, sheet_name="User_Summary", index=False)
            conflicts_df.to_excel(writer, sheet_name="User_Conflicts", index=False)
            user_tcodes_df.to_excel(writer, sheet_name="User_Role_Tcodes", index=False)
            function_map_df.to_excel(writer, sheet_name="Function_Tcodes", index=False)
            risk_pairs_df.to_excel(writer, sheet_name="Risk_Pairs", index=False)

        print(f"\n✅ User Conflict Report generated: {OUTPUT_FILE}")
        print(f"Total Users analyzed: {len(user_summary_df)}")
        print(f"Users with conflicts: {len(user_summary_df[user_summary_df['conflict_count'] > 0])}")
        print(f"Total Conflicts found: {len(conflicts_df)}")

        if not conflicts_df.empty:
            print(f"\n⚠️ CONFLICTS DETECTED (showing first 10):")
            for _, conflict in conflicts_df.head(10).iterrows():
                print(f"User: {conflict['user_name']} ({conflict['user_id']}) - Risk: {conflict['risk_id']} - {conflict['function_1']} vs {conflict['function_2']}")

    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
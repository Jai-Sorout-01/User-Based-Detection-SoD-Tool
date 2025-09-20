import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import io
from typing import Dict, List, Tuple, Any
import time
import os

# Page config
st.set_page_config(
    page_title="User-Based SoD Conflict Analyzer", 
    page_icon="👤", 
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS for attractive styling
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin-bottom: 2rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    
    .conflict-card {
        background: linear-gradient(135deg, #ff6b6b, #ee5a24);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        margin: 0.5rem 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    .success-card {
        background: linear-gradient(135deg, #00b894, #00cec9);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    .info-card {
        background: linear-gradient(135deg, #6c5ce7, #a29bfe);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        margin: 0.5rem 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    .user-card {
        background: linear-gradient(135deg, #fd79a8, #e84393);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        margin: 0.5rem 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        text-align: center;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        border-left: 4px solid #667eea;
    }
    
    .stButton > button {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        padding: 0.5rem 2rem;
        border-radius: 25px;
        font-weight: bold;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }
    
    .upload-area {
        border: 2px dashed #667eea;
        border-radius: 10px;
        padding: 2rem;
        text-align: center;
        background: linear-gradient(135deg, #f8f9ff, #e8f0fe);
        margin: 1rem 0;
    }
    
    .tab-content {
        background: white;
        padding: 2rem;
        border-radius: 15px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.1);
        margin-top: 1rem;
    }

    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display:none;}
</style>
""", unsafe_allow_html=True)

# Helper functions
def normalize_colname(col):
    """Normalize column names for consistent processing"""
    return str(col).strip().lower().replace(" ", "_").replace("-", "_")

@st.cache_data
def load_master_data():
    """Load and process all master data from local files"""
    try:
        BASE_PATH = r"D:\User Based Detection with Bulk Uploader"
        USER_DATA_FILE = os.path.join(BASE_PATH, "Users 1117 plant 19-09-2025.xlsx")
        ROLES_AUTH_FILE = os.path.join(BASE_PATH, "Roles and Authorisation_Parivartan_V1.0.xlsx")
        RISK_FILE = os.path.join(BASE_PATH, "SAP delivered standard GRC Risks_Consolidated.xlsx")
        
        # Check if files exist
        missing_files = []
        for file_path, file_name in [(USER_DATA_FILE, "Users 1117 plant 19-09-2025.xlsx"), 
                                   (ROLES_AUTH_FILE, "Roles and Authorisation_Parivartan *V1.0.xlsx"),
                                   (RISK_FILE, "SAP delivered standard GRC Risks_Consolidated.xlsx")]:
            if not os.path.exists(file_path):
                missing_files.append(file_name)
        
        if missing_files:
            st.error(f"Missing files: {', '.join(missing_files)}")
            return None
        
        # Load user data
        user_data = pd.read_excel(USER_DATA_FILE, sheet_name="Input")
        user_data.columns = [normalize_colname(c) for c in user_data.columns]
        
        # Load role-tcode mapping
        role_tcode_data = []
        excel_file = pd.ExcelFile(ROLES_AUTH_FILE)
        available_sheets = excel_file.sheet_names
        
        possible_sheets = ["Role Tcode Mapping", "Role Tcode Mapping_New", "User Mapping"]
        
        for sheet_name in possible_sheets:
            if sheet_name in available_sheets:
                df = pd.read_excel(ROLES_AUTH_FILE, sheet_name=sheet_name)
                df.columns = [normalize_colname(c) for c in df.columns]
                
                role_cols = [col for col in df.columns if 'role' in col.lower()]
                tcode_cols = [col for col in df.columns if 'tcode' in col.lower() or 't_code' in col.lower()]
                
                if role_cols and tcode_cols:
                    role_col = role_cols[0]
                    tcode_col = tcode_cols[0]
                    
                    mapping_df = df[[role_col, tcode_col]].dropna()
                    mapping_df.columns = ['role', 'tcode']
                    role_tcode_data.append(mapping_df)
        
        if not role_tcode_data:
            st.error("Could not find role-tcode mapping in any sheet")
            return None
        
        # Combine role-tcode mappings
        role_tcode_mapping = pd.concat(role_tcode_data, ignore_index=True)
        
        # Expand comma-separated tcodes
        expanded_role_tcodes = []
        for _, row in role_tcode_mapping.iterrows():
            role = str(row['role']).strip().upper()
            tcodes = str(row['tcode']).strip()
            
            if tcodes and tcodes not in ['nan', 'NaN', '']:
                for tcode in tcodes.split(','):
                    tcode = tcode.strip().upper()
                    if tcode:
                        expanded_role_tcodes.append({'role': role, 'tcode': tcode})
        
        role_tcode_df = pd.DataFrame(expanded_role_tcodes).drop_duplicates()
        
        # Extract user-role assignments
        user_role_assignments = []
        role_columns = [col for col in user_data.columns if 'role' in col.lower()]
        
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
        
        user_roles_df = pd.DataFrame(user_role_assignments).drop_duplicates()
        
        # Create user-tcode mapping
        user_tcodes_df = user_roles_df.merge(role_tcode_df, on='role', how='inner')
        
        # Load function mapping
        function_map = pd.read_excel(RISK_FILE, sheet_name="Function T-Code Mapping")
        function_map.columns = [normalize_colname(c) for c in function_map.columns]
        
        func_id_col = next((c for c in function_map.columns if "function_id" in c), None)
        tcode_col = next((c for c in function_map.columns if "action" in c and ("tcode" in c or "t_code" in c)), None)
        
        if not func_id_col or not tcode_col:
            st.error("Could not detect Function ID or T-code columns")
            return None
        
        function_map = function_map[[func_id_col, tcode_col]].rename(
            columns={func_id_col: "function_id", tcode_col: "tcode"}
        )
        
        # Expand function tcodes
        expanded_functions = []
        for _, row in function_map.iterrows():
            function_id = str(row['function_id']).strip().upper()
            tcodes = str(row['tcode']).strip()
            
            if tcodes and tcodes not in ['nan', 'NaN', '']:
                for tcode in tcodes.split(','):
                    tcode = tcode.strip().upper()
                    if tcode:
                        expanded_functions.append({
                            'function_id': function_id,
                            'tcode': tcode
                        })
        
        function_map_expanded = pd.DataFrame(expanded_functions).drop_duplicates()
        
        # Load risk pairs
        risk_pairs = pd.read_excel(RISK_FILE, sheet_name="Risk Function Mapping")
        risk_pairs.columns = [normalize_colname(c) for c in risk_pairs.columns]
        
        conflict_cols = [c for c in risk_pairs.columns if "conflicting_function" in c]
        
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
        
        # Add function tcodes to risk pairs
        func_tcodes = function_map_expanded.groupby("function_id")["tcode"].apply(
            lambda x: ", ".join(sorted(set(x)))
        ).reset_index()
        
        risk_pairs_df = risk_pairs_df.merge(
            func_tcodes, left_on="function_1", right_on="function_id", how="left"
        ).rename(columns={"tcode": "tcode1"}).drop(columns=["function_id"])
        
        risk_pairs_df = risk_pairs_df.merge(
            func_tcodes, left_on="function_2", right_on="function_id", how="left"
        ).rename(columns={"tcode": "tcode2"}).drop(columns=["function_id"])
        
        return {
            "user_tcodes": user_tcodes_df,
            "function_map": function_map_expanded,
            "risk_pairs": risk_pairs_df,
            "user_roles": user_roles_df,
            "role_tcodes": role_tcode_df
        }
        
    except Exception as e:
        st.error(f"Error loading master data: {str(e)}")
        return None

def process_uploaded_user_data(uploaded_file, master_data: Dict) -> Dict:
    """Process uploaded user data and extract user-role mappings"""
    try:
        # Read uploaded file
        if uploaded_file.name.endswith('.csv'):
            uploaded_df = pd.read_csv(uploaded_file)
        else:
            # Try to read Excel file
            excel_file = pd.ExcelFile(uploaded_file)
            sheet_names = excel_file.sheet_names
            
            # Let user choose sheet if multiple sheets
            if len(sheet_names) > 1:
                st.info(f"Multiple sheets found: {', '.join(sheet_names)}")
                selected_sheet = st.selectbox(
                    "Select sheet to analyze:",
                    options=sheet_names,
                    help="Choose which sheet contains the user data"
                )
                uploaded_df = pd.read_excel(uploaded_file, sheet_name=selected_sheet)
            else:
                uploaded_df = pd.read_excel(uploaded_file, sheet_name=sheet_names[0])
        
        # Normalize column names
        uploaded_df.columns = [normalize_colname(c) for c in uploaded_df.columns]
        
        # Detect user ID columns
        possible_user_cols = ['user_id', 'userid', 'user', 'id', 'username', 'user_name', 'login', 'account']
        user_id_col = None
        user_name_col = None
        
        for col in uploaded_df.columns:
            if any(user_col in col for user_col in possible_user_cols):
                if 'name' in col:
                    user_name_col = col
                else:
                    user_id_col = col
                    break
        
        if not user_id_col:
            # Try to auto-detect from first few columns
            if len(uploaded_df.columns) > 0:
                user_id_col = uploaded_df.columns[0]
                st.warning(f"Auto-detected user ID column: '{user_id_col}'. Please verify this is correct.")
            else:
                st.error("Could not detect user ID column")
                return None
        
        if not user_name_col and len(uploaded_df.columns) > 1:
            user_name_col = uploaded_df.columns[1]
        
        # Detect role columns
        role_columns = [col for col in uploaded_df.columns if 'role' in col]
        
        if not role_columns:
            st.warning("No role columns detected. Looking for other potential role fields...")
            # Look for other potential role fields
            potential_role_cols = [col for col in uploaded_df.columns if 
                                 any(keyword in col for keyword in ['auth', 'access', 'profile', 'group', 'permission'])]
            if potential_role_cols:
                role_columns = potential_role_cols
                st.info(f"Found potential role columns: {', '.join(role_columns)}")
        
        # Extract user-role assignments from uploaded data
        user_role_assignments = []
        
        for _, row in uploaded_df.iterrows():
            user_id = str(row.get(user_id_col, '')).strip()
            user_name = str(row.get(user_name_col, user_id)).strip() if user_name_col else user_id
            
            if not user_id or user_id in ['nan', 'NaN', '']:
                continue
            
            if role_columns:
                for role_col in role_columns:
                    role_value = str(row.get(role_col, '')).strip().upper()
                    if role_value and role_value not in ['nan', 'NaN', '']:
                        # Handle comma-separated roles
                        for role in role_value.split(','):
                            role = role.strip()
                            if role:
                                user_role_assignments.append({
                                    'user_id': user_id,
                                    'user_name': user_name,
                                    'role': role
                                })
            else:
                # If no role columns, just add user info
                user_role_assignments.append({
                    'user_id': user_id,
                    'user_name': user_name,
                    'role': 'UNKNOWN'
                })
        
        if not user_role_assignments:
            st.error("No valid user-role assignments found in uploaded file")
            return None
        
        uploaded_user_roles = pd.DataFrame(user_role_assignments).drop_duplicates()
        
        # Map roles to tcodes using master data
        if master_data and 'role_tcodes' in master_data:
            uploaded_user_tcodes = uploaded_user_roles.merge(
                master_data['role_tcodes'], on='role', how='inner'
            )
            
            if uploaded_user_tcodes.empty:
                st.warning("No role matches found in master data. Users might have different role names.")
                # Show role comparison
                uploaded_roles = set(uploaded_user_roles['role'].unique())
                master_roles = set(master_data['role_tcodes']['role'].unique())
                
                st.write("**Uploaded file roles:**", list(uploaded_roles)[:10])
                st.write("**Master data roles:**", list(master_roles)[:10])
                
                return {
                    'user_roles': uploaded_user_roles,
                    'user_tcodes': pd.DataFrame(),
                    'stats': {
                        'total_users': len(uploaded_user_roles['user_id'].unique()),
                        'total_roles': len(uploaded_user_roles['role'].unique()),
                        'role_matches': 0
                    }
                }
        else:
            uploaded_user_tcodes = pd.DataFrame()
        
        return {
            'user_roles': uploaded_user_roles,
            'user_tcodes': uploaded_user_tcodes,
            'stats': {
                'total_users': len(uploaded_user_roles['user_id'].unique()),
                'total_roles': len(uploaded_user_roles['role'].unique()),
                'total_tcodes': len(uploaded_user_tcodes['tcode'].unique()) if not uploaded_user_tcodes.empty else 0,
                'role_matches': len(uploaded_user_tcodes['role'].unique()) if not uploaded_user_tcodes.empty else 0
            }
        }
        
    except Exception as e:
        st.error(f"Error processing uploaded file: {str(e)}")
        return None

def analyze_uploaded_users_conflicts(uploaded_user_data: Dict, master_data: Dict) -> pd.DataFrame:
    """Analyze conflicts for uploaded users"""
    if not uploaded_user_data or not master_data or uploaded_user_data['user_tcodes'].empty:
        return pd.DataFrame()
    
    user_tcodes = uploaded_user_data['user_tcodes']
    function_map = master_data["function_map"]
    risk_pairs = master_data["risk_pairs"]
    
    # Map users to functions
    user_functions = user_tcodes.merge(function_map, on="tcode", how="inner")
    
    if user_functions.empty:
        return pd.DataFrame()
    
    # Find conflicts
    conflicts = user_functions.merge(
        risk_pairs, left_on="function_id", right_on="function_1", how="inner"
    )
    
    conflicts = conflicts.merge(
        user_functions,
        left_on=["function_2", "user_id"],
        right_on=["function_id", "user_id"],
        suffixes=("_f1", "_f2"),
        how="inner"
    )
    
    if conflicts.empty:
        return pd.DataFrame()
    
    # Select relevant columns
    final_conflicts = conflicts[[
        "user_id", "user_name_f1", "role_f1", "role_f2", "risk_id",
        "function_1", "function_2", "tcode1", "tcode2", "tcode_f1", "tcode_f2"
    ]].drop_duplicates()
    
    final_conflicts = final_conflicts.rename(columns={
        "user_name_f1": "user_name",
        "role_f1": "role_1",
        "role_f2": "role_2",
        "tcode_f1": "user_tcode_f1",
        "tcode_f2": "user_tcode_f2"
    })
    
    return final_conflicts

def analyze_user_conflicts(user_id: str, master_data: Dict) -> Dict:
    """Analyze conflicts for a specific user"""
    if not master_data:
        return {"user_id": user_id, "conflicts": [], "functions": [], "tcodes": [], "roles": [], "conflict_count": 0}
    
    user_id = str(user_id).strip()
    
    # Get user's data
    user_data_df = master_data["user_tcodes"][master_data["user_tcodes"]["user_id"] == user_id]
    
    if user_data_df.empty:
        return {"user_id": user_id, "conflicts": [], "functions": [], "tcodes": [], "roles": [], "conflict_count": 0, "error": f"User {user_id} not found"}
    
    user_name = user_data_df["user_name"].iloc[0] if not user_data_df.empty else ""
    user_roles = list(user_data_df["role"].unique())
    user_tcodes = list(user_data_df["tcode"].unique())
    
    if len(user_tcodes) == 0:
        return {"user_id": user_id, "user_name": user_name, "conflicts": [], "functions": [], "tcodes": [], "roles": user_roles, "conflict_count": 0, "error": f"No T-codes found for user {user_id}"}
    
    # Map user's tcodes to functions
    user_func = user_data_df.merge(master_data["function_map"], on="tcode", how="left")
    user_functions = list(user_func["function_id"].dropna().unique())
    
    conflicts = []
    risk_pairs = master_data["risk_pairs"]
    
    # Track user's function access with roles
    user_function_access = {}
    for _, row in user_func.iterrows():
        if pd.notna(row["function_id"]):
            func_id = row["function_id"]
            role = row["role"]
            tcode = row["tcode"]
            
            if func_id not in user_function_access:
                user_function_access[func_id] = {"roles": set(), "tcodes": set()}
            user_function_access[func_id]["roles"].add(role)
            user_function_access[func_id]["tcodes"].add(tcode)
    
    # Check conflicts by comparing user's functions with risk pairs
    processed_pairs = set()
    
    for _, risk_pair in risk_pairs.iterrows():
        func1_name = str(risk_pair.get("function_1", "")).strip()
        func2_name = str(risk_pair.get("function_2", "")).strip()
        risk_id = str(risk_pair.get("risk_id", "")).strip()
        
        # Skip invalid entries
        if not all([func1_name, func2_name, risk_id]) or \
           any(x in ["nan", "NaN", "None", ""] for x in [func1_name, func2_name, risk_id]):
            continue
        
        # Check if user has access to both conflicting functions
        if func1_name in user_function_access and func2_name in user_function_access:
            func_pair = tuple(sorted([func1_name, func2_name]))
            if func_pair not in processed_pairs:
                processed_pairs.add(func_pair)
                
                func1_roles = list(user_function_access[func1_name]["roles"])
                func2_roles = list(user_function_access[func2_name]["roles"])
                func1_user_tcodes = list(user_function_access[func1_name]["tcodes"])
                func2_user_tcodes = list(user_function_access[func2_name]["tcodes"])
                
                conflicts.append({
                    "type": f"SoD Violation ({risk_id})",
                    "function_1": func1_name,
                    "function_2": func2_name,
                    "description": f"User has access to conflicting functions: {func1_name} through roles {', '.join(func1_roles)} AND {func2_name} through roles {', '.join(func2_roles)}. This violates segregation of duties.",
                    "risk_id": risk_id,
                    "role_1": ", ".join(sorted(func1_roles)),
                    "role_2": ", ".join(sorted(func2_roles)),
                    "tcode_1": ", ".join(sorted(func1_user_tcodes)),
                    "tcode_2": ", ".join(sorted(func2_user_tcodes)),
                    "functions": [func1_name, func2_name],
                    "tcodes_involved": func1_user_tcodes + func2_user_tcodes
                })
    
    return {
        "user_id": user_id,
        "user_name": user_name,
        "roles": user_roles,
        "functions": user_functions,
        "tcodes": user_tcodes,
        "conflicts": conflicts,
        "conflict_count": len(conflicts),
        "function_details": [f"{func} (Roles: {', '.join(access['roles'])}, T-codes: {', '.join(access['tcodes'])})" 
                           for func, access in user_function_access.items()],
        "debug_info": {
            "tcodes_found": len(user_tcodes),
            "functions_identified": len(user_functions),
            "roles_assigned": len(user_roles),
            "risk_pairs_checked": len(risk_pairs),
            "function_access_mapping": {k: {"roles": list(v["roles"]), "tcodes": list(v["tcodes"])} 
                                      for k, v in user_function_access.items()},
            "conflicts_detected": len(conflicts)
        }
    }

def analyze_bulk_user_conflicts(master_data: Dict) -> pd.DataFrame:
    """Analyze conflicts for all users"""
    if not master_data:
        return pd.DataFrame()
    
    user_tcodes = master_data["user_tcodes"]
    function_map = master_data["function_map"]
    risk_pairs = master_data["risk_pairs"]
    
    # Map users to functions
    user_functions = user_tcodes.merge(function_map, on="tcode", how="inner")
    
    # Find conflicts
    conflicts = user_functions.merge(
        risk_pairs, left_on="function_id", right_on="function_1", how="inner"
    )
    
    conflicts = conflicts.merge(
        user_functions,
        left_on=["function_2", "user_id"],
        right_on=["function_id", "user_id"],
        suffixes=("_f1", "_f2"),
        how="inner"
    )
    
    # Select relevant columns
    final_conflicts = conflicts[[
        "user_id", "user_name_f1", "role_f1", "role_f2", "risk_id",
        "function_1", "function_2", "tcode1", "tcode2", "tcode_f1", "tcode_f2"
    ]].drop_duplicates()
    
    final_conflicts = final_conflicts.rename(columns={
        "user_name_f1": "user_name",
        "role_f1": "role_1",
        "role_f2": "role_2",
        "tcode_f1": "user_tcode_f1",
        "tcode_f2": "user_tcode_f2"
    })
    
    return final_conflicts

def create_user_conflict_report_excel(conflicts_df: pd.DataFrame, master_data: Dict = None, uploaded_data: Dict = None) -> bytes:
    """Create Excel report with multiple sheets for user-based conflicts"""
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Main conflicts sheet
        conflicts_df.to_excel(writer, sheet_name="User_Conflicts", index=False)
        
        # User summary sheet
        if not conflicts_df.empty:
            # Use uploaded data if available, otherwise use master data
            data_source = uploaded_data if uploaded_data and not uploaded_data['user_tcodes'].empty else master_data
            
            if data_source and 'user_tcodes' in data_source:
                user_summary = []
                for user_id in data_source["user_tcodes"]['user_id'].unique():
                    user_data = data_source["user_tcodes"][data_source["user_tcodes"]['user_id'] == user_id]
                    user_name = user_data['user_name'].iloc[0] if not user_data.empty else ""
                    roles = list(user_data['role'].unique())
                    tcodes = list(user_data['tcode'].unique())
                    conflict_count = len(conflicts_df[conflicts_df['user_id'] == user_id])
                    
                    user_summary.append({
                        'user_id': user_id,
                        'user_name': user_name,
                        'roles': ', '.join(roles),
                        'total_roles': len(roles),
                        'total_tcodes': len(tcodes),
                        'conflict_count': conflict_count,
                        'risk_status': 'HIGH RISK' if conflict_count > 0 else 'SAFE'
                    })
                
                user_summary_df = pd.DataFrame(user_summary)
                user_summary_df.to_excel(writer, sheet_name="User_Summary", index=False)
        
        # Additional sheets
        if master_data:
            master_data["function_map"].to_excel(writer, sheet_name="Function_Tcodes", index=False)
            master_data["risk_pairs"].to_excel(writer, sheet_name="Risk_Pairs", index=False)
            
        # Uploaded data sheets if available
        if uploaded_data and not uploaded_data['user_tcodes'].empty:
            uploaded_data['user_tcodes'].to_excel(writer, sheet_name="Uploaded_User_Data", index=False)
    
    output.seek(0)
    return output.read()

# Initialize session state
if "master_data" not in st.session_state:
    st.session_state.master_data = None

# Header
st.markdown("""
<div class="main-header">
    <h1>👤 User-Based SoD Conflict Analyzer</h1>
    <p>Comprehensive User-Level Segregation of Duties Conflict Detection System</p>
</div>
""", unsafe_allow_html=True)

# Load master data automatically
if st.session_state.master_data is None:
    with st.spinner("🔄 Loading master data from local files..."):
        st.session_state.master_data = load_master_data()
        if st.session_state.master_data:
            st.success("✅ Master data loaded successfully from local files!")
        else:
            st.error("❌ Could not load master data. Please check file paths and ensure all required files are present.")
            st.stop()

# Only show tabs if master data is loaded
if st.session_state.master_data:
    master_data = st.session_state.master_data
    
    # Sidebar stats
    with st.sidebar:
        st.markdown("""
        <div style='text-align: center; padding: 1rem; background: linear-gradient(135deg, #667eea, #764ba2); border-radius: 10px; color: white; margin-bottom: 1rem;'>
            <h3>📊 System Stats</h3>
        </div>
        """, unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("🔗 Risk Pairs", len(master_data["risk_pairs"]))
            st.metric("⚙️ Functions", len(master_data["function_map"]["function_id"].unique()))
        with col2:
            st.metric("💻 T-Codes", len(master_data["function_map"]["tcode"].unique()))
            st.metric("👥 Users", len(master_data["user_tcodes"]["user_id"].unique()))

    # Main tabs
    tab1, tab2 = st.tabs(["🔍 User Conflict Checker", "📊 Bulk Analysis & Reports"])

    # ==================== MANUAL TAB ====================
    with tab1:
        st.markdown('<div class="tab-content">', unsafe_allow_html=True)
        
        st.markdown("### 🎯 Select User to Analyze")
        
        # Show available users
        available_users = sorted(master_data["user_tcodes"]["user_id"].unique())
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            # Option 1: Dropdown selection
            user_input = st.selectbox(
                "Choose from Available Users",
                options=[""] + available_users,
                help=f"Select from {len(available_users)} available users to check for conflicts",
                key="user_selector"
            )
            
            st.markdown("**OR**")
            
            # Option 2: Manual text input
            manual_user = st.text_input(
                "Type User ID Manually",
                placeholder="Enter user ID...",
                help="Type any user ID to analyze",
                key="manual_user_input"
            )
        
        with col2:
            # Use manual input if provided, otherwise use dropdown
            final_user = manual_user.strip() if manual_user.strip() else user_input
            
            # Show quick stats for selected user
            if final_user:
                user_data = master_data["user_tcodes"][master_data["user_tcodes"]["user_id"] == final_user]
                if not user_data.empty:
                    user_name = user_data["user_name"].iloc[0]
                    user_roles = list(user_data["role"].unique())
                    user_tcodes = list(user_data["tcode"].unique())
                    user_functions = user_data.merge(master_data['function_map'], on='tcode', how='inner')['function_id'].unique()
                    
                    st.markdown(f"""
                    <div class="user-card">
                        <h4>👤 {user_name} ({final_user})</h4>
                        <p>• Roles: {len(user_roles)}</p>
                        <p>• T-Codes: {len(user_tcodes)}</p>
                        <p>• Functions: {len(user_functions)}</p>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div style='background: #fff3cd; padding: 1rem; border-radius: 8px; border-left: 4px solid #ffc107;'>
                        <h4>⚠️ User Not Found</h4>
                        <p>User ID '{final_user}' not found in master data</p>
                    </div>
                    """, unsafe_allow_html=True)
        
        # Automatically analyze user when selected
        if final_user and final_user != "":
            user_analysis = analyze_user_conflicts(final_user, master_data)
            
            st.markdown("---")
            
            # User info header
            st.markdown(f"""
            <div class="info-card">
                <h3>👤 User: {user_analysis.get('user_name', 'Unknown')} ({user_analysis['user_id']})</h3>
                <p>Roles: {len(user_analysis['roles'])} | Functions: {len(user_analysis['functions'])} | T-Codes: {len(user_analysis['tcodes'])} | Conflicts: {user_analysis['conflict_count']}</p>
            </div>
            """, unsafe_allow_html=True)
            
            # Metrics row
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.markdown(f"""
                <div class="metric-card">
                    <h2>{len(user_analysis['roles'])}</h2>
                    <p>Roles</p>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                st.markdown(f"""
                <div class="metric-card">
                    <h2>{len(user_analysis['functions'])}</h2>
                    <p>Functions</p>
                </div>
                """, unsafe_allow_html=True)
            
            with col3:
                st.markdown(f"""
                <div class="metric-card">
                    <h2>{len(user_analysis['tcodes'])}</h2>
                    <p>T-Codes</p>
                </div>
                """, unsafe_allow_html=True)
            
            with col4:
                st.markdown(f"""
                <div class="metric-card">
                    <h2 style="color: {'#e74c3c' if user_analysis['conflict_count'] > 0 else '#27ae60'}">{user_analysis['conflict_count']}</h2>
                    <p>Conflicts</p>
                </div>
                """, unsafe_allow_html=True)
            
            with col5:
                status = "🔴 HIGH RISK" if user_analysis['conflict_count'] > 0 else "🟢 SAFE"
                color = "#e74c3c" if user_analysis['conflict_count'] > 0 else "#27ae60"
                st.markdown(f"""
                <div class="metric-card">
                    <h4 style="color: {color}">{status}</h4>
                    <p>Status</p>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("---")
            
            # Detailed view
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("### 👔 Roles")
                if user_analysis['roles']:
                    for role in user_analysis['roles']:
                        st.markdown(f"• `{role}`")
                else:
                    st.info("No roles found for this user")
            
            with col2:
                st.markdown("### 📋 Functions")
                if user_analysis['functions']:
                    for func in user_analysis['functions']:
                        st.markdown(f"• `{func}`")
                else:
                    st.info("No functions found for this user")
            
            with col3:
                st.markdown("### 💻 T-Codes")
                if user_analysis['tcodes']:
                    # Show first 10 tcodes, with option to show more
                    for tcode in user_analysis['tcodes'][:10]:
                        st.markdown(f"• `{tcode}`")
                    if len(user_analysis['tcodes']) > 10:
                        st.markdown(f"... and {len(user_analysis['tcodes']) - 10} more")
                else:
                    st.info("No T-codes found for this user")
            
            # Conflicts section
            if user_analysis['conflicts']:
                st.markdown("### ⚠️ Detected Conflicts")
                for i, conflict in enumerate(user_analysis['conflicts'], 1):
                    st.markdown(f"""
                    <div class="conflict-card">
                        <h4>Conflict #{i}: {conflict.get('type', 'SoD Violation')}</h4>
                        <p><strong>{conflict.get('function_1', 'N/A')}</strong> ↔️ <strong>{conflict.get('function_2', 'N/A')}</strong></p>
                        <p>{conflict.get('description', 'Segregation of Duties conflict detected')}</p>
                        <small>
                            Roles: {conflict.get('role_1', 'N/A')} ↔️ {conflict.get('role_2', 'N/A')}<br>
                            T-Codes: {conflict.get('tcode_1', 'N/A')} ↔️ {conflict.get('tcode_2', 'N/A')}
                        </small>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.markdown("""
                <div class="success-card">
                    <h3>🎉 No Conflicts Found!</h3>
                    <p>This user appears to have proper segregation of duties</p>
                </div>
                """, unsafe_allow_html=True)
        
        st.markdown('</div>', unsafe_allow_html=True)

    # ==================== BULK TAB ====================
    with tab2:
        st.markdown('<div class="tab-content">', unsafe_allow_html=True)
        
        st.markdown("### 📊 System-Wide Analysis")
        
        # Create two main sections
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.markdown("### 📈 Master Data Analysis")
            total_users = len(master_data["user_tcodes"]["user_id"].unique())
            total_roles = len(master_data["user_tcodes"]["role"].unique())
            total_tcodes = len(master_data["user_tcodes"]["tcode"].unique())
            
            st.markdown(f"""
            <div class="info-card">
                <h4>📊 System Statistics</h4>
                <p>Total Users: {total_users}</p>
                <p>Total Roles: {total_roles}</p>
                <p>Total T-Codes: {total_tcodes}</p>
            </div>
            """, unsafe_allow_html=True)
            
            if st.button("🚀 Analyze Master Data Users", type="primary", use_container_width=True):
                with st.spinner("🔄 Analyzing all master data users for conflicts..."):
                    progress_bar = st.progress(0)
                    
                    # Analyze conflicts for all users
                    conflicts_df = analyze_bulk_user_conflicts(master_data)
                    progress_bar.progress(100)
                    
                    st.session_state['master_conflicts'] = conflicts_df
        
        with col2:
            st.markdown("### 📤 Upload Your Own Data")
            
            st.markdown("""
            <div class="upload-area">
                <h4>📁 Upload User Data File</h4>
                <p>Upload CSV or Excel file containing user-role assignments</p>
                <small>File should contain columns like: user_id, user_name, role1, role2, etc.</small>
            </div>
            """, unsafe_allow_html=True)
            
            uploaded_file = st.file_uploader(
                "Choose file",
                type=['csv', 'xlsx', 'xls'],
                help="Upload a CSV or Excel file with user data",
                key="bulk_uploader"
            )
            
            if uploaded_file is not None:
                with st.spinner("📊 Processing uploaded file..."):
                    uploaded_user_data = process_uploaded_user_data(uploaded_file, master_data)
                    
                    if uploaded_user_data:
                        st.success("✅ File processed successfully!")
                        
                        # Show upload stats
                        stats = uploaded_user_data['stats']
                        
                        col_a, col_b, col_c = st.columns(3)
                        with col_a:
                            st.metric("Users", stats['total_users'])
                        with col_b:
                            st.metric("Roles", stats['total_roles'])
                        with col_c:
                            st.metric("T-Codes", stats['total_tcodes'])
                        
                        if stats['role_matches'] > 0:
                            if st.button("🔍 Analyze Uploaded Users", type="primary", use_container_width=True):
                                with st.spinner("🔄 Analyzing uploaded users for conflicts..."):
                                    uploaded_conflicts = analyze_uploaded_users_conflicts(uploaded_user_data, master_data)
                                    st.session_state['uploaded_conflicts'] = uploaded_conflicts
                                    st.session_state['uploaded_data'] = uploaded_user_data
                        else:
                            st.warning("⚠️ No role matches found with master data. Cannot analyze conflicts.")
                            st.info("💡 Make sure the role names in your file match the master data role names.")
        
        st.markdown("---")
        
        # Display results section
        st.markdown("### 📋 Analysis Results")
        
        # Tabs for different result types
        result_tab1, result_tab2 = st.tabs(["🏠 Master Data Results", "📤 Uploaded Data Results"])
        
        with result_tab1:
            if 'master_conflicts' in st.session_state:
                conflicts_df = st.session_state['master_conflicts']
                
                if not conflicts_df.empty:
                    # Results summary
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.markdown(f"""
                        <div class="metric-card">
                            <h2 style="color: #e74c3c">{len(conflicts_df)}</h2>
                            <p>Total Conflicts</p>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col2:
                        unique_users = conflicts_df['user_id'].nunique()
                        st.markdown(f"""
                        <div class="metric-card">
                            <h2>{unique_users}</h2>
                            <p>Affected Users</p>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col3:
                        unique_risks = conflicts_df['risk_id'].nunique()
                        st.markdown(f"""
                        <div class="metric-card">
                            <h2 style="color: #e74c3c">{unique_risks}</h2>
                            <p>Risk Types</p>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    # Conflicts table
                    st.markdown("#### 📋 Master Data Conflicts")
                    st.dataframe(conflicts_df, use_container_width=True)
                    
                    # Download button
                    excel_data = create_user_conflict_report_excel(conflicts_df, master_data)
                    
                    filename = f"Master_SoD_Conflicts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                    
                    st.download_button(
                        label="📥 Download Master Data Report (Excel)",
                        data=excel_data,
                        file_name=filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                    
                else:
                    st.markdown("""
                    <div class="success-card">
                        <h2>🎉 No Conflicts Found in Master Data!</h2>
                        <p>All users show proper segregation of duties</p>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Still provide download option for clean report
                    excel_data = create_user_conflict_report_excel(pd.DataFrame(), master_data)
                    
                    filename = f"Master_Clean_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                    
                    st.download_button(
                        label="📥 Download Clean Master Report (Excel)",
                        data=excel_data,
                        file_name=filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
            else:
                st.info("👆 Click 'Analyze Master Data Users' to see results here")
        
        with result_tab2:
            if 'uploaded_conflicts' in st.session_state:
                uploaded_conflicts = st.session_state['uploaded_conflicts']
                uploaded_data = st.session_state.get('uploaded_data', {})
                
                if not uploaded_conflicts.empty:
                    # Results summary
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.markdown(f"""
                        <div class="metric-card">
                            <h2 style="color: #e74c3c">{len(uploaded_conflicts)}</h2>
                            <p>Total Conflicts</p>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col2:
                        unique_users = uploaded_conflicts['user_id'].nunique()
                        st.markdown(f"""
                        <div class="metric-card">
                            <h2>{unique_users}</h2>
                            <p>Affected Users</p>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col3:
                        unique_risks = uploaded_conflicts['risk_id'].nunique()
                        st.markdown(f"""
                        <div class="metric-card">
                            <h2 style="color: #e74c3c">{unique_risks}</h2>
                            <p>Risk Types</p>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    # Conflicts table
                    st.markdown("#### 📋 Uploaded Data Conflicts")
                    st.dataframe(uploaded_conflicts, use_container_width=True)
                    
                    # Download button
                    excel_data = create_user_conflict_report_excel(uploaded_conflicts, master_data, uploaded_data)
                    
                    filename = f"Uploaded_SoD_Conflicts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                    
                    st.download_button(
                        label="📥 Download Uploaded Data Report (Excel)",
                        data=excel_data,
                        file_name=filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                    
                else:
                    st.markdown("""
                    <div class="success-card">
                        <h2>🎉 No Conflicts Found in Uploaded Data!</h2>
                        <p>All uploaded users show proper segregation of duties</p>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Still provide download option for clean report
                    excel_data = create_user_conflict_report_excel(pd.DataFrame(), master_data, uploaded_data)
                    
                    filename = f"Uploaded_Clean_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                    
                    st.download_button(
                        label="📥 Download Clean Uploaded Report (Excel)",
                        data=excel_data,
                        file_name=filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
            else:
                st.info("👆 Upload a file and click 'Analyze Uploaded Users' to see results here")
        
        st.markdown('</div>', unsafe_allow_html=True)

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; padding: 2rem; background: linear-gradient(90deg, #667eea 0%, #764ba2 100%); border-radius: 10px; color: white; margin-top: 2rem;'>
    <h4>👤 User-Based SoD Conflict Analyzer</h4>
    <p>Comprehensive segregation of duties analysis across your entire organization</p>
</div>
""", unsafe_allow_html=True) 

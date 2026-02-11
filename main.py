"""
REAL ESTATE ERP - Professional Production System
With 5 Google Sheets Integration
"""

import streamlit as st
import pandas as pd
import numpy as np
import json
import os
import bcrypt
import re
from datetime import datetime, timedelta
from io import BytesIO
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List, Optional, Any

# ============================================
# SYSTEM CONFIGURATION
# ============================================
st.set_page_config(
    page_title="Real Estate ERP Pro",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Professional CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        font-weight: 700;
        margin-bottom: 1rem;
        text-align: center;
        padding: 1rem;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    .metric-card {
        background: white;
        padding: 20px;
        border-radius: 12px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.05);
        border: 1px solid #e5e7eb;
        margin-bottom: 10px;
    }
    .data-table {
        background: white;
        border-radius: 10px;
        overflow: hidden;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .filter-card {
        background: #f8fafc;
        padding: 15px;
        border-radius: 10px;
        border-left: 4px solid #4F46E5;
        margin-bottom: 10px;
    }
    .success-box {
        background: #D1FAE5;
        color: #065F46;
        padding: 12px;
        border-radius: 8px;
        border-left: 4px solid #10B981;
        margin: 8px 0;
    }
    .warning-box {
        background: #FEF3C7;
        color: #92400E;
        padding: 12px;
        border-radius: 8px;
        border-left: 4px solid #F59E0B;
        margin: 8px 0;
    }
    .info-box {
        background: #DBEAFE;
        color: #1E40AF;
        padding: 12px;
        border-radius: 8px;
        border-left: 4px solid #3B82F6;
        margin: 8px 0;
    }
    .multi-sheet-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 15px;
        border-radius: 10px;
        margin: 10px 0;
    }
    </style>
""", unsafe_allow_html=True)

# ============================================
# 5 GOOGLE SHEETS MANAGER (NEW)
# ============================================
class MultiSheetManager:
    """Professional manager for 5 Google Sheets integration"""
    
    def __init__(self):
        self.config_file = "data/multi_sheets_config.json"
        os.makedirs(os.path.dirname(self.config_file), exist_ok=True)
    
    @st.cache_data(ttl=600)
    def get_all_sheets_config(_self):
        """Get configuration for all 5 sheets"""
        if os.path.exists(_self.config_file):
            try:
                with open(_self.config_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                return {"sheets": [], "version": "1.0"}
        return {"sheets": [], "version": "1.0"}
    
    def get_sheet_by_type(_self, sheet_type):
        """Get sheet configuration by type"""
        config = _self.get_all_sheets_config()
        for sheet in config.get("sheets", []):
            if sheet.get("type") == sheet_type:
                return sheet
        return None
    
    @st.cache_data(ttl=300)
    def load_sheet_by_type(_self, sheet_type):
        """Load data from specific sheet type"""
        sheet_config = _self.get_sheet_by_type(sheet_type)
        if not sheet_config:
            return None, f"Sheet type '{sheet_type}' not configured"
        
        try:
            url = sheet_config.get('url')
            if not url:
                return None, "URL not configured"
            
            # Extract sheet ID from URL
            patterns = [r'/spreadsheets/d/([a-zA-Z0-9-_]+)', r'id=([a-zA-Z0-9-_]+)']
            sheet_id = None
            for pattern in patterns:
                match = re.search(pattern, url)
                if match:
                    sheet_id = match.group(1)
                    break
            
            if not sheet_id:
                return None, "Invalid URL format"
            
            # Load data
            import_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
            df = pd.read_excel(import_url)
            
            if df.empty:
                return None, "Sheet is empty"
            
            return df, "Successfully loaded"
            
        except Exception as e:
            return None, f"Error: {str(e)}"
    
    def save_sheets_config(_self, sheets_data):
        """Save configuration for multiple sheets"""
        config = {
            "sheets": sheets_data,
            "last_updated": datetime.now().isoformat(),
            "version": "1.0"
        }
        
        try:
            with open(_self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            st.cache_data.clear()
            return True
        except Exception as e:
            st.error(f"Configuration error: {str(e)}")
            return False
    
    def test_all_connections(_self):
        """Test connections to all configured sheets"""
        sheet_types = ["properties", "clients", "users", "activity", "transactions"]
        results = []
        
        for sheet_type in sheet_types:
            df, message = _self.load_sheet_by_type(sheet_type)
            if df is not None:
                results.append({
                    "type": sheet_type,
                    "status": "✅ Connected",
                    "records": len(df),
                    "columns": list(df.columns)
                })
            else:
                results.append({
                    "type": sheet_type,
                    "status": "❌ Failed",
                    "error": message,
                    "records": 0
                })
        
        return results

# ============================================
# SHEET HANDLERS FOR EACH TYPE
# ============================================

class PropertiesSheetHandler:
    """Handler for Properties_DB sheet"""
    
    def __init__(self):
        self.sheet_manager = MultiSheetManager()
        self.sheet_type = "properties"
    
    @st.cache_data(ttl=300)
    def load_properties(_self):
        """Load properties from Properties_DB sheet"""
        df, message = _self.sheet_manager.load_sheet_by_type(_self.sheet_type)
        
        if df is None:
            return pd.DataFrame()
        
        # Clean column names
        df.columns = df.columns.str.strip()
        
        # Map possible column names to standard names
        column_mapping = {
            'unit_id': ['unit_id', 'id', 'property_id'],
            'unit_type': ['unit_type', 'property_type', 'type'],
            'listing_type': ['listing_type', 'sale_rent', 'transaction_type'],
            'area': ['area', 'region', 'location', 'city'],
            'address': ['address', 'main_address', 'street_address'],
            'price_total': ['price_total', 'price', 'total_price', 'value'],
            'area_sqm': ['area_sqm', 'area_m2', 'size', 'square_meters'],
            'rooms': ['rooms', 'bedrooms', 'number_of_rooms'],
            'bathrooms': ['bathrooms', 'washrooms', 'number_of_bathrooms'],
            'floor_number': ['floor_number', 'floor', 'level'],
            'status': ['status', 'unit_status', 'availability']
        }
        
        # Apply column mapping
        for standard_col, possible_names in column_mapping.items():
            for possible in possible_names:
                if possible in df.columns and standard_col not in df.columns:
                    df.rename(columns={possible: standard_col}, inplace=True)
        
        # Ensure numeric columns
        numeric_cols = ['price_total', 'area_sqm', 'rooms', 'bathrooms', 'floor_number']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df

class ClientsSheetHandler:
    """Handler for Global_Leads sheet"""
    
    def __init__(self):
        self.sheet_manager = MultiSheetManager()
        self.sheet_type = "clients"
    
    @st.cache_data(ttl=300)
    def load_clients(_self, username=None, role=None):
        """Load clients from Global_Leads sheet"""
        df, message = _self.sheet_manager.load_sheet_by_type(_self.sheet_type)
        
        if df is None:
            return []
        
        # Clean column names
        df.columns = df.columns.str.strip()
        
        # Map columns
        column_mapping = {
            'client_id': ['client_id', 'id'],
            'name': ['name', 'client_name', 'full_name'],
            'phone': ['phone', 'phone_number', 'mobile'],
            'assigned_to': ['assigned_to', 'agent', 'sales_agent'],
            'status': ['status', 'client_status'],
            'source': ['source', 'lead_source'],
            'budget': ['budget', 'budget_range', 'price_range']
        }
        
        for standard_col, possible_names in column_mapping.items():
            for possible in possible_names:
                if possible in df.columns and standard_col not in df.columns:
                    df.rename(columns={possible: standard_col}, inplace=True)
        
        # Apply role-based filtering
        if username and role == 'sales':
            if 'assigned_to' in df.columns:
                df = df[df['assigned_to'].astype(str).str.contains(username, case=False, na=False)]
        
        return df.to_dict('records') if not df.empty else []
    
    def get_stats(_self, username=None, role=None):
        """Get client statistics"""
        clients = _self.load_clients(username, role)
        
        if not clients:
            return {
                "total": 0,
                "active": 0,
                "by_status": {},
                "by_source": {}
            }
        
        df = pd.DataFrame(clients)
        stats = {
            "total": len(clients),
            "active": len(df[df['status'] == 'Active']) if 'status' in df.columns else 0,
            "by_status": df['status'].value_counts().to_dict() if 'status' in df.columns else {},
            "by_source": df['source'].value_counts().to_dict() if 'source' in df.columns else {}
        }
        
        return stats

class UsersSheetHandler:
    """Handler for User_Registry sheet"""
    
    def __init__(self):
        self.sheet_manager = MultiSheetManager()
        self.sheet_type = "users"
    
    @st.cache_data(ttl=600)
    def load_users(_self):
        """Load users from User_Registry sheet"""
        df, message = _self.sheet_manager.load_sheet_by_type(_self.sheet_type)
        
        if df is None:
            return {}
        
        # Clean column names
        df.columns = df.columns.str.strip()
        
        # Convert to dictionary
        users = {}
        for _, row in df.iterrows():
            username = str(row.get('username', '')).strip()
            if username:
                users[username] = {
                    "password": str(row.get('password', '')),
                    "full_name": str(row.get('full_name', username)),
                    "role": str(row.get('role', 'user')).lower(),
                    "email": str(row.get('email', '')),
                    "department": str(row.get('department', 'General')),
                    "id": int(row.get('id', 0)) if pd.notna(row.get('id', 0)) else 0
                }
        
        return users

class ActivityLogsHandler:
    """Handler for Activity_Logs sheet"""
    
    def __init__(self):
        self.sheet_manager = MultiSheetManager()
        self.sheet_type = "activity"
    
    @st.cache_data(ttl=300)
    def load_activity_logs(_self, limit=100):
        """Load activity logs"""
        df, message = _self.sheet_manager.load_sheet_by_type(_self.sheet_type)
        
        if df is None:
            return pd.DataFrame()
        
        # Clean column names
        df.columns = df.columns.str.strip()
        
        return df.head(limit) if not df.empty else pd.DataFrame()

class TransactionsHandler:
    """Handler for Transactions sheet"""
    
    def __init__(self):
        self.sheet_manager = MultiSheetManager()
        self.sheet_type = "transactions"
    
    @st.cache_data(ttl=300)
    def load_transactions(_self):
        """Load transactions"""
        df, message = _self.sheet_manager.load_sheet_by_type(_self.sheet_type)
        
        if df is None:
            return pd.DataFrame()
        
        # Clean column names
        df.columns = df.columns.str.strip()
        
        return df

# ============================================
# PROFESSIONAL AUTHENTICATION SYSTEM (UPDATED)
# ============================================
@st.cache_data(ttl=300)
def load_users():
    """Load users from JSON file with professional security"""
    try:
        # Try Google Sheets first
        users_handler = UsersSheetHandler()
        sheet_users = users_handler.load_users()
        
        if sheet_users:
            # Save to local file for backup
            with open("users.json", "w", encoding='utf-8') as f:
                json.dump(sheet_users, f, indent=2, ensure_ascii=False)
            return sheet_users
        
        # Fallback to local file
        if os.path.exists("users.json"):
            with open("users.json", "r", encoding='utf-8') as f:
                users = json.load(f)
                return users
    except Exception as e:
        st.error(f"Authentication error: {str(e)}")
    
    # Create default professional users
    default_users = {
        "admin": {
            "id": 1,
            "role": "owner",
            "password": bcrypt.hashpw("admin123".encode('utf-8'), bcrypt.gensalt()).decode('utf-8'),
            "email": "admin@realestate.com",
            "full_name": "System Administrator",
            "department": "Management",
            "created": datetime.now().isoformat()
        },
        "manager": {
            "id": 2,
            "role": "manager",
            "password": bcrypt.hashpw("manager123".encode('utf-8'), bcrypt.gensalt()).decode('utf-8'),
            "email": "manager@realestate.com",
            "full_name": "Operations Manager",
            "department": "Management",
            "created": datetime.now().isoformat()
        },
        "analyst": {
            "id": 3,
            "role": "data_analyst",
            "password": bcrypt.hashpw("analyst123".encode('utf-8'), bcrypt.gensalt()).decode('utf-8'),
            "email": "analyst@realestate.com",
            "full_name": "Data Analyst",
            "department": "Analytics",
            "created": datetime.now().isoformat()
        },
        "sales1": {
            "id": 4,
            "role": "sales",
            "password": bcrypt.hashpw("sales123".encode('utf-8'), bcrypt.gensalt()).decode('utf-8'),
            "email": "john.smith@realestate.com",
            "full_name": "John Smith",
            "department": "Sales",
            "created": datetime.now().isoformat()
        },
        "sales2": {
            "id": 5,
            "role": "sales",
            "password": bcrypt.hashpw("sales123".encode('utf-8'), bcrypt.gensalt()).decode('utf-8'),
            "email": "sarah.jones@realestate.com",
            "full_name": "Sarah Jones",
            "department": "Sales",
            "created": datetime.now().isoformat()
        },
        "data_entry": {
            "id": 6,
            "role": "data_entry",
            "password": bcrypt.hashpw("data123".encode('utf-8'), bcrypt.gensalt()).decode('utf-8'),
            "email": "data@realestate.com",
            "full_name": "Data Specialist",
            "department": "Operations",
            "created": datetime.now().isoformat()
        }
    }
    
    try:
        with open("users.json", "w", encoding='utf-8') as f:
            json.dump(default_users, f, indent=2, ensure_ascii=False)
    except Exception as e:
        st.error(f"System initialization error: {str(e)}")
    
    return default_users

def authenticate_user(username, password):
    """Professional user authentication"""
    if not username or not password:
        return None
    
    users = load_users()
    
    if username in users:
        stored_hash = users[username]["password"]
        
        if isinstance(stored_hash, str):
            stored_hash = stored_hash.encode('utf-8')
        
        try:
            if bcrypt.checkpw(password.encode('utf-8'), stored_hash):
                return {
                    "username": username,
                    "role": users[username]["role"],
                    "id": users[username]["id"],
                    "email": users[username].get("email", ""),
                    "full_name": users[username].get("full_name", username),
                    "department": users[username].get("department", "")
                }
        except:
            # Try simple password check for demo
            if password == stored_hash.decode('utf-8') if isinstance(stored_hash, bytes) else stored_hash:
                return {
                    "username": username,
                    "role": users[username]["role"],
                    "id": users[username]["id"],
                    "email": users[username].get("email", ""),
                    "full_name": users[username].get("full_name", username),
                    "department": users[username].get("department", "")
                }
    
    return None

def log_activity(username, action, details=""):
    """Professional activity logging"""
    try:
        os.makedirs("logs", exist_ok=True)
        log_entry = f"{datetime.now().isoformat()}|{username}|{action}|{details}\n"
        with open("logs/activity.log", "a", encoding="utf-8") as f:
            f.write(log_entry)
    except Exception:
        pass

# ============================================
# UPDATED CLIENT DATABASE (USES GOOGLE SHEETS)
# ============================================
class ClientDatabase:
    """Professional client database with Google Sheets integration"""
    
    def __init__(self):
        self.sheet_handler = ClientsSheetHandler()
        self.local_backup = "data/clients_backup.json"
        os.makedirs(os.path.dirname(self.local_backup), exist_ok=True)
        
    @st.cache_data(ttl=300)
    def load_clients(_self, username, user_role):
        """Load clients with role-based access control"""
        clients = _self.sheet_handler.load_clients(username, user_role)
        
        if clients:
            # Save backup
            try:
                with open(_self.local_backup, 'w', encoding='utf-8') as f:
                    json.dump({"clients": clients, "last_sync": datetime.now().isoformat()}, f, indent=2)
            except:
                pass
            
            return {
                "clients": clients,
                "next_id": len(clients) + 1,
                "sources": ["Website", "Referral", "Walk-in", "Social Media", "Advertisement", "Direct"]
            }
        else:
            # Try local backup
            return _self._load_local_backup(username, user_role)
    
    def _load_local_backup(_self, username, user_role):
        """Load from local backup"""
        try:
            if os.path.exists(_self.local_backup):
                with open(_self.local_backup, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    clients = data.get("clients", [])
                    
                    # Apply role-based filtering
                    if user_role == 'sales':
                        clients = [c for c in clients if c.get('assigned_to') == username]
                    
                    return {
                        "clients": clients,
                        "next_id": len(clients) + 1,
                        "sources": ["Website", "Referral", "Walk-in", "Social Media", "Advertisement", "Direct"]
                    }
        except:
            pass
        
        return {"clients": [], "next_id": 1, "sources": []}
    
    def add_client(_self, client_data, username):
        """Add new client with professional data handling"""
        try:
            # Load existing data
            existing_data = _self.load_clients("system", "owner")
            
            # Auto-assign to sales user if applicable
            if not client_data.get('assigned_to') and st.session_state.user['role'] == 'sales':
                client_data['assigned_to'] = username
            
            # Set professional defaults
            client_data['client_id'] = existing_data.get('next_id', 1)
            client_data['created_at'] = datetime.now().isoformat()
            client_data['last_contact'] = datetime.now().isoformat()
            client_data['conversion_stage'] = 'Lead'
            
            # Add to database
            existing_data['clients'].append(client_data)
            existing_data['next_id'] += 1
            
            # Log activity
            log_activity(username, "add_client", 
                        f"Client: {client_data.get('name')}, Value: {client_data.get('value', 0)}")
            
            # Note: In production, this would update Google Sheets
            # For now, we save to local backup
            try:
                with open(_self.local_backup, 'w', encoding='utf-8') as f:
                    json.dump({"clients": existing_data['clients'], "last_sync": datetime.now().isoformat()}, 
                             f, indent=2, ensure_ascii=False)
            except:
                pass
            
            return client_data['client_id']
            
        except Exception as e:
            st.error(f"Client addition error: {str(e)}")
            return None
    
    def get_client_metrics(_self, username, user_role):
        """Get professional client metrics"""
        clients_data = _self.load_clients(username, user_role)
        clients = clients_data.get('clients', [])
        
        if not clients:
            return {
                "total": 0,
                "active": 0,
                "total_value": 0,
                "by_stage": {},
                "by_source": {}
            }
        
        # Calculate metrics
        active = [c for c in clients if c.get('status') == 'Active']
        total_value = sum(float(c.get('value', 0)) for c in clients)
        
        by_stage = {}
        by_source = {}
        
        for client in clients:
            stage = client.get('conversion_stage', 'Unknown')
            source = client.get('source', 'Unknown')
            
            by_stage[stage] = by_stage.get(stage, 0) + 1
            by_source[source] = by_source.get(source, 0) + 1
        
        return {
            "total": len(clients),
            "active": len(active),
            "total_value": total_value,
            "by_stage": by_stage,
            "by_source": by_source
        }

# ============================================
# UPDATED PROPERTY DATABASE (USES GOOGLE SHEETS)
# ============================================
class PropertyDatabase:
    """Professional property database management"""
    
    def __init__(self):
        self.sheet_handler = PropertiesSheetHandler()
        self.local_backup = "data/properties_backup.xlsx"
        os.makedirs(os.path.dirname(self.local_backup), exist_ok=True)
        
    @st.cache_data(ttl=300)
    def load_properties(_self):
        """Load property data from Google Sheets"""
        df = _self.sheet_handler.load_properties()
        
        if not df.empty:
            # Save backup
            try:
                df.to_excel(_self.local_backup, index=False)
            except:
                pass
            
            return df
        else:
            # Try local backup
            return _self._load_local_backup()
    
    def _load_local_backup(_self):
        """Load from local backup"""
        if os.path.exists(_self.local_backup):
            try:
                return pd.read_excel(_self.local_backup)
            except:
                pass
        return pd.DataFrame()
    
    def save_properties(_self, df):
        """Save property data (local only for now)"""
        try:
            df.to_excel(_self.local_backup, index=False)
            st.cache_data.clear()  # Clear cache
            return True
        except Exception as e:
            st.error(f"Save error: {str(e)}")
            return False
    
    def get_inventory_metrics(_self):
        """Get professional inventory metrics"""
        df = _self.load_properties()
        
        if df.empty:
            return {
                "total_units": 0,
                "available_units": 0,
                "sold_units": 0,
                "total_value": 0,
                "avg_price": 0
            }
        
        # Calculate metrics
        total_units = len(df)
        
        # Check status columns
        status_cols = ['status', 'unit_status', 'availability']
        status_col = None
        for col in status_cols:
            if col in df.columns:
                status_col = col
                break
        
        if status_col:
            available_units = len(df[df[status_col].astype(str).str.contains('available', case=False, na=False)])
            sold_units = len(df[df[status_col].astype(str).str.contains('sold|rented', case=False, na=False)])
        else:
            available_units = total_units
            sold_units = 0
        
        price_cols = ['price_total', 'price', 'value']
        price_col = None
        for col in price_cols:
            if col in df.columns:
                price_col = col
                break
        
        if price_col:
            total_value = df[price_col].sum()
            avg_price = df[price_col].mean()
        else:
            total_value = 0
            avg_price = 0
        
        return {
            "total_units": total_units,
            "available_units": available_units,
            "sold_units": sold_units,
            "total_value": total_value,
            "avg_price": avg_price
        }

# ============================================
# ADVANCED SALES INTERFACE (KEEPS ALL FILTERS)
# ============================================
class AdvancedSalesInterface:
    """Professional sales interface with advanced filtering"""
    
    def __init__(self):
        self.property_db = PropertyDatabase()
    
    def render_interface(_self):
        """Render professional sales interface"""
        st.markdown("<div class='main-header'>Property Inventory Search</div>", unsafe_allow_html=True)
        
        # Load data
        df = _self.property_db.load_properties()
        
        if df.empty:
            st.markdown("<div class='info-box'>No property data available. Please upload data first.</div>", unsafe_allow_html=True)
            return
        
        # Main layout
        col1, col2 = st.columns([3, 1])
        
        with col2:
            st.markdown("<div class='filter-card'>", unsafe_allow_html=True)
            filtered_df = _self._render_filters(df)
            st.markdown("</div>", unsafe_allow_html=True)
        
        with col1:
            _self._render_results(filtered_df)
    
    def _render_filters(_self, df):
        """Render advanced filters (KEEPS ALL EXISTING FILTERS)"""
        filtered_df = df.copy()
        
        # Price Range
        st.markdown("**Price Range**")
        price_cols = ['price_total', 'price', 'value']
        price_col = None
        for col in price_cols:
            if col in df.columns:
                price_col = col
                break
        
        if price_col:
            min_price = float(df[price_col].min()) if not df[price_col].isnull().all() else 0
            max_price = float(df[price_col].max()) if not df[price_col].isnull().all() else 1000000
            
            col_price1, col_price2 = st.columns(2)
            with col_price1:
                price_from = st.number_input("From", value=min_price, step=10000.0, key="price_from")
            with col_price2:
                price_to = st.number_input("To", value=max_price, step=10000.0, key="price_to")
            
            filtered_df = filtered_df[(filtered_df[price_col] >= price_from) & 
                                    (filtered_df[price_col] <= price_to)]
        
        # Area Range
        st.markdown("**Area Range (m²)**")
        area_cols = ['area_sqm', 'size', 'net_area']
        area_col = None
        for col in area_cols:
            if col in df.columns:
                area_col = col
                break
        
        if area_col:
            min_area = float(df[area_col].min()) if not df[area_col].isnull().all() else 0
            max_area = float(df[area_col].max()) if not df[area_col].isnull().all() else 500
            
            col_area1, col_area2 = st.columns(2)
            with col_area1:
                area_from = st.number_input("From", value=min_area, step=10.0, key="area_from")
            with col_area2:
                area_to = st.number_input("To", value=max_area, step=10.0, key="area_to")
            
            filtered_df = filtered_df[(filtered_df[area_col] >= area_from) & 
                                    (filtered_df[area_col] <= area_to)]
        
        # Property Type with Select All
        type_cols = ['unit_type', 'property_type', 'type']
        type_col = None
        for col in type_cols:
            if col in df.columns:
                type_col = col
                break
        
        if type_col:
            st.markdown("**Property Type**")
            options = sorted(df[type_col].dropna().unique().tolist())
            select_all = st.checkbox("Select All Types", value=True, key="select_all_types")
            default_options = options if select_all else []
            
            selected_types = st.multiselect("Select property types:", options, 
                                          default=default_options, key="type_filter")
            if selected_types:
                filtered_df = filtered_df[filtered_df[type_col].isin(selected_types)]
        
        # Location with Select All
        location_cols = ['area', 'region', 'location', 'city']
        location_col = None
        for col in location_cols:
            if col in df.columns:
                location_col = col
                break
        
        if location_col:
            st.markdown("**Location**")
            locations = sorted(df[location_col].dropna().unique().tolist())
            select_all_locations = st.checkbox("Select All Locations", value=True, key="select_all_locations")
            default_locations = locations if select_all_locations else []
            
            selected_locations = st.multiselect("Select locations:", locations,
                                              default=default_locations, key="location_filter")
            if selected_locations:
                filtered_df = filtered_df[filtered_df[location_col].isin(selected_locations)]
        
        # Rooms
        rooms_cols = ['rooms', 'bedrooms', 'number_of_rooms']
        rooms_col = None
        for col in rooms_cols:
            if col in df.columns:
                rooms_col = col
                break
        
        if rooms_col:
            st.markdown("**Number of Rooms**")
            rooms_options = sorted(df[rooms_col].dropna().unique().tolist())
            select_all_rooms = st.checkbox("Select All", value=True, key="select_all_rooms")
            default_rooms = rooms_options if select_all_rooms else []
            
            selected_rooms = st.multiselect("Select rooms:", rooms_options,
                                          default=default_rooms, key="rooms_filter")
            if selected_rooms:
                filtered_df = filtered_df[filtered_df[rooms_col].isin(selected_rooms)]
        
        # Status
        status_cols = ['status', 'unit_status', 'availability']
        status_col = None
        for col in status_cols:
            if col in df.columns:
                status_col = col
                break
        
        if status_col:
            st.markdown("**Property Status**")
            status_options = sorted(df[status_col].dropna().unique().tolist())
            select_all_status = st.checkbox("Select All Status", value=True, key="select_all_status")
            default_status = status_options if select_all_status else []
            
            selected_status = st.multiselect("Select status:", status_options,
                                           default=default_status, key="status_filter")
            if selected_status:
                filtered_df = filtered_df[filtered_df[status_col].isin(selected_status)]
        
        # Advanced Features
        with st.expander("Advanced Features", expanded=False):
            # Amenities
            amenities = ['electricity', 'water', 'gas', 'elevator', 'garage', 'furnished']
            for amenity in amenities:
                if amenity in df.columns:
                    st.markdown(f"**{amenity.title()}**")
                    options = ['All', 'Yes', 'No']
                    selection = st.selectbox(f"{amenity.title()}:", options, key=f"amenity_{amenity}")
                    
                    if selection == 'Yes':
                        filtered_df = filtered_df[filtered_df[amenity] == True]
                    elif selection == 'No':
                        filtered_df = filtered_df[filtered_df[amenity] == False]
        
        return filtered_df
    
    def _render_results(_self, filtered_df):
        """Render search results professionally"""
        if filtered_df.empty:
            st.markdown("<div class='warning-box'>No properties match your search criteria.</div>", unsafe_allow_html=True)
            return
        
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
            st.metric("Results Found", len(filtered_df))
            st.markdown("</div>", unsafe_allow_html=True)
        
        with col2:
            price_cols = ['price_total', 'price', 'value']
            price_col = None
            for col in price_cols:
                if col in filtered_df.columns:
                    price_col = col
                    break
            
            if price_col:
                avg_price = filtered_df[price_col].mean()
                st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
                st.metric("Average Price", f"${avg_price:,.0f}")
                st.markdown("</div>", unsafe_allow_html=True)
        
        with col3:
            area_cols = ['area_sqm', 'size', 'net_area']
            area_col = None
            for col in area_cols:
                if col in filtered_df.columns:
                    area_col = col
                    break
            
            if area_col:
                avg_area = filtered_df[area_col].mean()
                st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
                st.metric("Average Area", f"{avg_area:,.0f} m²")
                st.markdown("</div>", unsafe_allow_html=True)
        
        with col4:
            type_cols = ['unit_type', 'property_type', 'type']
            type_col = None
            for col in type_cols:
                if col in filtered_df.columns:
                    type_col = col
                    break
            
            if type_col:
                top_type = filtered_df[type_col].mode()[0] if not filtered_df[type_col].mode().empty else "N/A"
                st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
                st.metric("Most Common Type", top_type)
                st.markdown("</div>", unsafe_allow_html=True)
        
        # Keyword Search
        st.markdown("**Keyword Search**")
        keyword = st.text_input("Search across all text fields:", 
                              placeholder="e.g., sea view, garage, modern, luxury")
        
        if keyword:
            mask = pd.Series(False, index=filtered_df.index)
            text_cols = [col for col in filtered_df.columns if filtered_df[col].dtype == 'object']
            for col in text_cols:
                mask = mask | filtered_df[col].astype(str).str.contains(keyword, case=False, na=False)
            filtered_df = filtered_df[mask]
        
        # Display results
        st.markdown("<div class='data-table'>", unsafe_allow_html=True)
        st.dataframe(filtered_df, use_container_width=True, height=400)
        st.markdown("</div>", unsafe_allow_html=True)
        
        # Export functionality
        if not filtered_df.empty:
            st.markdown("---")
            buffer = BytesIO()
            filtered_df.to_excel(buffer, index=False, engine='openpyxl')
            
            st.download_button(
                label="📥 Export Results (Excel)",
                data=buffer.getvalue(),
                file_name=f"property_search_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

# ============================================
# MULTI-SHEET DASHBOARD (NEW)
# ============================================
def render_multi_sheet_dashboard():
    """Dashboard showing data from all 5 sheets"""
    st.markdown("<div class='main-header'>Multi-Sheet Analytics Dashboard</div>", unsafe_allow_html=True)
    
    # Initialize handlers
    sheet_manager = MultiSheetManager()
    properties_handler = PropertiesSheetHandler()
    clients_handler = ClientsSheetHandler()
    users_handler = UsersSheetHandler()
    activity_handler = ActivityLogsHandler()
    transactions_handler = TransactionsHandler()
    
    # Check if sheets are configured
    sheets_config = sheet_manager.get_all_sheets_config()
    
    if not sheets_config.get("sheets"):
        st.warning("⚠️ No Google Sheets configured yet.")
        st.info("Please configure your 5 Google Sheets using the configuration panel.")
        return
    
    # Display configuration status
    st.markdown("### 📊 Sheets Status")
    cols = st.columns(5)
    
    sheet_types = ["properties", "clients", "users", "activity", "transactions"]
    sheet_names = ["Properties", "Clients", "Users", "Activity", "Transactions"]
    
    for i, (sheet_type, sheet_name) in enumerate(zip(sheet_types, sheet_names)):
        with cols[i]:
            sheet_config = sheet_manager.get_sheet_by_type(sheet_type)
            if sheet_config:
                st.success(f"✅ {sheet_name}")
                st.caption("Configured")
            else:
                st.warning(f"⏳ {sheet_name}")
                st.caption("Not configured")
    
    # Stats from each sheet
    st.markdown("---")
    st.markdown("### 📈 Quick Statistics")
    
    try:
        # Properties stats
        properties_df = properties_handler.load_properties()
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Properties", len(properties_df))
        with col2:
            if 'price_total' in properties_df.columns:
                total_value = properties_df['price_total'].sum()
                st.metric("Portfolio Value", f"${total_value:,.0f}")
        with col3:
            if 'unit_type' in properties_df.columns:
                types = properties_df['unit_type'].nunique()
                st.metric("Property Types", types)
        with col4:
            if 'area' in properties_df.columns:
                locations = properties_df['area'].nunique()
                st.metric("Locations", locations)
        
        # Clients stats
        clients = clients_handler.load_clients()
        col5, col6, col7, col8 = st.columns(4)
        
        with col5:
            st.metric("Total Clients", len(clients))
        with col6:
            active_clients = len([c for c in clients if c.get('status') == 'Active'])
            st.metric("Active Clients", active_clients)
        with col7:
            users = users_handler.load_users()
            st.metric("System Users", len(users))
        with col8:
            transactions_df = transactions_handler.load_transactions()
            st.metric("Transactions", len(transactions_df))
        
    except Exception as e:
        st.warning(f"Some statistics unavailable: {str(e)}")
    
    # Data preview tabs
    st.markdown("---")
    st.markdown("### 👁️ Data Preview")
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Properties", "Clients", "Users", "Activity", "Transactions"])
    
    with tab1:
        st.markdown("#### Properties Database")
        if not properties_df.empty:
            st.dataframe(properties_df.head(10), use_container_width=True)
            st.caption(f"Showing 10 of {len(properties_df)} properties")
        else:
            st.info("No properties data available")
    
    with tab2:
        st.markdown("#### Global Leads")
        if clients:
            df = pd.DataFrame(clients)
            st.dataframe(df.head(10), use_container_width=True)
            st.caption(f"Showing 10 of {len(clients)} clients")
        else:
            st.info("No clients data available")
    
    with tab3:
        st.markdown("#### User Registry")
        users = users_handler.load_users()
        if users:
            df = pd.DataFrame.from_dict(users, orient='index')
            st.dataframe(df.head(10), use_container_width=True)
            st.caption(f"Showing 10 of {len(users)} users")
        else:
            st.info("No users data available")
    
    with tab4:
        st.markdown("#### Activity Logs")
        activity_df = activity_handler.load_activity_logs(20)
        if not activity_df.empty:
            st.dataframe(activity_df, use_container_width=True)
            st.caption("Showing latest 20 activities")
        else:
            st.info("No activity logs available")
    
    with tab5:
        st.markdown("#### Transactions")
        transactions_df = transactions_handler.load_transactions()
        if not transactions_df.empty:
            st.dataframe(transactions_df.head(10), use_container_width=True)
            st.caption(f"Showing 10 of {len(transactions_df)} transactions")
        else:
            st.info("No transactions data available")

# ============================================
# MULTI-SHEET CONFIGURATION (NEW)
# ============================================
def render_multi_sheet_configuration():
    """Configuration panel for 5 Google Sheets"""
    st.markdown("<div class='main-header'>Google Sheets Configuration</div>", unsafe_allow_html=True)
    
    sheet_manager = MultiSheetManager()
    current_config = sheet_manager.get_all_sheets_config()
    
    st.markdown("### Configure Your 5 Google Sheets")
    st.info("""
    **Required Google Sheets Structure:**
    
    1. **Properties_DB** - Property listings
    Columns: unit_id, unit_type, listing_type, area, address, price_total, area_sqm, rooms, bathrooms, etc.
    
    2. **Global_Leads** - Client database
    Columns: client_id, name, phone, assigned_to, status, source, budget, etc.
    
    3. **User_Registry** - User accounts
    Columns: username, password, full_name, role, email, department
    
    4. **Activity_Logs** - System activities
    Columns: timestamp, username, action, details
    
    5. **Transactions** - Financial transactions
    Columns: trans_id, unit_id, client_id, amount, date, agent
    """)
    
    with st.form("multi_sheet_config_form"):
        # Sheet 1: Properties
        st.markdown("#### 🏢 Properties Database")
        prop_url = st.text_input(
            "Properties_DB Google Sheet URL",
            value=next((s.get('url', '') for s in current_config.get('sheets', []) 
                       if s.get('type') == 'properties'), ''),
            help="URL of Google Sheet containing property data"
        )
        
        # Sheet 2: Clients
        st.markdown("#### 👥 Global Leads")
        clients_url = st.text_input(
            "Global_Leads Google Sheet URL",
            value=next((s.get('url', '') for s in current_config.get('sheets', []) 
                       if s.get('type') == 'clients'), ''),
            help="URL of Google Sheet containing client/lead data"
        )
        
        # Sheet 3: Users
        st.markdown("#### 👤 User Registry")
        users_url = st.text_input(
            "User_Registry Google Sheet URL",
            value=next((s.get('url', '') for s in current_config.get('sheets', []) 
                       if s.get('type') == 'users'), ''),
            help="URL of Google Sheet containing user accounts"
        )
        
        # Sheet 4: Activity
        st.markdown("#### 📝 Activity Logs")
        activity_url = st.text_input(
            "Activity_Logs Google Sheet URL",
            value=next((s.get('url', '') for s in current_config.get('sheets', []) 
                       if s.get('type') == 'activity'), ''),
            help="URL of Google Sheet for activity tracking"
        )
        
        # Sheet 5: Transactions
        st.markdown("#### 💰 Transactions")
        transactions_url = st.text_input(
            "Transactions Google Sheet URL",
            value=next((s.get('url', '') for s in current_config.get('sheets', []) 
                       if s.get('type') == 'transactions'), ''),
            help="URL of Google Sheet for transactions"
        )
        
        if st.form_submit_button("💾 Save All Configurations", use_container_width=True):
            sheets_config = []
            
            # Add each configured sheet
            if prop_url:
                sheets_config.append({
                    "type": "properties",
                    "url": prop_url,
                    "label": "Properties Database",
                    "configured_at": datetime.now().isoformat()
                })
            
            if clients_url:
                sheets_config.append({
                    "type": "clients",
                    "url": clients_url,
                    "label": "Global Leads",
                    "configured_at": datetime.now().isoformat()
                })
            
            if users_url:
                sheets_config.append({
                    "type": "users",
                    "url": users_url,
                    "label": "User Registry",
                    "configured_at": datetime.now().isoformat()
                })
            
            if activity_url:
                sheets_config.append({
                    "type": "activity",
                    "url": activity_url,
                    "label": "Activity Logs",
                    "configured_at": datetime.now().isoformat()
                })
            
            if transactions_url:
                sheets_config.append({
                    "type": "transactions",
                    "url": transactions_url,
                    "label": "Transactions",
                    "configured_at": datetime.now().isoformat()
                })
            
            if sheets_config:
                success = sheet_manager.save_sheets_config(sheets_config)
                if success:
                    st.success("✅ All Google Sheets configured successfully!")
                    st.rerun()
                else:
                    st.error("Failed to save configurations")
            else:
                st.warning("Please provide at least one sheet URL")
    
    # Test connections
    st.markdown("---")
    st.markdown("### 🔗 Test Connections")
    
    if st.button("Test All Sheet Connections", use_container_width=True):
        with st.spinner("Testing connections..."):
            results = sheet_manager.test_all_connections()
            
            for result in results:
                col1, col2, col3 = st.columns([2, 1, 3])
                with col1:
                    st.write(f"**{result['type'].title()}**")
                with col2:
                    st.write(result['status'])
                with col3:
                    if result['status'] == "✅ Connected":
                        st.caption(f"{result['records']} records, {len(result['columns'])} columns")
                    else:
                        st.caption(result['error'])

# ============================================
# QUICK SETUP WIZARD (NEW)
# ============================================
def render_quick_setup_wizard():
    """Quick setup wizard for new users"""
    st.markdown("<div class='main-header'>Quick Setup Wizard</div>", unsafe_allow_html=True)
    
    step = st.radio("Setup Step", 
                   ["1. Create Sheets", "2. Share Sheets", "3. Enter URLs", "4. Test & Finish"],
                   horizontal=True)
    
    if step == "1. Create Sheets":
        st.markdown("#### Step 1: Create 5 Google Sheets")
        
        st.markdown("""
        **Required Sheets:**
        
        1. **Properties_DB** - Property listings
        2. **Global_Leads** - Client database  
        3. **User_Registry** - User accounts
        4. **Activity_Logs** - System activities
        5. **Transactions** - Financial transactions
        
        **Quick Actions:**
        """)
        
        if st.button("📋 Copy Template Structure", use_container_width=True):
            templates = """
            Properties_DB columns:
            unit_id, unit_type, listing_type, area, address, price_total, payment_type, monthly_payment, net_area, area_sqm, floor_number, total_floors, rooms, bathrooms, furnished, electricity, water, gas, elevator, garage, date_added, notes
            
            Global_Leads columns:
            client_id, name, phone, assigned_to, status, notes, source, budget
            
            User_Registry columns:
            username, password, full_name, role, email, department
            
            Activity_Logs columns:
            timestamp, username, action, details
            
            Transactions columns:
            trans_id, unit_id, client_id, amount, date, agent
            """
            st.code(templates, language="text")
        
        st.info("💡 Create 5 new Google Sheets or use existing ones with similar structure.")
    
    elif step == "2. Share Sheets":
        st.markdown("#### Step 2: Share Your Sheets")
        
        st.markdown("""
        **For each sheet:**
        1. Open the Google Sheet
        2. Click **Share** button (top-right)
        3. Click **General access**
        4. Select **Anyone with the link**
        5. Set permission to **Viewer**
        6. Click **Copy link**
        7. Paste the link in next step
        """)
    
    elif step == "3. Enter URLs":
        st.markdown("#### Step 3: Enter Sheet URLs")
        
        sheet_manager = MultiSheetManager()
        
        with st.form("quick_setup_form"):
            st.markdown("**Enter your Google Sheet URLs:**")
            
            prop_url = st.text_input("Properties_DB URL", placeholder="https://docs.google.com/spreadsheets/d/...")
            clients_url = st.text_input("Global_Leads URL", placeholder="https://docs.google.com/spreadsheets/d/...")
            users_url = st.text_input("User_Registry URL", placeholder="https://docs.google.com/spreadsheets/d/...")
            activity_url = st.text_input("Activity_Logs URL", placeholder="https://docs.google.com/spreadsheets/d/...")
            transactions_url = st.text_input("Transactions URL", placeholder="https://docs.google.com/spreadsheets/d/...")
            
            if st.form_submit_button("Save & Continue", use_container_width=True):
                sheets_config = []
                
                urls = [prop_url, clients_url, users_url, activity_url, transactions_url]
                types = ["properties", "clients", "users", "activity", "transactions"]
                labels = ["Properties DB", "Global Leads", "User Registry", "Activity Logs", "Transactions"]
                
                for url, sheet_type, label in zip(urls, types, labels):
                    if url:
                        sheets_config.append({
                            "type": sheet_type,
                            "url": url,
                            "label": label,
                            "configured_at": datetime.now().isoformat()
                        })
                
                if sheets_config:
                    success = sheet_manager.save_sheets_config(sheets_config)
                    if success:
                        st.success(f"✅ {len(sheets_config)} sheets configured successfully!")
                        st.session_state.setup_complete = True
                        st.rerun()
    
    elif step == "4. Test & Finish":
        st.markdown("#### Step 4: Test & Finish Setup")
        
        if st.session_state.get('setup_complete', False):
            st.success("🎉 Setup Complete!")
            
            # Test connections
            sheet_manager = MultiSheetManager()
            
            st.markdown("**Testing connections...**")
            results = sheet_manager.test_all_connections()
            
            for result in results:
                if result['status'] == "✅ Connected":
                    st.success(f"✅ {result['type'].title()}: {result['records']} records loaded")
                else:
                    st.warning(f"⚠️ {result['type'].title()}: {result['error']}")
            
            st.markdown("---")
            st.markdown("### 🚀 Ready to Go!")
            
            col_btn1, col_btn2 = st.columns(2)
            with col_btn1:
                if st.button("Go to Dashboard", use_container_width=True):
                    st.session_state.current_page = "multi_sheet_dashboard"
            with col_btn2:
                if st.button("Start Using System", use_container_width=True):
                    st.session_state.current_page = "home"
        
        else:
            st.warning("Please complete Step 3 first.")

# ============================================
# MANAGER CONTROL PANEL (UPDATED)
# ============================================
class ManagerControlPanel:
    """Professional manager control panel"""
    
    def __init__(self):
        self.users = load_users()
    
    def render_panel(_self):
        """Render manager control panel"""
        st.markdown("<div class='main-header'>Management Control Panel</div>", unsafe_allow_html=True)
        
        tab1, tab2, tab3, tab4 = st.tabs(["Staff Management", "Activity Log", "System Configuration", "Google Sheets"])
        
        with tab1:
            _self._render_staff_management()
        
        with tab2:
            _self._render_activity_log()
        
        with tab3:
            _self._render_system_config()
        
        with tab4:
            render_multi_sheet_configuration()
    
    def _render_staff_management(_self):
        """Render staff management interface"""
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("### Current Staff")
            _self._render_staff_table()
        
        with col2:
            st.markdown("### Add New Employee")
            _self._render_add_employee_form()
    
    def _render_staff_table(_self):
        """Render staff table with management options"""
        table_data = []
        for username, user_data in _self.users.items():
            table_data.append({
                "Username": username,
                "Full Name": user_data.get('full_name', ''),
                "Role": user_data.get('role', '').title(),
                "Department": user_data.get('department', ''),
                "Email": user_data.get('email', ''),
            })
        
        if table_data:
            df = pd.DataFrame(table_data)
            st.dataframe(df, use_container_width=True, height=300)
            
            # Delete functionality
            st.markdown("### Remove Employee")
            delete_user = st.selectbox("Select user to remove:", 
                                     list(_self.users.keys()),
                                     key="delete_user_select")
            
            if st.button("Remove Employee", type="secondary", key="remove_employee_btn"):
                if delete_user and delete_user != st.session_state.user['username']:
                    _self._remove_user(delete_user)
                else:
                    st.warning("Cannot remove your own account or system administrator")
        else:
            st.info("No staff records found")
    
    def _remove_user(_self, username):
        """Remove user from system"""
        try:
            if username in _self.users:
                del _self.users[username]
                with open("users.json", 'w', encoding='utf-8') as f:
                    json.dump(_self.users, f, indent=2, ensure_ascii=False)
                
                log_activity(st.session_state.user['username'], "remove_user", f"Removed: {username}")
                st.success(f"User {username} removed successfully")
                st.rerun()
        except Exception as e:
            st.error(f"Error removing user: {str(e)}")
    
    def _render_add_employee_form(_self):
        """Render add employee form"""
        with st.form("add_employee_form"):
            username = st.text_input("Username*", key="new_username")
            full_name = st.text_input("Full Name*", key="new_full_name")
            email = st.text_input("Email*", key="new_email")
            role = st.selectbox("Role*", 
                              ["sales", "data_entry", "data_analyst", "manager"],
                              key="new_role")
            department = st.selectbox("Department", 
                                    ["Sales", "Analytics", "Management", "Operations"],
                                    key="new_department")
            password = st.text_input("Password*", type="password", key="new_password")
            
            submitted = st.form_submit_button("Add Employee", use_container_width=True)
            
            if submitted:
                if not all([username, full_name, email, password]):
                    st.error("All required fields must be completed")
                    return
                
                if username in _self.users:
                    st.error("Username already exists")
                    return
                
                # Create new user
                new_user_id = max([u['id'] for u in _self.users.values()]) + 1
                _self.users[username] = {
                    "id": new_user_id,
                    "role": role,
                    "password": bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8'),
                    "email": email,
                    "full_name": full_name,
                    "department": department,
                    "created": datetime.now().isoformat()
                }
                
                # Save to file
                try:
                    with open("users.json", 'w', encoding='utf-8') as f:
                        json.dump(_self.users, f, indent=2, ensure_ascii=False)
                    
                    log_activity(st.session_state.user['username'], "add_user", 
                               f"Added: {username} as {role}")
                    st.success(f"Employee {full_name} added successfully")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error saving user: {str(e)}")
    
    def _render_activity_log(_self):
        """Render activity log viewer"""
        st.markdown("### System Activity Log")
        
        # Load activity log from Google Sheets
        activity_handler = ActivityLogsHandler()
        activity_df = activity_handler.load_activity_logs(100)
        
        if not activity_df.empty:
            # Display log
            st.dataframe(activity_df, use_container_width=True, height=400)
            
            # Export option
            csv = activity_df.to_csv(index=False)
            st.download_button(
                label="📥 Export Log (CSV)",
                data=csv,
                file_name=f"activity_log_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                use_container_width=True
            )
        else:
            st.info("No activity log entries found")
    
    def _render_system_config(_self):
        """Render system configuration"""
        st.markdown("### System Configuration")
        st.info("Google Sheets configuration is now available in the 'Google Sheets' tab")

# ============================================
# DATA ANALYST DASHBOARD (UPDATED)
# ============================================
class DataAnalystDashboard:
    """Professional data analyst dashboard"""
    
    def __init__(self):
        self.property_db = PropertyDatabase()
        self.client_db = ClientDatabase()
        self.transactions_handler = TransactionsHandler()
    
    def render_dashboard(_self):
        """Render data analyst dashboard"""
        st.markdown("<div class='main-header'>Data Analytics Dashboard</div>", unsafe_allow_html=True)
        
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["Market Overview", "Client Analytics", 
                                                "Property Analytics", "System Data", "Multi-Sheet"])
        
        with tab1:
            _self._render_market_overview()
        
        with tab2:
            _self._render_client_analytics()
        
        with tab3:
            _self._render_property_analytics()
        
        with tab4:
            _self._render_system_data()
        
        with tab5:
            render_multi_sheet_dashboard()
    
    def _render_market_overview(_self):
        """Render market overview"""
        st.markdown("### Market Overview")
        
        property_df = _self.property_db.load_properties()
        client_metrics = _self.client_db.get_client_metrics("analyst", "data_analyst")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
            st.metric("Total Inventory", len(property_df))
            st.markdown("</div>", unsafe_allow_html=True)
        
        with col2:
            st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
            st.metric("Total Clients", client_metrics['total'])
            st.markdown("</div>", unsafe_allow_html=True)
        
        with col3:
            st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
            portfolio_value = _self.property_db.get_inventory_metrics()['total_value']
            st.metric("Portfolio Value", f"${portfolio_value:,.0f}")
            st.markdown("</div>", unsafe_allow_html=True)
        
        with col4:
            st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
            transactions_df = _self.transactions_handler.load_transactions()
            total_transactions = len(transactions_df)
            st.metric("Transactions", total_transactions)
            st.markdown("</div>", unsafe_allow_html=True)
        
        # Market trends
        if not property_df.empty:
            # Price distribution
            price_cols = ['price_total', 'price', 'value']
            price_col = None
            for col in price_cols:
                if col in property_df.columns:
                    price_col = col
                    break
            
            if price_col:
                fig = px.histogram(property_df, x=price_col, 
                                  title="Price Distribution",
                                  labels={price_col: 'Price ($)'})
                st.plotly_chart(fig, use_container_width=True)
    
    def _render_client_analytics(_self):
        """Render client analytics"""
        st.markdown("### Client Analytics")
        
        clients_data = _self.client_db.load_clients("analyst", "data_analyst")
        clients = clients_data.get('clients', [])
        
        if not clients:
            st.info("No client data available")
            return
        
        df = pd.DataFrame(clients)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
            if 'value' in df.columns:
                total_value = df['value'].sum()
                st.metric("Total Client Value", f"${total_value:,.0f}")
            st.markdown("</div>", unsafe_allow_html=True)
        
        with col2:
            st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
            if 'source' in df.columns:
                top_source = df['source'].mode()[0] if not df['source'].mode().empty else "N/A"
                st.metric("Top Source", top_source)
            st.markdown("</div>", unsafe_allow_html=True)
        
        with col3:
            st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
            if 'status' in df.columns:
                active_clients = len(df[df['status'] == 'Active'])
                st.metric("Active Clients", active_clients)
            st.markdown("</div>", unsafe_allow_html=True)
        
        # Source analysis
        if 'source' in df.columns:
            source_data = df['source'].value_counts()
            fig = px.bar(source_data, title="Lead Source Analysis")
            st.plotly_chart(fig, use_container_width=True)
    
    def _render_property_analytics(_self):
        """Render property analytics"""
        st.markdown("### Property Analytics")
        
        df = _self.property_db.load_properties()
        
        if df.empty:
            st.info("No property data available")
            return
        
        metrics = _self.property_db.get_inventory_metrics()
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
            st.metric("Total Units", metrics['total_units'])
            st.markdown("</div>", unsafe_allow_html=True)
        
        with col2:
            st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
            st.metric("Available Units", metrics['available_units'])
            st.markdown("</div>", unsafe_allow_html=True)
        
        with col3:
            st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
            st.metric("Sold Units", metrics['sold_units'])
            st.markdown("</div>", unsafe_allow_html=True)
        
        with col4:
            st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
            st.metric("Average Price", f"${metrics['avg_price']:,.0f}")
            st.markdown("</div>", unsafe_allow_html=True)
        
        # Property type distribution
        type_cols = ['unit_type', 'property_type', 'type']
        type_col = None
        for col in type_cols:
            if col in df.columns:
                type_col = col
                break
        
        if type_col:
            type_counts = df[type_col].value_counts()
            fig = px.pie(values=type_counts.values, names=type_counts.index,
                        title="Property Type Distribution")
            st.plotly_chart(fig, use_container_width=True)
    
    def _render_system_data(_self):
        """Render raw system data"""
        st.markdown("### System Data Tables")
        
        # Property data
        st.markdown("#### Property Inventory")
        df = _self.property_db.load_properties()
        if not df.empty:
            st.dataframe(df, use_container_width=True, height=300)
        else:
            st.info("No property data available")
        
        # Client data
        st.markdown("#### Client Database")
        clients_data = _self.client_db.load_clients("analyst", "data_analyst")
        clients = clients_data.get('clients', [])
        if clients:
            clients_df = pd.DataFrame(clients)
            st.dataframe(clients_df, use_container_width=True, height=300)
        else:
            st.info("No client data available")

# ============================================
# PROFESSIONAL DASHBOARDS (UPDATED)
# ============================================
def render_owner_dashboard():
    """Professional owner dashboard"""
    user = st.session_state.user
    st.markdown(f"<div class='main-header'>Executive Dashboard</div>", unsafe_allow_html=True)
    st.markdown(f"**Welcome, {user['full_name']}** | *Executive Access*")
    
    property_db = PropertyDatabase()
    client_db = ClientDatabase()
    
    # Executive KPIs
    st.markdown("### Executive Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
        property_metrics = property_db.get_inventory_metrics()
        st.metric("Total Inventory", property_metrics['total_units'])
        st.markdown("</div>", unsafe_allow_html=True)
    
    with col2:
        st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
        st.metric("Portfolio Value", f"${property_metrics['total_value']:,.0f}")
        st.markdown("</div>", unsafe_allow_html=True)
    
    with col3:
        st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
        client_metrics = client_db.get_client_metrics(user['username'], user['role'])
        st.metric("Total Clients", client_metrics['total'])
        st.markdown("</div>", unsafe_allow_html=True)
    
    with col4:
        st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
        users = load_users()
        active_users = len([u for u in users.values() if u.get('role') in ['sales', 'manager', 'data_analyst']])
        st.metric("Active Staff", active_users)
        st.markdown("</div>", unsafe_allow_html=True)
    
    # Quick access to new features
    st.markdown("---")
    st.markdown("### 📋 Quick Access")
    
    col5, col6, col7 = st.columns(3)
    
    with col5:
        if st.button("📊 Multi-Sheet Dashboard", use_container_width=True):
            st.session_state.current_page = "multi_sheet_dashboard"
    
    with col6:
        if st.button("⚙️ Google Sheets Config", use_container_width=True):
            st.session_state.current_page = "multi_sheet_config"
    
    with col7:
        if st.button("🚀 Quick Setup", use_container_width=True):
            st.session_state.current_page = "quick_setup"

def render_manager_dashboard():
    """Professional manager dashboard"""
    st.markdown("<div class='main-header'>Management Dashboard</div>", unsafe_allow_html=True)
    
    control_panel = ManagerControlPanel()
    control_panel.render_panel()

def render_sales_dashboard():
    """Professional sales dashboard"""
    user = st.session_state.user
    st.markdown(f"<div class='main-header'>Sales Dashboard</div>", unsafe_allow_html=True)
    st.markdown(f"**Welcome, {user['full_name']}** | *Sales Professional*")
    
    tab1, tab2, tab3, tab4 = st.tabs(["Property Search", "My Clients", "Performance", "Google Sheets"])
    
    with tab1:
        sales_interface = AdvancedSalesInterface()
        sales_interface.render_interface()
    
    with tab2:
        client_db = ClientDatabase()
        clients_data = client_db.load_clients(user['username'], user['role'])
        clients = clients_data.get('clients', [])
        
        metrics = client_db.get_client_metrics(user['username'], user['role'])
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
            st.metric("My Clients", metrics['total'])
            st.markdown("</div>", unsafe_allow_html=True)
        
        with col2:
            st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
            st.metric("Active Clients", metrics['active'])
            st.markdown("</div>", unsafe_allow_html=True)
        
        with col3:
            st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
            st.metric("Total Value", f"${metrics['total_value']:,.0f}")
            st.markdown("</div>", unsafe_allow_html=True)
        
        # Add new client form (same as before)
        st.markdown("### Add New Client")
        with st.form("add_client_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                name = st.text_input("Client Name*", key="client_name")
                phone = st.text_input("Phone", key="client_phone")
                email = st.text_input("Email", key="client_email")
                budget_min = st.number_input("Minimum Budget", value=0.0, key="client_min_budget")
            
            with col2:
                budget_max = st.number_input("Maximum Budget", value=0.0, key="client_max_budget")
                source = st.selectbox("Source", ["Direct", "Referral", "Website", "Social Media", "Other"],
                                    key="client_source")
                status = st.selectbox("Status", ["Lead", "Contacted", "Qualified", "Proposal", "Negotiation"],
                                    key="client_status")
                value = st.number_input("Estimated Value", value=0.0, key="client_value")
            
            notes = st.text_area("Notes", key="client_notes")
            
            if st.form_submit_button("Add Client", use_container_width=True):
                if name:
                    client_data = {
                        "name": name,
                        "phone": phone,
                        "email": email,
                        "budget_min": budget_min,
                        "budget_max": budget_max,
                        "source": source,
                        "status": "Active",
                        "assigned_to": user['username'],
                        "client_status": status,
                        "value": value,
                        "notes": notes
                    }
                    
                    client_id = client_db.add_client(client_data, user['username'])
                    if client_id:
                        st.success(f"Client added successfully! ID: {client_id}")
                        st.rerun()
                else:
                    st.error("Client name is required")
        
        # Client list
        if clients:
            df = pd.DataFrame(clients)
            st.dataframe(df[['name', 'phone', 'email', 'client_status', 'value']], 
                        use_container_width=True, height=300)
        else:
            st.info("No clients assigned to you")
    
    with tab3:
        st.markdown("### My Performance Metrics")
        
        if clients:
            df = pd.DataFrame(clients)
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                pipeline_value = df['value'].sum() if 'value' in df.columns else 0
                st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
                st.metric("Pipeline Value", f"${pipeline_value:,.0f}")
                st.markdown("</div>", unsafe_allow_html=True)
            
            with col2:
                avg_deal_size = df['value'].mean() if 'value' in df.columns else 0
                st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
                st.metric("Average Deal Size", f"${avg_deal_size:,.0f}")
                st.markdown("</div>", unsafe_allow_html=True)
            
            with col3:
                st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
                st.metric("Total Clients", len(clients))
                st.markdown("</div>", unsafe_allow_html=True)
    
    with tab4:
        st.markdown("### Google Sheets Status")
        
        sheet_manager = MultiSheetManager()
        results = sheet_manager.test_all_connections()
        
        for result in results:
            if result['status'] == "✅ Connected":
                st.success(f"✅ {result['type'].title()}: {result['records']} records")
            else:
                st.warning(f"⚠️ {result['type'].title()}: {result['error']}")

def render_data_analyst_dashboard():
    """Professional data analyst dashboard"""
    dashboard = DataAnalystDashboard()
    dashboard.render_dashboard()

def render_data_entry_dashboard():
    """Professional data entry dashboard"""
    st.markdown("<div class='main-header'>Data Management Dashboard</div>", unsafe_allow_html=True)
    
    tab1, tab2, tab3 = st.tabs(["Data Upload", "Data Management", "Google Sheets"])
    
    with tab1:
        st.markdown("### Data Upload Center")
        
        upload_option = st.radio("Select upload method:", 
                                ["Local File", "Google Sheets"], 
                                horizontal=True)
        
        if upload_option == "Local File":
            uploaded_file = st.file_uploader("Choose Excel or CSV file", 
                                           type=['xlsx', 'xls', 'csv'])
            
            if uploaded_file:
                try:
                    if uploaded_file.name.endswith('.csv'):
                        df = pd.read_csv(uploaded_file)
                    else:
                        df = pd.read_excel(uploaded_file)
                    
                    st.success(f"File loaded successfully: {len(df)} records")
                    
                    st.markdown("#### Data Preview")
                    st.dataframe(df.head(), use_container_width=True)
                    
                    if st.button("Save to Database", use_container_width=True):
                        property_db = PropertyDatabase()
                        if property_db.save_properties(df):
                            st.success("Data saved to database")
                            log_activity(st.session_state.user['username'], "upload_data",
                                       f"Uploaded {len(df)} records")
                        else:
                            st.error("Error saving data")
                            
                except Exception as e:
                    st.error(f"Error loading file: {str(e)}")
        
        else:
            st.markdown("### Import from Google Sheets")
            
            sheet_url = st.text_input("Google Sheets URL", key="import_sheet_url")
            if st.button("Import Data", use_container_width=True) and sheet_url:
                try:
                    sheet_id = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', sheet_url).group(1)
                    import_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
                    df = pd.read_excel(import_url)
                    
                    st.success(f"Imported {len(df)} records")
                    
                    st.markdown("#### Data Preview")
                    st.dataframe(df.head(), use_container_width=True)
                    
                    if st.button("Save Imported Data", use_container_width=True):
                        property_db = PropertyDatabase()
                        if property_db.save_properties(df):
                            st.success("Data saved to database")
                        else:
                            st.error("Error saving data")
                except Exception as e:
                    st.error(f"Error importing: {str(e)}")
    
    with tab2:
        st.markdown("### Data Management")
        
        property_db = PropertyDatabase()
        df = property_db.load_properties()
        
        if df.empty:
            st.info("No data available for management")
            return
        
        st.markdown(f"**Current Inventory:** {len(df)} records")
        st.dataframe(df, use_container_width=True, height=400)
        
        buffer = BytesIO()
        df.to_excel(buffer, index=False, engine='openpyxl')
        
        st.download_button(
            label="📥 Export Current Data (Excel)",
            data=buffer.getvalue(),
            file_name=f"property_data_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
    
    with tab3:
        render_multi_sheet_dashboard()

# ============================================
# UPDATED NAVIGATION WITH NEW OPTIONS
# ============================================
def render_navigation():
    """Professional navigation sidebar"""
    with st.sidebar:
        user = st.session_state.user
        
        # User profile
        st.markdown(f"### {user['full_name']}")
        st.markdown(f"*{user['role'].title()}*")
        st.markdown(f"**Department:** {user.get('department', 'General')}")
        
        # Quick metrics
        st.markdown("---")
        st.markdown("### Quick Metrics")
        
        if user['role'] in ['owner', 'manager', 'data_analyst']:
            property_db = PropertyDatabase()
            metrics = property_db.get_inventory_metrics()
            st.metric("Inventory", metrics['total_units'])
        
        if user['role'] in ['owner', 'manager', 'sales', 'data_analyst']:
            client_db = ClientDatabase()
            client_metrics = client_db.get_client_metrics(user['username'], user['role'])
            st.metric("My Clients", client_metrics['total'])
        
        # Multi-Sheet Status
        st.markdown("---")
        st.markdown("### 📋 Multi-Sheet System")
        
        sheet_manager = MultiSheetManager()
        sheets_config = sheet_manager.get_all_sheets_config()
        configured_sheets = len(sheets_config.get("sheets", []))
        
        st.metric("Configured Sheets", f"{configured_sheets}/5")
        
        # Navigation
        st.markdown("---")
        st.markdown("### Navigation")
        
        # Role-based navigation
        if user['role'] == 'owner':
            if st.button("🏢 Executive Dashboard", use_container_width=True):
                st.session_state.current_page = "owner"
            if st.button("📊 Multi-Sheet Dashboard", use_container_width=True):
                st.session_state.current_page = "multi_sheet_dashboard"
            if st.button("⚙️ Sheets Configuration", use_container_width=True):
                st.session_state.current_page = "multi_sheet_config"
        
        elif user['role'] == 'manager':
            if st.button("👨‍💼 Management Panel", use_container_width=True):
                st.session_state.current_page = "manager"
            if st.button("📊 Multi-Sheet Dashboard", use_container_width=True):
                st.session_state.current_page = "multi_sheet_dashboard"
        
        elif user['role'] == 'data_analyst':
            if st.button("📊 Analytics Dashboard", use_container_width=True):
                st.session_state.current_page = "analyst"
            if st.button("📈 Multi-Sheet View", use_container_width=True):
                st.session_state.current_page = "multi_sheet_dashboard"
        
        elif user['role'] == 'sales':
            if st.button("🔍 Property Search", use_container_width=True):
                st.session_state.current_page = "sales_search"
            if st.button("👥 My Clients", use_container_width=True):
                st.session_state.current_page = "sales_clients"
            if st.button("📊 Sheets Status", use_container_width=True):
                st.session_state.current_page = "multi_sheet_dashboard"
        
        elif user['role'] == 'data_entry':
            if st.button("📁 Data Upload", use_container_width=True):
                st.session_state.current_page = "data_upload"
            if st.button("📊 Data Management", use_container_width=True):
                st.session_state.current_page = "data_manage"
            if st.button("📋 Multi-Sheet", use_container_width=True):
                st.session_state.current_page = "multi_sheet_dashboard"
        
        # Universal navigation
        st.markdown("---")
        if st.button("🏠 Home", use_container_width=True):
            st.session_state.current_page = user['role']
        
        # Quick setup for admins
        if user['role'] in ['owner', 'manager']:
            if st.button("🚀 Quick Setup", use_container_width=True):
                st.session_state.current_page = "quick_setup"
        
        # Logout
        if st.button("🚪 Logout", type="primary", use_container_width=True):
            log_activity(user['username'], "logout")
            st.session_state.clear()
            st.rerun()

# ============================================
# PROFESSIONAL LOGIN PAGE
# ============================================
def render_login_page():
    """Professional login page"""
    st.markdown("<div class='main-header'>Real Estate ERP System</div>", unsafe_allow_html=True)
    st.markdown("### Professional Access Portal")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        with st.form("login_form"):
            username = st.text_input("Username", placeholder="Enter your username")
            password = st.text_input("Password", type="password", placeholder="Enter your password")
            
            login_button = st.form_submit_button("Login", use_container_width=True)
        
        if login_button:
            if username and password:
                user = authenticate_user(username, password)
                if user:
                    st.session_state.user = user
                    log_activity(username, "login", "Successful authentication")
                    st.success(f"Welcome back, {user['full_name']}!")
                    st.rerun()
                else:
                    st.error("Invalid credentials. Please try again.")
            else:
                st.error("Username and password are required.")
        
        # Demo access
        st.markdown("---")
        st.markdown("### Demo Access")
        
        demo_users = {
            "Executive": {"user": "admin", "pass": "admin123"},
            "Management": {"user": "manager", "pass": "manager123"},
            "Analytics": {"user": "analyst", "pass": "analyst123"},
            "Sales": {"user": "sales1", "pass": "sales123"}
        }
        
        cols = st.columns(len(demo_users))
        for idx, (role, creds) in enumerate(demo_users.items()):
            with cols[idx]:
                if st.button(role, use_container_width=True, key=f"demo_{role}"):
                    user = authenticate_user(creds['user'], creds['pass'])
                    if user:
                        st.session_state.user = user
                        st.success(f"Demo login as {role}")
                        st.rerun()
        
        # New feature highlight
        st.markdown("---")
        with st.expander("🎯 New Features", expanded=True):
            st.markdown("""
            **5 Google Sheets Integration:**
            - **Properties_DB**: Property listings database
            - **Global_Leads**: Client and lead management  
            - **User_Registry**: User authentication system
            - **Activity_Logs**: System activity tracking
            - **Transactions**: Financial transactions
            
            **Benefits:**
            - Real-time data synchronization
            - Centralized data management
            - Professional reporting
            - Automated backups
            """)

# ============================================
# MAIN APPLICATION
# ============================================
def main():
    """Main application entry point"""
    
    # Initialize session state
    if 'user' not in st.session_state:
        st.session_state.user = None
    if 'current_page' not in st.session_state:
        st.session_state.current_page = None
    
    # Check authentication
    if st.session_state.user is None:
        render_login_page()
    else:
        # Render navigation
        render_navigation()
        
        # Route to appropriate dashboard
        if st.session_state.current_page is None:
            st.session_state.current_page = st.session_state.user['role']
        
        # Page routing with new pages
        current_page = st.session_state.current_page
        
        if current_page == "multi_sheet_dashboard":
            render_multi_sheet_dashboard()
        elif current_page == "multi_sheet_config":
            render_multi_sheet_configuration()
        elif current_page == "quick_setup":
            render_quick_setup_wizard()
        elif current_page == "owner":
            render_owner_dashboard()
        elif current_page == "manager":
            render_manager_dashboard()
        elif current_page == "analyst":
            render_data_analyst_dashboard()
        elif current_page in ["sales_search", "sales_clients", "sales_performance"]:
            render_sales_dashboard()
        elif current_page in ["data_upload", "data_manage"]:
            render_data_entry_dashboard()
        else:
            # Default to role-based dashboard
            user_role = st.session_state.user['role']
            if user_role == 'owner':
                render_owner_dashboard()
            elif user_role == 'manager':
                render_manager_dashboard()
            elif user_role == 'data_analyst':
                render_data_analyst_dashboard()
            elif user_role == 'sales':
                render_sales_dashboard()
            elif user_role == 'data_entry':
                render_data_entry_dashboard()

# ============================================
# ENTRY POINT
# ============================================
if __name__ == "__main__":
    main()
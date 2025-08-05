#!/usr/bin/env python3
"""
Test admin functionality with Owner role
"""

import requests
from bs4 import BeautifulSoup

BASE_URL = "http://127.0.0.1:5000"

def test_admin_functionality():
    """Test admin panel access with Owner role"""
    session = requests.Session()
    
    print("🔍 Testing Admin Functionality...")
    
    # Login as Owner
    print("1. Logging in as Owner...")
    login_page = session.get(f"{BASE_URL}/login")
    soup = BeautifulSoup(login_page.text, 'html.parser')
    csrf_input = soup.find('input', {'name': 'csrf_token'})
    csrf_token = csrf_input.get('value') if csrf_input else None
    
    if not csrf_token:
        print("❌ No CSRF token found")
        return False
        
    login_data = {
        "username": "admin_owner",
        "password": "AdminPass456!",
        "csrf_token": csrf_token
    }
    
    login_response = session.post(f"{BASE_URL}/login", data=login_data)
    
    if login_response.status_code == 200 and "dashboard" in login_response.text.lower():
        print("✅ Owner logged in successfully")
    else:
        print("❌ Owner login failed")
        return False
        
    # Test admin panel access
    print("2. Testing admin panel access...")
    admin_response = session.get(f"{BASE_URL}/admin")
    
    if admin_response.status_code == 200:
        if "admin" in admin_response.text.lower() and "log" in admin_response.text.lower():
            print("✅ Owner can access admin panel")
            
            # Check for admin content
            soup = BeautifulSoup(admin_response.text, 'html.parser')
            tables = soup.find_all('table')
            if tables:
                print(f"✅ Admin panel contains {len(tables)} table(s) (likely activity logs)")
            else:
                print("⚠️  Admin panel accessible but no tables found")
                
            return True
        else:
            print("❌ Admin panel accessible but missing expected content")
            return False
    else:
        print(f"❌ Admin panel access failed (HTTP {admin_response.status_code})")
        return False

if __name__ == "__main__":
    success = test_admin_functionality()
    if success:
        print("\n✅ Admin functionality test PASSED")
    else:
        print("\n❌ Admin functionality test FAILED")
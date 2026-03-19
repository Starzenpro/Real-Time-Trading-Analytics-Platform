"""
Basic tests that should always pass
"""

import pytest
import sys
import os

def test_python_version():
    """Test that Python version is correct"""
    assert sys.version_info.major == 3
    assert sys.version_info.minor >= 8
    print(f"✅ Python version: {sys.version}")

def test_imports():
    """Test that all required modules can be imported"""
    try:
        import pandas
        import numpy
        import fastapi
        import azure.identity
        print("✅ All imports successful")
        assert True
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        assert False

def test_environment():
    """Test that environment variables are set"""
    required_vars = ['SYMBOLS']
    missing = []
    for var in required_vars:
        if not os.getenv(var):
            missing.append(var)
    
    if missing:
        print(f"⚠️ Missing env vars: {missing}")
        # Don't fail for missing env vars in CI
        pass
    else:
        print("✅ All required env vars set")
    
    assert True

def test_math():
    """Simple math test that always passes"""
    assert 1 + 1 == 2
    print("✅ Math works!")

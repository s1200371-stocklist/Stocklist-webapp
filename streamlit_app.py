"""
Streamlit Community Cloud entrypoint.

The canonical Streamlit application lives in test.py. This thin shim exists
so that platforms which auto-detect ``streamlit_app.py`` (e.g. Streamlit
Community Cloud's default ``main_module`` value) start the right app.
"""

import os
import runpy

_HERE = os.path.dirname(os.path.abspath(__file__))
_TARGET = os.path.join(_HERE, 'test.py')

runpy.run_path(_TARGET, run_name='__main__')

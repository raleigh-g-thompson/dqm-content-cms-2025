#!/usr/bin/env python3
"""Legacy entry point.  The CMS69 re-attribution logic now lives in the
generalized ``reattribute_uscore_profiles.py`` (which also covers CMS165 and
CMS135).  Kept as a thin wrapper so existing references (e.g. F-08) keep
working.  Run manually when adding mis-attribution targets::

    python ./scripts/reattribute_uscore_profiles.py
"""

from reattribute_uscore_profiles import main

if __name__ == "__main__":
    raise SystemExit(main())
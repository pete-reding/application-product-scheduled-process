"""
product_normalizer
==================
Daily pipeline that normalizes free-text agricultural product names
captured by sprayer applicator machines into a validated catalog.

Pipeline flow
-------------
1. Extract new records from agmri.agmri.base_feature (CDC watermark)
2. Run the 9-step deterministic matching cascade
3. Write decisions to my_db.product_normalization.*
4. Export review HTML for any unresolved entries
5. Send macOS notification on completion
"""

__version__ = "0.1.0"
__author__ = "Pete Reding"

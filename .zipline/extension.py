# extension.py
# monkey patch to handle 'latest' ingest dir name in Zipline bundles
import pandas as pd
from zipline.data.bundles import core as bundles_core
# --- MONKEY PATCH START ---
# Save the original function just in case
_original_from_bundle_ingest_dirname = bundles_core.from_bundle_ingest_dirname
def patched_from_bundle_ingest_dirname(ingest_dir_name):
    """
    Patched version that handles 'latest' by returning the current time.
    """
    if ingest_dir_name == 'latest':
        # Return a dummy timestamp (now) so Zipline doesn't crash
        return pd.Timestamp.now().normalize()
    # Otherwise, use the original logic (parse standard timestamp strings)
    return _original_from_bundle_ingest_dirname(ingest_dir_name)
# Apply the patch to the Zipline module
bundles_core.from_bundle_ingest_dirname = patched_from_bundle_ingest_dirname
# --- MONKEY PATCH END ---
# sharadar bundle registration
try:
    from zipline.data.bundles import register
    from zipline.finance import metrics
    from sharadar.loaders.ingest_sharadar import from_nasdaqdatalink
    from sharadar.util.metric_daily import default_daily
    register("sharadar", from_nasdaqdatalink(), create_writers=False)
    metrics.register('default_daily', default_daily)
except ImportError:
    pass
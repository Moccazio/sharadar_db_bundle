import sys
import os
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np

# --- PATH FIX: Allow importing from parent directory ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# --- IMPORT FIX: Use the correct script name ---
from ingest_macro_improved import (
    create_macro_equities_df,
    create_macro_prices_df,
    trading_date,
    _append_ohlc,
    _to_prices_df
)

class TestMacroIngestion(unittest.TestCase):

    def setUp(self):
        # Create a dummy trading calendar mock
        self.mock_calendar = MagicMock()
        # Setup specific dates for the mock
        self.start_date = pd.Timestamp("2020-01-01")
        self.end_date = pd.Timestamp("2020-01-05")
        
        # Mock is_session behavior
        self.mock_calendar.is_session.side_effect = lambda x: x.weekday() < 5
        self.mock_calendar.sessions_in_range.return_value = pd.date_range(
            self.start_date, self.end_date, freq='B'
        )
        
    def test_trading_date_logic(self):
        """Test that trading_date handles weekends correctly."""
        # Friday (Valid session)
        friday = pd.Timestamp("2021-01-01")
        self.mock_calendar.is_session.return_value = True
        result = trading_date(friday, self.mock_calendar)
        self.assertEqual(result, friday.normalize())

        # Saturday (Invalid session -> should roll forward)
        saturday = pd.Timestamp("2021-01-02")
        self.mock_calendar.is_session.return_value = False
        monday = pd.Timestamp("2021-01-04")
        self.mock_calendar.next_close.return_value = monday
        
        result = trading_date(saturday, self.mock_calendar)
        self.assertEqual(result, monday.normalize())

    # NOTE: Updates patch target to 'ingest_macro_improved'
    @patch('ingest_macro_improved.last_available_date')
    def test_create_macro_equities_df(self, mock_last_date):
        """Verify the metadata dataframe contains correct SIDs and assets."""
        mock_last_date.return_value = "2023-12-31"
        
        df = create_macro_equities_df()
        
        # Check required columns exist
        expected_cols = ['symbol', 'asset_name', 'start_date', 'end_date', 
                         'first_traded', 'auto_close_date', 'exchange']
        self.assertTrue(all(col in df.columns for col in expected_cols))
        
        # CRITICAL CHECK: Verify the 20 Year Bond SID (10240) exists
        self.assertIn(10240, df.index)
        self.assertEqual(df.loc[10240]['symbol'], 'TR20Y')
        
        # Verify Corporate Bond
        self.assertIn(10400, df.index)
        self.assertEqual(df.loc[10400]['symbol'], 'CBOND')

    # NOTE: Updates patch targets to 'ingest_macro_improved'
    @patch('ingest_macro_improved.pdr.DataReader')
    @patch('ingest_macro_improved.last_available_date')
    def test_create_macro_prices_df_structure_and_mapping(self, mock_last_date, mock_pdr):
        """
        Test that data is fetched, the correct tickers are requested (FIX CHECK),
        and the output shape is correct.
        """
        mock_last_date.return_value = "2020-01-10"
        
        # --- Mock FRED Data Responses ---
        # 1. Treasury Data Mock (The complex one with multiple columns)
        dates = pd.date_range("2020-01-01", "2020-01-10", freq='B')
        mock_tres_data = pd.DataFrame(
            np.random.rand(len(dates), 9), 
            index=dates, 
            columns=['DTB3', 'DTB6', 'DGS1', 'DGS2', 'DGS3', 'DGS5', 'DGS7', 'DGS10', 'DGS20']
        )
        
        # 2. Single Column Mocks (Corp Bond, IndPro, etc.)
        mock_single_col = pd.DataFrame(
            np.random.rand(len(dates), 1), 
            index=dates, 
            columns=['VALUE']
        )

        def side_effect(tickers, source, start, end):
            if isinstance(tickers, list) and 'DTB3' in tickers:
                return mock_tres_data
            return mock_single_col

        mock_pdr.side_effect = side_effect

        # --- Run the function ---
        df_result = create_macro_prices_df("2020-01-01", self.mock_calendar)

        # --- Assertions ---
        
        # 1. Verify the 'Fix': Check if DGS20 and DGS2 were requested in the first call
        call_args = mock_pdr.call_args_list[0]
        requested_tickers = call_args[0][0] 
        
        self.assertIn('DGS2', requested_tickers, "Failed: 'DGS2' ticker missing from request.")
        self.assertIn('DGS20', requested_tickers, "Failed: 'DGS20' ticker missing from request.")
        
        # 2. Check Result Structure
        self.assertIsInstance(df_result.index, pd.MultiIndex)
        self.assertEqual(df_result.index.names, ['date', 'sid'])
        
        unique_sids = df_result.index.get_level_values('sid').unique()
        self.assertIn(10240, unique_sids)
        self.assertIn(10400, unique_sids)

if __name__ == '__main__':
    unittest.main()
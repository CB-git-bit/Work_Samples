import pandas as pd
import unittest
from unittest.mock import patch, mock_open
from pandas.testing import assert_frame_equal

from data_processing import calculate_adj_close_rolling_med

class TestCalculateAdjCloseRollingMed(unittest.TestCase):

    def setUp(self):
        self.df = pd.DataFrame({
            'Symbol': ['AAPL', 'AAPL', 'GOOG', 'GOOG', 'MSFT', 'MSFT'],
            'Date': ['2021-01-01', '2021-01-02', '2021-01-01', '2021-01-02', '2021-01-01', '2021-01-02'],
            'Adj Close': [100.0, 200.0, 300.0, 400.0, 500.0, 600.0],
            'vol_moving_avg': [100.0, 150.0, 300.0, 300.0, 500.0, 500.0]
        })
        self.df['Date'] = pd.to_datetime(self.df['Date'])
        self.df.set_index('Date', inplace=True)
        self.expected_result = pd.DataFrame({
            'Symbol': ['AAPL', 'AAPL', 'GOOG', 'GOOG', 'MSFT', 'MSFT'],
            'Date': ['2021-01-01', '2021-01-02', '2021-01-01', '2021-01-02', '2021-01-01', '2021-01-02'],
            'Adj Close': [100.0, 200.0, 300.0, 400.0, 500.0, 600.0],
            'vol_moving_avg': [100.0, 150.0, 300.0, 300.0, 500.0, 500.0],
            'adj_close_rolling_med': [100.0, 150.0, 300.0, 300.0, 500.0, 500.0]
        })
        self.expected_result['Date'] = pd.to_datetime(self.expected_result['Date'])
        self.expected_result.set_index('Date', inplace=True)

    @patch('pandas.read_parquet')
    @patch('pandas.DataFrame.to_parquet')
    def test_calculate_adj_close_rolling_med(self, mock_to_parquet, mock_read_parquet):
        mock_read_parquet.return_value = self.df
        result = calculate_adj_close_rolling_med()
        pd.testing.assert_frame_equal(result, self.expected_result)
        mock_to_parquet.assert_called_once_with('feature_eng.parquet')

    @patch('pandas.read_parquet')
    @patch('pandas.DataFrame.to_parquet')
    def test_calculate_adj_close_rolling_med_empty_df(self, mock_to_parquet, mock_read_parquet):
        mock_read_parquet.return_value = pd.DataFrame()
        result = calculate_adj_close_rolling_med()
        self.assertIsNone(result)
        mock_to_parquet.assert_not_called()

    @patch('pandas.read_parquet')
    @patch('pandas.DataFrame.to_parquet')
    def test_calculate_adj_close_rolling_med_no_adj_close_column(self, mock_to_parquet, mock_read_parquet):
        mock_read_parquet.return_value = self.df.drop('Adj Close', axis=1)
        result = calculate_adj_close_rolling_med()
        self.assertIsNone(result)
        mock_to_parquet.assert_not_called()

    @patch('pandas.read_parquet')
    @patch('pandas.DataFrame.to_parquet')
    def test_calculate_adj_close_rolling_med_no_symbol_column(self, mock_to_parquet, mock_read_parquet):
        mock_read_parquet.return_value = self.df.drop('Symbol', axis=1)
        result = calculate_adj_close_rolling_med()
        self.assertIsNone(result)
        mock_to_parquet.assert_not_called()

    @patch('pandas.read_parquet')
    @patch('pandas.DataFrame.to_parquet')
    def test_calculate_adj_close_rolling_med_no_date_column(self, mock_to_parquet, mock_read_parquet):
        mock_read_parquet.return_value = self.df.drop('Date', axis=1)
        result = calculate_adj_close_rolling_med()
        self.assertIsNone(result)
        mock_to_parquet.assert_not_called()

if __name__ == '__main__':
    unittest.main()
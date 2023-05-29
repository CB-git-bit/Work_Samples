import pandas as pd
import numpy as np
import unittest
from unittest.mock import patch, mock_open
import joblib
from pandas.testing import assert_frame_equal

from data_processing import calculate_vol_moving_avg

class TestCalculateVolMovingAvg(unittest.TestCase):

    def setUp(self):
        self.df = pd.DataFrame({
            'Symbol': ['AAPL', 'AAPL', 'GOOG', 'GOOG', 'MSFT', 'MSFT'],
            'Date': ['2021-01-01', '2021-01-02', '2021-01-01', '2021-01-02', '2021-01-01', '2021-01-02'],
            'Volume': [100, 200, 300, 400, 500, 600]
        })
        self.df['Date'] = pd.to_datetime(self.df['Date'])
        self.df.set_index('Date', inplace=True)
        self.expected_result = pd.DataFrame({
            'Symbol': ['AAPL', 'AAPL', 'GOOG', 'GOOG', 'MSFT', 'MSFT'],
            'Date': ['2021-01-01', '2021-01-02', '2021-01-01', '2021-01-02', '2021-01-01', '2021-01-02'],
            'Volume': [100, 200, 300, 400, 500, 600],
            'vol_moving_avg': [100.0, 150.0, 300.0, 300.0, 500.0, 500.0]
        })
        self.expected_result['Date'] = pd.to_datetime(self.expected_result['Date'])
        self.expected_result.set_index('Date', inplace=True)

    @patch('pandas.read_parquet')
    @patch('pandas.DataFrame.to_parquet')
    def test_calculate_vol_moving_avg(self, mock_to_parquet, mock_read_parquet):
        mock_read_parquet.return_value = self.df
        result = calculate_vol_moving_avg()
        pd.testing.assert_frame_equal(result, self.expected_result)
        mock_to_parquet.assert_called_once_with('vol_moving_avg.parquet')

    @patch('pandas.read_parquet')
    @patch('pandas.DataFrame.to_parquet')
    def test_calculate_vol_moving_avg_empty_df(self, mock_to_parquet, mock_read_parquet):
        mock_read_parquet.return_value = pd.DataFrame()
        result = calculate_vol_moving_avg()
        self.assertIsNone(result)
        mock_to_parquet.assert_not_called()

    @patch('pandas.read_parquet')
    @patch('pandas.DataFrame.to_parquet')
    def test_calculate_vol_moving_avg_no_volume_column(self, mock_to_parquet, mock_read_parquet):
        mock_read_parquet.return_value = self.df.drop('Volume', axis=1)
        result = calculate_vol_moving_avg()
        self.assertIsNone(result)
        mock_to_parquet.assert_not_called()

    @patch('pandas.read_parquet')
    @patch('pandas.DataFrame.to_parquet')
    def test_calculate_vol_moving_avg_no_symbol_column(self, mock_to_parquet, mock_read_parquet):
        mock_read_parquet.return_value = self.df.drop('Symbol', axis=1)
        result = calculate_vol_moving_avg()
        self.assertIsNone(result)
        mock_to_parquet.assert_not_called()

    @patch('pandas.read_parquet')
    @patch('pandas.DataFrame.to_parquet')
    def test_calculate_vol_moving_avg_no_date_column(self, mock_to_parquet, mock_read_parquet):
        mock_read_parquet.return_value = self.df.drop('Date', axis=1)
        result = calculate_vol_moving_avg()
        self.assertIsNone(result)
        mock_to_parquet.assert_not_called()

if __name__ == '__main__':
    unittest.main()

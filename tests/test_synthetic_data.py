from pathlib import Path

from supply_chain_ai.utils.synthetic_data import generate_synthetic_m5_like_dataset


def test_synthetic_data_files_created(tmp_path: Path) -> None:
    generate_synthetic_m5_like_dataset(tmp_path, num_days=10)
    assert (tmp_path / "sales_train_validation.csv").exists()
    assert (tmp_path / "calendar.csv").exists()
    assert (tmp_path / "sell_prices.csv").exists()

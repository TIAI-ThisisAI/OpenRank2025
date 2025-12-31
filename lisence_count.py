import pandas as pd

def read_excel_sheet(excel_file: str, sheet_name: str) -> pd.DataFrame:
    """
    读取指定 Excel 文件和 Sheet
    """
    try:
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        return df
    except FileNotFoundError:
        raise FileNotFoundError(f"未找到文件 {excel_file}")
    except ValueError:
        raise ValueError(f"未找到名为 '{sheet_name}' 的工作表")

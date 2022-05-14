from pathlib import Path
from typing import List, Union


def get_files_dir(dir_path: Union[str, Path], pattern="*") -> List[Path]:
    """指定フォルダ内に存在するファイルのパスを返す

    Args:
        dir_path (Union[str,Path]): フォルダのパス
        pattern (str, optional): 一致パターン。ワイルドカード。 Defaults to "*".

    Returns:
        List[Path]: _description_
    """
    if type(dir_path) == str:
        dir_path = Path(dir_path)

    return list(dir_path.glob(pattern))


def get_file_exists(file_path: Union[str, Path]) -> bool:
    """指定のパスの存在確認

    Args:
        file_path (Union[str, Path]): 存在確認したいパス

    Returns:
        bool: 存在すればTrue、そうでなければFalse
    """
    if type(file_path) == str:
        file_path = Path(file_path)
    return file_path.exists()


def mkdir(file_path: Union[str, Path]) -> None:
    """指定のパスに必要な中間のフォルダを含めてフォルダ作成する

    Args:
        file_path (Union[str, Path]): 作成したいファイルのパス

    """

    if type(file_path) == str:
        file_path = Path(file_path)
    dir_path = file_path.parent
    dir_path.mkdir(parents=True, exist_ok=True)

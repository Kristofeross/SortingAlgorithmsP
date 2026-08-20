from pathlib import Path
import pandas as pd

from visualization.utils import ensure_directory


def export_table(
    df: pd.DataFrame,
    directory: Path,
    filename: str,
    caption: str,
    label: str,
    column_format: str | None = None,
) -> None:
    ensure_directory(directory)

    csv_path = directory / f"{filename}.csv"
    tex_path = directory / f"{filename}.tex"

    df.to_csv(
        csv_path,
        index=False,
        float_format="%.4f",
    )

    latex = df.to_latex(
        index=False,
        escape=True,
        float_format=lambda value: f"{value:.4f}",
        caption=caption,
        label=label,
        column_format=column_format,
        position="htbp",
    )

    tex_path.write_text(latex, encoding="utf-8")

    print(f"  Zapisano: {csv_path}")
    print(f"  Zapisano: {tex_path}")
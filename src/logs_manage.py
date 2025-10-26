"""Utilidades de logging sencillo con archivos por ejecución."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional


class Logs:
    """Gestiona el registro de eventos en un archivo de log por ejecución."""

    def __init__(
        self,
        log_dir: str | Path = "logs",
        log_file: Optional[str | Path] = None,
        header: Optional[str] = None,
    ) -> None:
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)

        if log_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            self.log_file = self.log_dir / f"log_{timestamp}.txt"
        else:
            self.log_file = Path(log_file)
            if not self.log_file.is_absolute():
                self.log_file = self.log_dir / self.log_file

        if not self.log_file.exists():
            self._write_header(header)

    # ------------------------------------------------------------------
    def _write_header(self, header: Optional[str]) -> None:
        default_header = header or "LOG DE PROCESO - AIRBNB CDMX"
        with open(self.log_file, "w", encoding="utf-8") as fh:
            fh.write("=" * 70 + "\n")
            fh.write(f"{default_header}\n")
            fh.write("=" * 70 + "\n")
            fh.write(f"Fecha de inicio: {datetime.now():%Y-%m-%d %H:%M:%S}\n")
            fh.write("=" * 70 + "\n\n")

    # ------------------------------------------------------------------
    def _write(self, level: str, message: str) -> None:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{timestamp}] [{level.upper()}] {message}\n"
        with open(self.log_file, "a", encoding="utf-8") as fh:
            fh.write(line)

    # ------------------------------------------------------------------
    def info(self, message: str) -> None:
        self._write("INFO", message)

    def warning(self, message: str) -> None:
        self._write("WARNING", message)

    def error(self, message: str) -> None:
        self._write("ERROR", message)

    # ------------------------------------------------------------------
    def nueva_entrada(self, contexto: str) -> None:
        """Inserta un separador con contexto para secciones largas."""
        self.info("-" * 50)
        self.info(contexto)
        self.info("-" * 50)

    # ------------------------------------------------------------------
    @property
    def ruta(self) -> Path:
        return self.log_file

    # ------------------------------------------------------------------
    def crear_sublogger(self, header: Optional[str] = None) -> "Logs":
        """Devuelve un nuevo logger que reutiliza el mismo archivo."""
        return Logs(log_dir=self.log_dir, log_file=self.log_file.name, header=header)

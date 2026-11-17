from sqlalchemy import String, JSON, Double, BigInteger, Integer
from sqlalchemy.orm import Mapped, mapped_column
from src.config.database import Base


class Dataset(Base):
    __tablename__ = 'datasets'
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(160), unique=True)
    source: Mapped[str] = mapped_column(String(200), default='manual.json')
    records: Mapped[list] = mapped_column(JSON)
    checksum: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[float] = mapped_column(Double)
    # pending：分片上传尚未解析完成，对查询与分析不可见；ready：可核对、可分析。
    status: Mapped[str] = mapped_column(String(16), default='ready')
    total_rows: Mapped[int] = mapped_column(BigInteger, default=0)
    upload_id: Mapped[int | None] = mapped_column(Integer, nullable=True, unique=True)

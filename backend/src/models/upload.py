from sqlalchemy import String, JSON, Double, Integer, BigInteger, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column
from src.config.database import Base


class UploadSession(Base):
    """一次超大文件分批接入。分片落盘，指纹通过后才创建隐藏批次。"""
    __tablename__ = 'upload_sessions'
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(160))
    source: Mapped[str] = mapped_column(String(200))
    size: Mapped[int] = mapped_column(BigInteger)
    sha256: Mapped[str] = mapped_column(String(64))
    fingerprint_actual: Mapped[str] = mapped_column(String(64), default='')
    chunk_size: Mapped[int] = mapped_column(Integer)
    total_chunks: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(20), default='uploading')
    parsed_rows: Mapped[int] = mapped_column(BigInteger, default=0)
    parse_offset: Mapped[int] = mapped_column(BigInteger, default=0)
    header: Mapped[list] = mapped_column(JSON, default=list)
    failed_rows: Mapped[list] = mapped_column(JSON, default=list)
    error: Mapped[str] = mapped_column(String(1000), default='')
    dataset_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_by: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[float] = mapped_column(Double)
    updated_at: Mapped[float] = mapped_column(Double)
    parse_token: Mapped[str | None] = mapped_column(String(40), nullable=True)
    parse_token_at: Mapped[float | None] = mapped_column(Double, nullable=True)
    parse_attempts: Mapped[int] = mapped_column(Integer, default=0)


class UploadChunk(Base):
    """已接收分片索引；同位置只保留一份内容。"""
    __tablename__ = 'upload_chunks'
    __table_args__ = (UniqueConstraint('upload_id', 'seq', name='uq_upload_chunks_slot'),)
    id: Mapped[int] = mapped_column(primary_key=True)
    upload_id: Mapped[int] = mapped_column(Integer, index=True)
    seq: Mapped[int] = mapped_column(Integer)
    size: Mapped[int] = mapped_column(BigInteger)
    sha256: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[float] = mapped_column(Double)


class DatasetRow(Base):
    """大文件批次的逐行存储，分页与流式计算都不必整份读入。"""
    __tablename__ = 'dataset_rows'
    __table_args__ = (
        UniqueConstraint('dataset_id', 'seq', name='uq_dataset_rows_seq'),
        Index('ix_dataset_rows_group', 'dataset_id', 'arm', 'shift'),
    )
    id: Mapped[int] = mapped_column(primary_key=True)
    dataset_id: Mapped[int] = mapped_column(Integer, index=True)
    seq: Mapped[int] = mapped_column(Integer)
    run: Mapped[str] = mapped_column(String(191))
    arm: Mapped[str] = mapped_column(String(32))
    shift: Mapped[str] = mapped_column(String(32))
    seconds: Mapped[float] = mapped_column(Double)
    payload: Mapped[dict] = mapped_column(JSON)

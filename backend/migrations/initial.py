from sqlalchemy import inspect, text
from src.config.database import Base, engine
from src import models


DATASET_COLUMNS = {
    'status': "VARCHAR(16) NOT NULL DEFAULT 'ready'",
    'total_rows': "BIGINT NOT NULL DEFAULT 0",
    'upload_id': "INTEGER NULL",
}


def _upgrade_datasets():
    """已有部署的 datasets 表补列；新表由 create_all 直接建好。"""
    inspector = inspect(engine)
    if 'datasets' not in inspector.get_table_names():
        return
    existing = {c['name'] for c in inspector.get_columns('datasets')}
    missing = [(name, ddl) for name, ddl in DATASET_COLUMNS.items() if name not in existing]
    if not missing:
        return
    with engine.begin() as connection:
        for name, ddl in missing:
            connection.execute(text(f'ALTER TABLE datasets ADD COLUMN {name} {ddl}'))
        connection.execute(text("UPDATE datasets SET status='ready' WHERE status IS NULL OR status=''"))
    try:
        with engine.begin() as connection:
            connection.execute(text('CREATE UNIQUE INDEX uq_datasets_upload ON datasets (upload_id)'))
    except Exception:
        pass  # 已有同类索引或数据库不支持，不影响读写路径


def migrate():
    Base.metadata.create_all(engine)
    _upgrade_datasets()


if __name__ == '__main__':
    migrate()

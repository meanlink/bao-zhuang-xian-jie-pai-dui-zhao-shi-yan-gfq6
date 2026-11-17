from fastapi import HTTPException
from sqlalchemy import select
from src.models import Dataset
from src.services.business import metadata
from src.utils.serialization import checksum
import math, time
def create(db, data):
    name=data.name.strip()
    if len(name)<2: raise HTTPException(422, '批次名称去除空格后至少需要两个字符')
    if db.scalar(select(Dataset).where(Dataset.name==name)):
        raise HTTPException(409, '该数据批次名已存在，请修改名称')
    for index,row in enumerate(data.records):
        for c in metadata()['columns']:
            value=row.get(c['key'])
            if value is None or value=='': raise HTTPException(422, f'第 {index+1} 行缺少字段 {c["label"]}')
            if c['type']=='number':
                try:
                    if isinstance(value,bool) or not math.isfinite(float(value)): raise ValueError()
                    row[c['key']]=float(value)
                except (TypeError, ValueError, OverflowError): raise HTTPException(422, f'第 {index+1} 行 {c["label"]} 必须为有限数值')
    d=Dataset(name=name, source=data.source, records=data.records, checksum=checksum(data.records), created_at=time.time(), status='ready', total_rows=len(data.records))
    db.add(d); db.flush()
    return d


def visible(statement):
    """未完成批次对查询和排序不可见；小批导入与已解析批次(status=ready)可见。"""
    return statement.where(Dataset.status == 'ready')


def records_page(db, dataset, page, page_size):
    """统一的分页读取：小批走 JSON，大文件批次按 seq 从行表读取。"""
    if dataset.upload_id:
        from src.services.uploads import row_page
        return row_page(db, dataset.id, page, page_size)
    rows = dataset.records or []
    start = (page - 1) * page_size
    return {'total': len(rows), 'page': page, 'page_size': page_size,
            'rows': rows[start:start + page_size],
            'first_seq': start + 1 if start < len(rows) else None,
            'last_seq': min(start + page_size, len(rows)) or None}

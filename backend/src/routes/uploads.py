import hashlib
import os
import uuid

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select

from src.config.database import get_db
from src.config.settings import ARTIFACT_DIR
from src.middlewares.auth import can_write, current_user
from src.models import Dataset, UploadSession
from src.schemas.dataset import UploadInput
from src.services import uploads
from src.services.audit import record
from src.utils.serialization import dataset_dict

router = APIRouter(prefix='/uploads')
MAX_REQUEST_BYTES = uploads.MAX_CHUNK_SIZE + 64 * 1024


def _upload_or_404(db, upload_id):
    row = db.get(UploadSession, upload_id)
    if not row:
        raise HTTPException(404, '上传单不存在')
    return row


@router.get('')
def listing(db=Depends(get_db), u=Depends(current_user)):
    rows = db.scalars(select(UploadSession).order_by(UploadSession.id.desc()).limit(30))
    return [uploads.state(db, row) for row in rows]


@router.post('', status_code=201)
def create(data: UploadInput, db=Depends(get_db), u=Depends(can_write)):
    row = uploads.create(db, data, u)
    record(db, u.email, 'upload.create',
           {'id': row.id, 'size': row.size, 'chunks': row.total_chunks})
    db.commit()
    return uploads.state(db, row)


@router.get('/{id}')
def detail(id: int, db=Depends(get_db), u=Depends(current_user)):
    row = _upload_or_404(db, id)
    return uploads.state(db, row)


@router.put('/{id}/chunks/{seq}')
async def put_chunk(id: int, seq: int, request: Request, db=Depends(get_db), u=Depends(can_write)):
    """接收原始分片字节；乱序、重复均可，逐片落盘，不整份读入。"""
    row = _upload_or_404(db, id)
    expected = uploads.expected_size(row, seq)
    if expected is None:
        raise HTTPException(422, f'分片序号必须在 0 到 {row.total_chunks - 1} 之间')
    declared = request.headers.get('content-length')
    if declared is not None and int(declared) > MAX_REQUEST_BYTES:
        raise HTTPException(413, '单个分片超过允许的最大体积')
    if row.status != 'uploading':
        # 已结束接收：仅允许完全相同的重复分片（幂等重放），其余拒收
        existing = uploads.chunks_of(db, row.id).get(seq)
        if existing is None:
            raise HTTPException(409, '该上传单已结束分片接收，不能再追加分片')
    workdir = os.path.join(ARTIFACT_DIR, 'uploads', '.incoming')
    os.makedirs(workdir, exist_ok=True)
    tmp = os.path.join(workdir, uuid.uuid4().hex + '.tmp')
    digest = hashlib.sha256()
    size = 0
    try:
        with open(tmp, 'wb') as handle:
            async for piece in request.stream():
                size += len(piece)
                if size > expected:
                    raise HTTPException(413, f'分片 {seq} 大小超出声明的 {expected} 字节')
                digest.update(piece)
                handle.write(piece)
            handle.flush()
            os.fsync(handle.fileno())
        if size != expected:
            raise HTTPException(422, f'分片 {seq} 大小应为 {expected} 字节，收到 {size} 字节')
        header_sha = (request.headers.get('x-chunk-sha256') or '').lower()
        if header_sha and header_sha != digest.hexdigest():
            raise HTTPException(422, '分片内容与声明的 SHA256 不一致')
        chunk, duplicate = uploads.accept_chunk(db, row, seq, tmp, size, digest.hexdigest())
    except HTTPException:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    return {'seq': chunk.seq, 'size': chunk.size, 'sha256': chunk.sha256,
            'duplicate': duplicate, 'received_count': len(uploads.chunks_of(db, row.id)),
            'total_chunks': row.total_chunks}


@router.post('/{id}/complete')
def complete(id: int, db=Depends(get_db), u=Depends(can_write)):
    row = _upload_or_404(db, id)
    try:
        row = uploads.complete(db, row)
    except HTTPException as error:
        if error.status_code == 409 and '稍后刷新' in str(error.detail):
            row = db.get(UploadSession, id)
        else:
            raise
    record(db, u.email, 'upload.complete',
           {'id': row.id, 'dataset': row.dataset_id, 'sha256': row.sha256})
    db.commit()
    if row.status in ('uploaded', 'parsing'):
        # 线程内部原子领取解析权；并发完结时只有一个线程继续
        uploads.start_parse_thread(row.id)
    row = db.get(UploadSession, id)
    return uploads.state(db, row)


@router.post('/{id}/parse')
def parse(id: int, db=Depends(get_db), u=Depends(can_write)):
    row = _upload_or_404(db, id)
    row = uploads.retry_parse(db, row, u)
    record(db, u.email, 'upload.parse', {'id': row.id, 'status': row.status})
    db.commit()
    return uploads.state(db, row)


@router.get('/{id}/rows')
def rows(id: int, page: int = 1, page_size: int = 50, db=Depends(get_db), u=Depends(current_user)):
    row = _upload_or_404(db, id)
    if row.status != 'completed' or not row.dataset_id:
        raise HTTPException(409, '批次尚未解析完成，暂不可核对')
    page = max(1, page)
    page_size = max(1, min(200, page_size))
    data = uploads.row_page(db, row.dataset_id, page, page_size)
    return data


@router.get('/{id}/dataset')
def dataset(id: int, db=Depends(get_db), u=Depends(current_user)):
    row = _upload_or_404(db, id)
    if row.status != 'completed' or not row.dataset_id:
        raise HTTPException(409, '批次尚未解析完成，暂不可核对')
    data = db.get(Dataset, row.dataset_id)
    if not data or data.status != 'ready':
        raise HTTPException(404, '数据批次不存在')
    return dataset_dict(data)

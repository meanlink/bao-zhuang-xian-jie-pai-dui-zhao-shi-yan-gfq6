"""超大文件分批接入：分片接收、原文件指纹校验、流式解析与断点续传。

分片按 seq 落盘；全部到齐并通过原文件 SHA256 指纹后才创建隐藏批次。解析
逐批写入 dataset_rows，并与字节偏移在同一事务里提交，进程中断后从已提交
位置继续。只有完整解析成功的批次才转为 ready，对查询和分析可见。
"""
import hashlib
import math
import os
import threading
import time
import uuid

from fastapi import HTTPException
from sqlalchemy import func, select, update, delete
from sqlalchemy.exc import IntegrityError

from src.config.settings import ARTIFACT_DIR, UPLOAD_MAX_BYTES
from src.models import Dataset, DatasetRow, UploadChunk, UploadSession
from src.services.business import metadata
from src.services.streaming import (
    ChunkStream, CsvScanner, JsonArrayScanner, ParseError, sniff_json,
)

PARSE_BATCH = 500
PARSE_CLAIM_SECONDS = 90
MAX_PARSE_ATTEMPTS = 20
MAX_FAILED_ROWS = 100
DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024
MIN_CHUNK_SIZE = 1024
MAX_CHUNK_SIZE = 64 * 1024 * 1024
MAX_CHUNKS = 200000
MAX_SIZE = UPLOAD_MAX_BYTES

COLUMNS = {c['key']: c for c in metadata()['columns']}


def upload_dir(upload_id):
    return os.path.join(ARTIFACT_DIR, 'uploads', str(upload_id))


def chunk_path(upload_id, seq):
    return os.path.join(upload_dir(upload_id), f'{seq:08d}.part')


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def _is_sha256(value):
    value = (value or '').strip()
    return len(value) == 64 and all(c in '0123456789abcdefABCDEF' for c in value)


def _name_taken(db, name, upload_id=None):
    if db.scalar(select(Dataset).where(Dataset.name == name)):
        return True
    return db.scalar(select(UploadSession).where(
        UploadSession.name == name,
        UploadSession.status.notin_(('failed',)),
        UploadSession.id != (upload_id or -1),
    )) is not None


def create(db, data, user):
    name = data.name.strip()
    if len(name) < 2:
        raise HTTPException(422, '批次名称去除空格后至少需要两个字符')
    if data.size <= 0 or data.size > MAX_SIZE:
        raise HTTPException(422, '文件为空或超过支持的最大体积')
    if data.sha256 and not _is_sha256(data.sha256):
        raise HTTPException(422, '原文件指纹必须是 64 位 SHA256 十六进制')
    chunk_size = data.chunk_size or DEFAULT_CHUNK_SIZE
    if not (MIN_CHUNK_SIZE <= chunk_size <= MAX_CHUNK_SIZE):
        raise HTTPException(422, '分片大小须在 1KB 到 64MB 之间')
    total = math.ceil(data.size / chunk_size)
    if total > MAX_CHUNKS:
        raise HTTPException(422, '分片数量过多，请增大分片大小')
    if _name_taken(db, name):
        raise HTTPException(409, '该数据批次名已存在，请修改名称')
    now = time.time()
    row = UploadSession(
        name=name, source=(data.source or 'upload.bin')[:200], size=data.size,
        sha256=(data.sha256 or '').lower(), chunk_size=chunk_size, total_chunks=total,
        status='uploading', created_by=user.id, created_at=now, updated_at=now,
        parse_offset=0, parsed_rows=0, header=[], failed_rows=[],
    )
    db.add(row)
    db.flush()
    return row


def chunks_of(db, upload_id):
    return {c.seq: c for c in db.scalars(select(UploadChunk).where(UploadChunk.upload_id == upload_id))}


def chunks_of_raw(upload_id):
    from src.config.database import Session
    with Session() as check:
        return chunks_of(check, upload_id)


def _ranges(seqs):
    """把分片序号压缩成 [start, end] 闭区间，避免超大列表撑爆响应。"""
    ranges = []
    for seq in seqs:
        if ranges and seq == ranges[-1][1] + 1:
            ranges[-1][1] = seq
        else:
            ranges.append([seq, seq])
    return ranges


def _expand(ranges):
    out = []
    for start, end in ranges:
        out.extend(range(start, end + 1))
    return out


def state(db, upload, detail=False):
    chunks = sorted(chunks_of(db, upload.id))
    ranges = _ranges(chunks)
    missing = []
    seq = 0
    for start, end in ranges:
        while seq < start:
            missing.append(seq)
            seq += 1
        seq = end + 1
    while seq < upload.total_chunks:
        missing.append(seq)
        seq += 1
    dataset = db.get(Dataset, upload.dataset_id) if upload.dataset_id else None
    payload = {
        'id': upload.id, 'name': upload.name, 'source': upload.source, 'size': upload.size,
        'sha256': upload.sha256, 'fingerprint_actual': upload.fingerprint_actual,
        'chunk_size': upload.chunk_size, 'total_chunks': upload.total_chunks,
        'received_ranges': ranges, 'received_count': len(chunks),
        'missing_ranges': _ranges(missing[:5000]),
        'missing_chunks': missing[:500], 'missing_count': len(missing),
        'status': upload.status,
        'parsed_rows': upload.parsed_rows, 'parse_offset': upload.parse_offset,
        'parse_progress': round(upload.parse_offset / upload.size, 4) if upload.size else 0,
        'failed_rows': upload.failed_rows or [], 'error': upload.error,
        'dataset_id': upload.dataset_id, 'dataset_status': dataset.status if dataset else None,
        'created_at': upload.created_at, 'updated_at': upload.updated_at,
    }
    if detail or upload.total_chunks <= 2000:
        payload['received_chunks'] = chunks
    return payload


def expected_size(upload, seq):
    if seq < 0 or seq >= upload.total_chunks:
        return None
    return min(upload.chunk_size, upload.size - seq * upload.chunk_size)


def accept_chunk(db, upload, seq, tmp_path, size, digest):
    """把已落盘的临时分片登记为正式分片。

    先写分片索引（数据库唯一约束裁决胜者），再落盘，因此同位置异内容永远不会
    覆盖已接收内容；重复上传同内容幂等返回。
    """
    expected = expected_size(upload, seq)
    if expected is None:
        raise HTTPException(422, f'分片序号必须在 0 到 {upload.total_chunks - 1} 之间')
    if size != expected:
        raise HTTPException(422, f'分片 {seq} 大小应为 {expected} 字节，收到 {size} 字节')

    def settle(existing):
        path = chunk_path(upload.id, seq)
        if existing.sha256 != digest or existing.size != size:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise HTTPException(409, f'分片 {seq} 已接收不同内容；同一位置不允许覆盖')
        if not os.path.exists(path) or os.path.getsize(path) != existing.size:
            os.makedirs(upload_dir(upload.id), exist_ok=True)
            os.replace(tmp_path, path)
        elif os.path.exists(tmp_path):
            os.remove(tmp_path)
        return existing

    existing = chunks_of(db, upload.id).get(seq)
    if existing:
        return settle(existing), True
    try:
        db.add(UploadChunk(upload_id=upload.id, seq=seq, size=size,
                           sha256=digest, created_at=time.time()))
        db.execute(update(UploadSession).where(UploadSession.id == upload.id)
                   .values(updated_at=time.time()))
        db.commit()
    except IntegrityError:
        db.rollback()
        winner = chunks_of(db, upload.id).get(seq)
        if winner is None:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise HTTPException(409, f'分片 {seq} 登记冲突，请重试')
        return settle(winner), True
    except Exception:
        db.rollback()
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise
    os.makedirs(upload_dir(upload.id), exist_ok=True)
    try:
        os.replace(tmp_path, chunk_path(upload.id, seq))
    except Exception:
        # 索引已登记但文件未落盘：删除索引，允许重传同一分片
        db.execute(delete(UploadChunk).where(
            UploadChunk.upload_id == upload.id, UploadChunk.seq == seq))
        db.commit()
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise
    return db.scalar(select(UploadChunk).where(
        UploadChunk.upload_id == upload.id, UploadChunk.seq == seq)), False


def _ordered_paths(upload_id, total):
    paths = [chunk_path(upload_id, seq) for seq in range(total)]
    for path in paths:
        if not os.path.exists(path):
            raise HTTPException(409, '仍有缺失分片，不能完成上传')
        if os.path.getsize(path) == 0:
            raise HTTPException(409, '存在空分片，请重新上传该分片')
    return paths


def verify_and_fingerprint(upload):
    """一次顺序读取完成三件事：分片大小核对、分片内容与接收记录比对、
    整份文件 SHA256 指纹。内存固定为一个读取块，不随文件总量增长。"""
    paths = _ordered_paths(upload.id, upload.total_chunks)
    records = chunks_of_raw(upload.id)
    whole = hashlib.sha256()
    total_size = 0
    for seq, path in enumerate(paths):
        size = os.path.getsize(path)
        expected = expected_size(upload, seq)
        if size != expected:
            raise HTTPException(409, f'分片 {seq} 大小不符，请重新上传该分片')
        record = records.get(seq)
        if record is None:
            raise HTTPException(409, f'分片 {seq} 未登记，请重新上传该分片')
        piece = hashlib.sha256()
        with open(path, 'rb') as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b''):
                piece.update(block)
                whole.update(block)
        if piece.hexdigest() != record.sha256:
            raise HTTPException(409, f'分片 {seq} 内容与接收记录不一致，请重新上传')
        total_size += size
    return whole.hexdigest(), total_size


def complete(db, upload):
    """校验齐全并核对原文件指纹，然后建立唯一隐藏批次，交由解析线程处理。

    先做只读校验，再用 status='uploading' 的条件更新终结上传单：并发完结时
    只有一个调用能完成状态转换，其余调用看到已转换的状态直接返回，绝不覆盖
    解析中/已完成状态，也不会另建批次。
    """
    upload_id = upload.id
    row = db.get(UploadSession, upload_id)
    if row is None:
        raise HTTPException(404, '上传单不存在')
    if row.status in ('uploaded', 'parsing', 'completed'):
        return row
    if row.status != 'uploading':
        raise HTTPException(409, row.error or '该上传单已失败，请重新上传完整文件')
    actual, total_size = verify_and_fingerprint(row)
    if total_size != row.size:
        _mark_upload_failed(db, row, f'分片总大小为 {total_size} 字节，与声明的 {row.size} 字节不一致', actual)
        raise HTTPException(422, row.error)
    if row.sha256 and actual != row.sha256:
        _mark_upload_failed(db, row, '原文件指纹校验失败：拼接后的内容与上传声明的 SHA256 不一致，整批拒绝解析', actual)
        raise HTTPException(422, row.error)

    dataset = db.scalar(select(Dataset).where(Dataset.upload_id == row.id))
    if dataset is None:
        clash = db.scalar(select(Dataset).where(Dataset.name == row.name))
        if clash:
            _mark_upload_failed(db, row, '同名数据批次已存在，无法生成新批次', actual)
            raise HTTPException(409, row.error)
        dataset = Dataset(
            name=row.name, source=row.source, records=[], checksum=actual,
            created_at=time.time(), status='pending', total_rows=0, upload_id=row.id,
        )
        db.add(dataset)
        try:
            db.flush()
        except IntegrityError:
            # 并发调用已经建立了同一上传单的批次：回滚本次插入，复用胜者
            db.rollback()
            dataset = db.scalar(select(Dataset).where(Dataset.upload_id == upload_id))
            if dataset is None:
                row = db.get(UploadSession, upload_id)
                if row is not None and row.status in ('uploaded', 'parsing', 'completed'):
                    return row
                raise HTTPException(409, '该上传单正在生成批次，请稍后刷新')
            row = db.get(UploadSession, upload_id)

    result = db.execute(
        update(UploadSession)
        .where(UploadSession.id == upload_id, UploadSession.status == 'uploading')
        .values(
            dataset_id=dataset.id, status='uploaded', error='',
            sha256=actual, fingerprint_actual=actual, updated_at=time.time(),
        )
    )
    if result.rowcount != 1:
        # 另一个并发调用已经完成了状态转换；本次创建的批次随事务回滚
        db.rollback()
        row = db.get(UploadSession, upload_id)
        if row is None:
            raise HTTPException(404, '上传单不存在')
        if row.status == 'failed':
            raise HTTPException(409, row.error or '该上传单已失败')
        return row
    db.commit()
    row = db.get(UploadSession, upload_id)
    if row.dataset_id != dataset.id:
        # 极端情况下（数据库不支持条件更新）以最终写入的批次为准
        row = db.get(UploadSession, upload_id)
    return row


def _mark_upload_failed(db, row, message, actual=''):
    row.fingerprint_actual = actual
    row.status = 'failed'
    row.error = message[:990]
    row.updated_at = time.time()
    db.commit()


def _claim(db, upload_id):
    """原子领取解析权；并发调用只允许一个成功，超时后可被恢复进程接管。"""
    token = uuid.uuid4().hex
    now = time.time()
    result = db.execute(
        update(UploadSession)
        .where(
            UploadSession.id == upload_id,
            UploadSession.status.in_(('uploaded', 'parsing')),
            UploadSession.parse_token.is_(None),
        )
        .values(status='parsing', parse_token=token, parse_token_at=now,
                updated_at=now, parse_attempts=UploadSession.parse_attempts + 1)
    )
    db.commit()
    return token if result.rowcount == 1 else None


def _reclaim(db, upload_id):
    """接替已失联的解析线程（原令牌超时），同样只有一个调用成功。"""
    token = uuid.uuid4().hex
    now = time.time()
    result = db.execute(
        update(UploadSession)
        .where(
            UploadSession.id == upload_id,
            UploadSession.status == 'parsing',
            UploadSession.parse_token.is_not(None),
            UploadSession.parse_token_at < now - PARSE_CLAIM_SECONDS,
        )
        .values(parse_token=token, parse_token_at=now, updated_at=now,
                parse_attempts=UploadSession.parse_attempts + 1)
    )
    db.commit()
    return token if result.rowcount == 1 else None


def acquire(db, upload_id, allow_stale=False):
    if allow_stale:
        return _claim(db, upload_id) or _reclaim(db, upload_id)
    return _claim(db, upload_id)


def _touch(db, upload_id, token, values):
    """带令牌的检查点更新；令牌失效时回滚整批写入，避免并发双写。"""
    values = dict(values)
    values.update(parse_token_at=time.time(), updated_at=time.time())
    result = db.execute(
        update(UploadSession)
        .where(
            UploadSession.id == upload_id,
            UploadSession.parse_token == token,
            UploadSession.status == 'parsing',
        )
        .values(**values)
    )
    return result.rowcount == 1


def _fail(db, upload_id, token, message, failed_rows=None):
    """整批拒绝：删除暂存行与隐藏批次，上传单记录失败原因和失败行。"""
    db.rollback()
    row = db.scalar(select(UploadSession).where(UploadSession.id == upload_id).with_for_update())
    if row is None or (token and row.parse_token not in (None, token)):
        return
    row.status = 'failed'
    row.parse_token = None
    row.parse_token_at = None
    row.error = message[:990]
    row.failed_rows = (failed_rows or [])[:MAX_FAILED_ROWS]
    # 整批未入库，进度归零；失败行编号保留用于核对
    row.parsed_rows = 0
    row.parse_offset = 0
    row.updated_at = time.time()
    if row.dataset_id:
        dataset = db.get(Dataset, row.dataset_id)
        if dataset and dataset.status != 'ready':
            db.execute(delete(DatasetRow).where(DatasetRow.dataset_id == dataset.id))
            db.delete(dataset)
            row.dataset_id = None
    db.commit()


def _normalise(record, header):
    """校验一条记录；CSV 字段按 UTF-8 原文解码，JSON 保留原值。"""
    if header is not None:
        if len(record) != len(header):
            raise ValueError(f'列数不一致：表头 {len(header)} 列，本行 {len(record)} 列')
        missing = [key for key in COLUMNS if key not in header]
        if missing:
            raise ValueError('缺少字段 ' + '、'.join(COLUMNS[key]['label'] for key in missing))
        values = {}
        for index, key in enumerate(header):
            raw = record[index]
            try:
                values[key] = raw.decode('utf-8', 'strict') if isinstance(raw, bytes) else str(raw)
            except UnicodeDecodeError:
                raise ValueError(f'字段 {key} 不是合法 UTF-8')
        payload = dict(values)
    else:
        values = record
        payload = dict(record)
    output = {}
    for key, column in COLUMNS.items():
        value = values.get(key)
        if value is None or value == '':
            raise ValueError(f'缺少字段 {column["label"]}')
        output[key] = value
    run = str(output['run']).strip()
    arm = str(output['arm']).strip()
    shift = str(output['shift']).strip()
    if not run:
        raise ValueError('片段号不能为空')
    if arm not in ('A', 'B'):
        raise ValueError('实验组只能为 A 或 B')
    if not shift:
        raise ValueError('班次不能为空')
    try:
        seconds = float(output['seconds'])
    except (TypeError, ValueError):
        raise ValueError('单件节拍必须是数值')
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError('单件节拍必须为正有限数')
    payload['run'], payload['arm'], payload['shift'], payload['seconds'] = run, arm, shift, seconds
    return payload, run, arm, shift, seconds


def compact_payload(payload):
    """标准四列的行不再重复存整行 JSON；额外业务列原样保留。"""
    if set(payload) == set(COLUMNS):
        return {}
    return payload


def expand_payload(row):
    if row.payload:
        return row.payload
    return {'run': row.run, 'arm': row.arm, 'shift': row.shift, 'seconds': row.seconds}


def _flush(db, upload_id, token, dataset_id, start_seq, pending, next_offset, header):
    """一个事务内写入一批行并推进检查点；失败则整批拒绝。"""
    runs = [payload['run'] for payload, _ in pending]
    seen = set()
    duplicate = None
    for index, run in enumerate(runs):
        if run in seen:
            duplicate = (index, run)
            break
        seen.add(run)
    if duplicate is None:
        existing = db.scalar(
            select(DatasetRow.run).where(
                DatasetRow.dataset_id == dataset_id,
                DatasetRow.run.in_(runs),
            ).limit(1)
        )
        if existing is not None:
            duplicate = (runs.index(existing), existing)
    if duplicate is not None:
        index, run = duplicate
        _fail(db, upload_id, token,
              f'第 {start_seq + index + 1} 条记录校验失败，整批拒绝入库',
              [{'row': start_seq + index + 1,
                'reason': f'片段号 {run} 与已入库记录重复'}])
        return False
    values = [{
        'dataset_id': dataset_id, 'seq': start_seq + index + 1,
        'run': payload['run'], 'arm': payload['arm'], 'shift': payload['shift'],
        'seconds': payload['seconds'], 'payload': compact_payload(payload),
    } for index, (payload, _) in enumerate(pending)]
    try:
        db.execute(DatasetRow.__table__.insert(), values)
    except IntegrityError:
        db.rollback()
        _fail(db, upload_id, token,
              f'第 {start_seq + 1} 至 {start_seq + len(pending)} 条记录写入冲突，整批拒绝入库',
              [{'row': start_seq + 1, 'reason': '片段号重复或数据冲突'}])
        return False
    if not _touch(db, upload_id, token, {
        'parsed_rows': start_seq + len(pending),
        'parse_offset': next_offset,
        'header': header if header is not None else UploadSession.header,
    }):
        db.rollback()
        return False
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        _fail(db, upload_id, token, '解析写入冲突，整批拒绝入库',
              [{'row': start_seq + 1, 'reason': '片段号重复'}])
        return False
    return True


def _finalize(db, upload_id, token):
    """当前批次唯一收尾：行数落库、批次转 ready，检查点与状态同事务提交。"""
    row = db.scalar(select(UploadSession).where(UploadSession.id == upload_id).with_for_update())
    if row is None or row.parse_token != token or row.status != 'parsing':
        db.rollback()
        return False
    if row.parsed_rows <= 0:
        db.rollback()
        _fail(db, upload_id, token, '文件中没有可用数据记录，整批拒绝入库', [])
        return False
    dataset = db.get(Dataset, row.dataset_id) if row.dataset_id else None
    if dataset is None or dataset.status != 'pending':
        db.rollback()
        _fail(db, upload_id, token, '上传对应的隐藏批次不可用，无法完成解析', [])
        return False
    now = time.time()
    result = db.execute(
        update(UploadSession)
        .where(
            UploadSession.id == upload_id,
            UploadSession.parse_token == token,
            UploadSession.status == 'parsing',
        )
        .values(status='completed', parse_token=None, parse_token_at=None,
                updated_at=now, error='', parse_offset=row.size)
    )
    if result.rowcount != 1:
        db.rollback()
        return False
    dataset.total_rows = row.parsed_rows
    dataset.checksum = row.sha256
    dataset.records = []
    dataset.status = 'ready'
    db.commit()
    return True


def run_parse(upload_id, token=None):
    """解析入口。接口线程或 worker 都可调用；自身负责领取、续传与收尾。"""
    from src.config.database import Session

    claim_db = None
    if token is None:
        from src.config.database import Session as ClaimSession
        claim_db = ClaimSession()
        try:
            token = acquire(claim_db, upload_id, allow_stale=True)
        finally:
            claim_db.close()
        if token is None:
            return False
    from src.config.database import Session
    with Session() as db:
        try:
            return _parse(db, upload_id, token)
        except ParseError as error:
            _fail(db, upload_id, token, str(error), error.failed)
            return False
        except Exception as error:
            # 可恢复中断：保留已提交位置与隐藏批次，释放解析权等待续传。
            # 连续多次中断仍无法推进时才判定失败，避免无限重试。
            db.rollback()
            try:
                row = db.get(UploadSession, upload_id)
                if row is not None and row.parse_token == token and row.status == 'parsing':
                    if (row.parse_attempts or 0) >= MAX_PARSE_ATTEMPTS:
                        _fail(db, upload_id, token, f'解析连续中断，已保留位置但无法继续：{error}', [])
                    else:
                        row.parse_token = None
                        row.parse_token_at = None
                        row.error = f'解析中断，已保留位置，稍后自动续传：{error}'[:990]
                        row.updated_at = time.time()
                        db.commit()
            except Exception:
                db.rollback()
            raise


def _parse(db, upload_id, token):
    upload = db.get(UploadSession, upload_id)
    if upload is None or upload.status != 'parsing' or upload.parse_token != token:
        return False
    dataset = db.get(Dataset, upload.dataset_id) if upload.dataset_id else None
    if dataset is None:
        _fail(db, upload_id, token, '上传对应的隐藏批次不存在', [])
        return False
    if dataset.status == 'ready':
        return _finalize(db, upload_id, token)
    if dataset.status != 'pending':
        _fail(db, upload_id, token, '批次已处于失败状态，无法继续解析', [])
        return False

    paths = _ordered_paths(upload.id, upload.total_chunks)
    parsed_rows = upload.parsed_rows or 0
    offset = upload.parse_offset or 0
    header = upload.header or None
    with ChunkStream(paths) as stream:
        is_json = sniff_json(stream)
        if is_json:
            scanner = JsonArrayScanner(stream, start=offset)
            scanner.ordinal = parsed_rows
            records = scanner.records()
            record_header = None
        else:
            scanner = CsvScanner(stream, start=offset, header=header)
            scanner.ordinal = parsed_rows
            records = scanner.records()
            record_header = scanner.header
        pending = []
        next_offset = offset
        while True:
            try:
                item = next(records)
            except StopIteration:
                break
            except ParseError as error:
                failed = error.failed or [{
                    'row': parsed_rows + len(pending) + 1, 'reason': str(error)}]
                _fail(db, upload_id, token, str(error), failed)
                return False
            if item[0] == 'header':
                record_header = item[2]
                next_offset = item[1]
                continue
            if is_json:
                ordinal, end_offset, record = item
            else:
                ordinal, end_offset, record = item
                record_header = scanner.header
            try:
                payload, run, arm, shift, seconds = _normalise(record, record_header)
            except ValueError as error:
                # 任意坏行都整批拒绝；末行坏数据同样不会入库
                _fail(db, upload_id, token,
                      f'第 {ordinal} 条记录校验失败，整批拒绝入库',
                      [{'row': ordinal, 'reason': str(error)}])
                return False
            pending.append(({'run': run, 'arm': arm, 'shift': shift,
                             'seconds': seconds}, end_offset))
            next_offset = end_offset
            if len(pending) >= PARSE_BATCH:
                if not _flush(db, upload_id, token, dataset.id, parsed_rows,
                              pending, next_offset, None if is_json else scanner.header):
                    return False
                parsed_rows += len(pending)
                pending = []
        if pending:
            if not _flush(db, upload_id, token, dataset.id, parsed_rows,
                          pending, next_offset, None if is_json else scanner.header):
                return False
    return _finalize(db, upload_id, token)


def start_parse_thread(upload_id, token=None):
    """后台解析线程；线程内部先原子领取解析权，重复触发不会并行解析。"""
    thread = threading.Thread(target=_thread_parse, args=(upload_id, token), daemon=True)
    thread.start()
    return thread


def _thread_parse(upload_id, token=None):
    import logging
    try:
        run_parse(upload_id, token)
    except Exception:
        logging.exception('Upload parse stopped; committed position kept for retry')


def resume_candidates(db, limit=3):
    """可恢复解析的上传单：已校验未开始，或解析令牌超时（线程已中断）。"""
    now = time.time()
    query = select(UploadSession.id).where(
        UploadSession.status.in_(('uploaded', 'parsing')),
        (UploadSession.parse_token.is_(None))
        | (
            UploadSession.parse_token.is_not(None)
            & (UploadSession.parse_token_at < now - PARSE_CLAIM_SECONDS)
        ),
    )
    return list(db.scalars(query.order_by(UploadSession.id).limit(limit)))


def cleanup_finished(db, keep_seconds=7 * 86400):
    """已完成上传单的分片可回收；记录与最终批次保留以便核对。"""
    cutoff = time.time() - keep_seconds
    for row in db.scalars(select(UploadSession).where(
            UploadSession.status == 'completed', UploadSession.updated_at < cutoff)).all():
        cleanup_dir(row.id)


def retry_parse(db, upload, user):
    if upload.status == 'completed':
        return upload
    if upload.status != 'uploaded' and upload.status != 'parsing':
        raise HTTPException(409, upload.error or '该上传单当前不可解析')
    # 线程内部原子领取解析权：已在解析时不会重复执行
    start_parse_thread(upload.id)
    return db.get(UploadSession, upload.id)


def row_page(db, dataset_id, page, page_size):
    total = db.scalar(select(func.count()).select_from(DatasetRow)
                      .where(DatasetRow.dataset_id == dataset_id)) or 0
    rows = list(db.scalars(
        select(DatasetRow).where(DatasetRow.dataset_id == dataset_id)
        .order_by(DatasetRow.seq).offset((page - 1) * page_size).limit(page_size)
    ))
    return {
        'total': total, 'page': page, 'page_size': page_size,
        'rows': [expand_payload(row) for row in rows],
        'first_seq': rows[0].seq if rows else None,
        'last_seq': rows[-1].seq if rows else None,
    }


def iter_row_batches(db, dataset_id, batch=2000):
    """按 seq 键集分页读取，供流式统计使用。

    只取统计需要的三列并逐批丢弃，不实例化 ORM 对象，内存上限为一批的
    元组列表，与总行数无关。
    """
    last = 0
    while True:
        rows = db.execute(
            select(DatasetRow.seq, DatasetRow.arm, DatasetRow.shift, DatasetRow.seconds)
            .where(DatasetRow.dataset_id == dataset_id, DatasetRow.seq > last)
            .order_by(DatasetRow.seq).limit(batch)
        ).all()
        if not rows:
            return
        last = rows[-1][0]
        yield [(arm, shift, seconds) for _seq, arm, shift, seconds in rows]
        del rows

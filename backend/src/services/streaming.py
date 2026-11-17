"""超大文件接入的流式读取。

CSV 按字节解析：UTF-8 多字节序列与引号内换行都可以安全跨分片，字段值按原
字节解码后保持原值。JSON 用增量文本解码 + raw_decode 逐条读取。任何时刻
只保留当前记录与一个读取缓冲，内存不随文件总量增长。
"""
import codecs
import json
import os
import re


class ParseError(Exception):
    """文件内容不合法：整批拒绝入库，并由接入单记录失败行。"""

    def __init__(self, message, failed=None):
        super().__init__(message)
        self.failed = failed or []


class ChunkStream:
    """按 seq 顺序把分片文件串成一个只读流，偏移以整个文件计。"""

    def __init__(self, paths):
        self.paths = list(paths)
        self._sizes = [os.path.getsize(p) for p in self.paths]
        self._starts = []
        total = 0
        for size in self._sizes:
            self._starts.append(total)
            total += size
        self.size = total
        self._index = 0
        self._file = None
        self._pos = 0

    def tell(self):
        return self._pos

    def close(self):
        if self._file:
            self._file.close()
            self._file = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def seek(self, offset):
        if offset < 0 or offset > self.size:
            raise ValueError('分片流偏移越界')
        self.close()
        index = 0
        while index < len(self._sizes) and offset >= self._starts[index] + self._sizes[index]:
            index += 1
        self._index = index
        self._pos = offset
        if index < len(self._sizes):
            self._file = open(self.paths[index], 'rb')
            self._file.seek(offset - self._starts[index])

    def read(self, size=262144):
        parts = []
        while size > 0 and self._index < len(self._sizes):
            if self._file is None:
                self._file = open(self.paths[self._index], 'rb')
            data = self._file.read(size)
            if not data:
                self.close()
                self._index += 1
                continue
            parts.append(data)
            size -= len(data)
            self._pos += len(data)
        return b''.join(parts)


_CSV_SPECIAL = re.compile(rb'[\",\r\n]')
_QUOTE = re.compile(rb'\"')
_NORMAL, _IN_QUOTES, _QUOTE_PENDING = 0, 1, 2


class CsvScanner:
    """RFC 4180 字节扫描器。

    records() 依次产出 ('header', 结束偏移, 字段名列表) 与
    ('row', 结束偏移, 原始字节字段列表)。结束偏移是该记录终止符之后的位置，
    解析中断后从这里继续。
    """

    def __init__(self, stream, start=0, header=None):
        self.stream = stream
        self.start = start
        self.header = list(header) if header else None
        self.ordinal = 0

    def _header_keys(self, record):
        keys = []
        for index, raw in enumerate(record):
            try:
                text = raw.decode('utf-8', 'strict')
            except UnicodeDecodeError:
                raise ParseError('CSV 表头不是合法 UTF-8，整批拒绝入库')
            if index == 0:
                text = text.lstrip('﻿')
            keys.append(text.strip())
        if not any(keys):
            raise ParseError('CSV 表头为空，整批拒绝入库')
        self.header = keys
        return keys

    def records(self):
        stream = self.stream
        stream.seek(self.start)
        fields = []
        field = bytearray()
        state = _NORMAL
        swallow_lf = False
        header_pending = self.header is None

        def finish(end):
            nonlocal fields, field
            record = fields + [bytes(field)]
            fields, field = [], bytearray()
            if not any(record):
                return None
            if header_pending:
                return ('header', end, self._header_keys(record))
            self.ordinal += 1
            return ('row', end, record)

        while True:
            chunk = stream.read()
            if not chunk:
                break
            base = stream.tell() - len(chunk)
            size = len(chunk)
            index = 0
            while index < size:
                if swallow_lf:
                    swallow_lf = False
                    if chunk[index] == 10:
                        index += 1
                        continue
                if state == _QUOTE_PENDING:
                    if chunk[index] == 34:
                        field.append(34)
                        state = _IN_QUOTES
                        index += 1
                    else:
                        state = _NORMAL
                    continue
                if state == _IN_QUOTES:
                    match = _QUOTE.search(chunk, index)
                    if not match:
                        field += chunk[index:]
                        index = size
                        break
                    field += chunk[index:match.start()]
                    index = match.start() + 1
                    state = _QUOTE_PENDING
                    continue
                match = _CSV_SPECIAL.search(chunk, index)
                if not match:
                    field += chunk[index:]
                    index = size
                    break
                field += chunk[index:match.start()]
                b = chunk[match.start()]
                index = match.start() + 1
                if b == 34:
                    if field:
                        field.append(34)
                    else:
                        state = _IN_QUOTES
                elif b == 44:
                    fields.append(bytes(field))
                    field = bytearray()
                else:
                    item = finish(base + index)
                    swallow_lf = b == 13
                    if item is not None:
                        yield item
                        header_pending = self.header is None
        if state == _IN_QUOTES or state == _QUOTE_PENDING:
            raise ParseError('CSV 最后一行的引号未闭合，整批拒绝入库')
        if fields or field:
            if header_pending:
                raise ParseError('CSV 只有表头，没有数据行，整批拒绝入库')
            item = finish(stream.size)
            if item is not None:
                yield item
        elif header_pending:
            raise ParseError('CSV 文件为空，整批拒绝入库')


_JSON_DECODER = json.JSONDecoder()
_JSON_MAX_RECORD = 16 * 1024 * 1024


class JsonArrayScanner:
    """增量读取 JSON 对象数组，逐条产出 (序号, 结束偏移, 对象)。"""

    def __init__(self, stream, start=0):
        self.stream = stream
        self.start = start
        self.ordinal = 0

    def records(self):
        stream = self.stream
        stream.seek(self.start)
        decoder = codecs.getincrementaldecoder('utf-8')()
        buffer = ''
        base = self.start          # buffer[0] 在文件中的字节偏移
        eof = False

        def drop(count):
            nonlocal buffer, base
            if count:
                base += len(buffer[:count].encode('utf-8'))
                buffer = buffer[count:]

        def fill():
            nonlocal buffer, eof
            data = stream.read()
            if data:
                try:
                    buffer += decoder.decode(data)
                except UnicodeDecodeError:
                    raise ParseError('JSON 文件不是合法 UTF-8，整批拒绝入库')
            else:
                try:
                    buffer += decoder.decode(b'', final=True)
                except UnicodeDecodeError:
                    raise ParseError('JSON 文件不是合法 UTF-8，整批拒绝入库')
                eof = True

        def skip_space():
            stripped = buffer.lstrip(' \t\r\n')
            drop(len(buffer) - len(stripped))

        # 断点续传：偏移指向已读记录之后，接下来应是逗号或右括号
        started = self.start > 0
        expect_value = not started
        while True:
            skip_space()
            if not buffer:
                if eof:
                    raise ParseError('JSON 文件为空或数组未闭合，整批拒绝入库')
                fill()
                continue
            if not started:
                if buffer[0] != '[':
                    raise ParseError('JSON 根节点必须是对象数组，整批拒绝入库')
                started = True
                drop(1)
                continue
            if buffer[0] == ']':
                if expect_value and self.ordinal:
                    raise ParseError('JSON 数组尾随逗号，整批拒绝入库')
                drop(1)
                skip_space()
                while not eof and not buffer:
                    fill()
                    skip_space()
                if buffer:
                    raise ParseError('JSON 数组后存在多余内容，整批拒绝入库')
                return
            if expect_value:
                try:
                    obj, end = _JSON_DECODER.raw_decode(buffer)
                except ValueError:
                    if len(buffer) > _JSON_MAX_RECORD:
                        raise ParseError('JSON 单条记录过大或格式错误，整批拒绝入库',
                                         failed=[{'row': self.ordinal + 1, 'reason': '单条记录格式错误'}])
                    if eof:
                        line = self.ordinal + 1
                        raise ParseError(f'JSON 第 {line} 条记录不完整或格式错误（末行坏数据），整批拒绝入库',
                                         failed=[{'row': line, 'reason': '记录不完整或格式错误'}])
                    fill()
                    continue
                if not isinstance(obj, dict):
                    line = self.ordinal + 1
                    raise ParseError(f'JSON 第 {line} 条记录不是对象，整批拒绝入库',
                                     failed=[{'row': line, 'reason': '记录不是对象'}])
                self.ordinal += 1
                end_offset = base + len(buffer[:end].encode('utf-8'))
                drop(end)
                expect_value = False
                yield self.ordinal, end_offset, obj
                continue
            # 记录之后只允许逗号或右括号
            if buffer[0] == ',':
                drop(1)
                expect_value = True
                continue
            raise ParseError('JSON 数组元素之间缺少逗号，整批拒绝入库')


def sniff_json(stream):
    """根据开头字节判断是否为 JSON 数组。"""
    pos = stream.tell()
    head = stream.read(4096)
    stream.seek(pos)
    for index, b in enumerate(head):
        if b in b' \t\r\n':
            continue
        return b == 91                       # [
    return False

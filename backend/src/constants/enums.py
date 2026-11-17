from enum import Enum
class Role(str, Enum):
    admin = 'admin'
    analyst = 'analyst'
    viewer = 'viewer'
class JobStatus(str, Enum):
    queued = 'queued'
    running = 'running'
    completed = 'completed'
    failed = 'failed'

class UploadStatus(str, Enum):
    uploading = 'uploading'
    uploaded = 'uploaded'
    parsing = 'parsing'
    parsing_failed = 'parsing_failed'
    completed = 'completed'
    failed = 'failed'

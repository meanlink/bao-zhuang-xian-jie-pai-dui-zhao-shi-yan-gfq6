from pydantic import BaseModel, Field


class DatasetInput(BaseModel):
    name: str = Field(min_length=2, max_length=160)
    source: str = Field(default='manual.json', max_length=200)
    records: list[dict] = Field(min_length=5, max_length=5000)


class UploadInput(BaseModel):
    name: str = Field(min_length=2, max_length=160)
    source: str = Field(default='upload.bin', max_length=200)
    size: int
    sha256: str = Field(default='', max_length=64)
    chunk_size: int | None = Field(default=None)

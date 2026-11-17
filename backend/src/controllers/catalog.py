from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, func
from src.config.database import get_db
from src.middlewares.auth import current_user
from src.models import Dataset, Job, Report, Audit
from src.services.business import metadata
router=APIRouter()
@router.get('/meta')
def meta():return metadata()
@router.get('/overview')
def overview(db=Depends(get_db),user=Depends(current_user)):
    ready=select(func.count()).select_from(Dataset).where(Dataset.status=='ready')
    # 小批批次保留原 JSON 记录；大文件批次按行表计数，未完成批次不计入
    small_rows=sum(len(d.records or []) for d in db.scalars(
        select(Dataset).where(Dataset.status=='ready', Dataset.upload_id.is_(None))))
    big_rows=db.scalar(select(func.coalesce(func.sum(Dataset.total_rows),0)).select_from(Dataset).where(Dataset.status=='ready',Dataset.upload_id.is_not(None))) or 0
    return {'datasets':db.scalar(ready), 'records':int(small_rows)+int(big_rows),
            'upload_datasets':db.scalar(select(func.count()).select_from(Dataset).where(Dataset.status=='ready',Dataset.upload_id.is_not(None))),
            'upload_pending':db.scalar(select(func.count()).select_from(Dataset).where(Dataset.status!='ready')),
            'jobs':db.scalar(select(func.count()).select_from(Job)), 'approved':db.scalar(select(func.count()).select_from(Job).where(Job.approved==True)), 'reports':db.scalar(select(func.count()).select_from(Report))}
@router.get('/audits')
def audits(db=Depends(get_db),user=Depends(current_user)):
    return [{'id':a.id,'actor':a.actor,'action':a.action,'detail':a.detail,'at':a.at} for a in db.scalars(select(Audit).order_by(Audit.id.desc()).limit(50))]

/** @typedef {{id:number,name:string,source:string,rows:number,checksum:string,status:string,upload_id:number|null}} Dataset */
/** @typedef {{id:number,name:string,source:string,size:number,sha256:string,chunk_size:number,total_chunks:number,received_chunks:number[],missing_chunks:number[],status:string,parsed_rows:number,parse_progress:number,failed_rows:{row:number,reason:string}[],dataset_id:number|null}} UploadSession */
/** @typedef {{id:number,name:string,dataset_id:number,status:string,result:Object,approved:boolean}} AnalysisJob */
/** @typedef {{id:number,name:string,job_id:number,destination:number,next_run:number,enabled:boolean}} ReportSchedule */
export {};

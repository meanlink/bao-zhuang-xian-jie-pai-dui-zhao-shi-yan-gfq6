import {datasets,jobs,uploads} from '../api/entities.js';
import {workspace} from '../stores/workspace.js';
import {session} from '../stores/session.js';
import {heading} from '../components/common/shell.js';
import {table} from '../components/common/table.js';
import {esc,notify} from '../utils/dom.js';
import {submit} from '../hooks/forms.js';
import {parseCSV} from '../utils/csv.js';

const POLL_MS=2500;

const STATUS={uploading:'接收分片中',uploaded:'待解析',parsing:'解析中',failed:'已拒绝',completed:'已完成'};
const openRowPages=new Map();

function pct(v){return Math.round((v||0)*100)+'%';}

function chunkGrid(u){
 const total=u.total_chunks,shown=Math.min(total,120),ranges=u.received_ranges||[],missing=u.missing_chunks||[];
 const inReceived=i=>ranges.some(([start,end])=>i>=start&&i<=end);
 const cells=[];
 for(let i=0;i<shown;i++){const ok=inReceived(i);cells.push('<span class="chunk '+(ok?'ok':'missing')+'" title="分片 '+i+(ok?' 已接收':' 缺失')+'"></span>');}
 return '<div class="chunk-grid">'+cells.join('')+(total>shown?'<span class="chunk-more">+'+(total-shown)+'</span>':'')+'</div>';
}

function uploadCard(u){
 const missing=u.missing_chunks||[];
 const missingCount=u.missing_count??missing.length;
 const failRows=(u.failed_rows||[]).slice(0,8);
 const visible=u.status==='completed';
 return '<article class="upload-card" data-upload="'+u.id+'"><header><div><strong>'+esc(u.name)+'</strong><p>'+esc(u.source)+' · '+(u.size/1048576).toFixed(2)+' MB · '+u.total_chunks+' 片</p></div><span class="pill '+esc(u.status)+'">'+esc(STATUS[u.status]||u.status)+'</span></header>'+
  '<div class="upload-meta"><span>已收 <strong>'+u.received_count+'</strong>/'+u.total_chunks+' 片</span><span>缺片 '+(missingCount?'<strong class="bad">'+missingCount+'</strong>':'0')+'</span><span>解析进度 <strong>'+pct(u.parse_progress)+'</strong>（'+u.parsed_rows+' 条）</span></div>'+
  chunkGrid(u)+
  (missingCount?'<p class="hint bad">缺失分片：'+missing.slice(0,24).map(esc).join('、')+(missingCount>24?' …':'')+'（可重新选择同一文件续传）</p>':'')+
  '<div class="parse-bar"><i style="width:'+pct(u.parse_progress)+'"></i></div>'+
  (u.failed_rows?.length?'<div class="failed-rows"><strong>失败行（整批未入库）</strong><ul>'+failRows.map(r=>'<li>第 '+esc(r.row)+' 条：'+esc(r.reason)+'</li>').join('')+'</ul>'+(u.failed_rows.length>failRows.length?'<small>另有 '+(u.failed_rows.length-failRows.length)+' 条…</small>':'')+'</div>':'')+
  (u.error&&u.status!=='completed'?'<p class="hint bad">'+esc(u.error)+'</p>':'')+
  '<p class="hint">原文件指纹 '+(u.fingerprint_actual?'<code>'+esc(u.fingerprint_actual.slice(0,24))+'…</code> 已核对':'待全部到齐后核对')+'；'+
  (u.sha256?'声明 '+esc(u.sha256.slice(0,16))+'…':'未声明（按实际内容登记）')+'</p>'+
  (visible?'<div class="upload-actions"><span class="pill ready">最终批次 #'+u.dataset_id+' · '+u.parsed_rows+' 条</span><button class="button small" data-rows="'+u.id+'">分页核对</button><button class="button small" data-compare="'+u.id+'">原节拍比较</button><button class="button small" data-preview="'+u.dataset_id+'">查看批次</button></div>':
   (u.status==='failed'?'<div class="upload-actions"><span class="hint">失败批次对查询与分析不可见，未写入任何记录。</span></div>':''))+
  '<div class="row-page" id="rows-'+u.id+'"></div></article>';
}

export async function dataPage(){
 workspace.datasets=await datasets.list();
 let uploadList=[];
 try{uploadList=await uploads.list();}catch(e){uploadList=[];}
 const selected=workspace.datasets[0]?await datasets.get(workspace.datasets[0].id):null,m=workspace.meta;
 return heading('02 / DATA INTAKE','数据接入与批次追溯','超大文件按分片续传，全部到齐并核对原文件指纹后才解析。未完成批次对查询与分析不可见；原小批导入保留。')+
 '<div class="dataset-grid"><section class="panel"><div class="section-title"><h2>业务批次</h2><span>'+workspace.datasets.length+' 批可分析</span></div>'+
 '<div class="batch-list">'+workspace.datasets.map(d=>'<button class="batch-item" data-dataset="'+d.id+'"><strong>'+esc(d.name)+'</strong><span>'+d.rows+' 条 · '+esc(d.source)+(d.upload_id?' · 分片接入':'')+'</span><small>SHA256 '+esc(d.checksum.slice(0,18))+'…</small></button>').join('')+'</div>'+
 '<div class="section-title"><h2>超大文件分批接入</h2><span>分片续传 · 指纹校验 · 流式解析</span></div>'+
 '<form id="upload-form"><label>批次名称<input name="name" required minlength="2" placeholder="例如：11 月全量秒表导出"></label>'+
 '<label>选择 CSV 或 JSON 原文件<input type="file" id="big-file" accept=".json,.csv,.txt"></label>'+
 '<div class="parameter-grid"><label>分片大小<select name="chunk_size"><option value="1048576">1 MB</option><option value="4194304" selected>4 MB</option><option value="8388608">8 MB</option></select></label>'+
 '<label>原文件 SHA256（可选）<input name="sha256" placeholder="64 位十六进制，用于原文件指纹校验"></label></div>'+
 '<button type="submit" class="primary" '+(session.user.role==='viewer'?'disabled':'')+'>上传并校验</button>'+
 '<p class="hint" id="upload-progress">选择文件后按分片上传；中断后重新选择同一文件会只补缺片。相同位置不同内容会被拒收。</p></form>'+
 '<div class="section-title"><h2>接入单</h2><span>缺片、解析进度与最终批次</span></div><div class="upload-list">'+(uploadList.length?uploadList.map(uploadCard).join(''):'<p class="empty">尚无超大文件接入单。</p>')+'</div></section>'+
 '<section class="panel"><div class="section-title"><h2>原小批导入（5–5,000 条）</h2><span>保留现有方式</span></div>'+
 '<form id="dataset-form"><label>批次名称<input name="name" required minlength="2" placeholder="例如：11 月复测批次"></label><label>上传 CSV 或 JSON<input type="file" id="file-input" accept=".json,.csv"></label><label>记录 JSON<textarea name="records" rows="6" placeholder="粘贴 JSON 数组，或先选择上方文件" required></textarea></label><p class="hint">字段：'+m.columns.map(c=>esc(c.key)+' / '+esc(c.label)).join('，')+'。5–5,000 条记录。</p><button type="submit" class="primary" '+(session.user.role==='viewer'?'disabled':'')+'>校验并接入批次</button></form>'+
 '<div class="section-title"><h2 id="preview-name">'+esc(selected?.name||'尚无批次')+'</h2><span>预览前 30 条</span></div><div id="data-preview">'+table(m.columns,selected?.records.slice(0,30)||[])+'</div></section></div>';
}

async function uploadFile(file,form,onStatus){
 const name=form.get('name').trim(),chunkSize=Number(form.get('chunk_size')),declared=(form.get('sha256')||'').trim().toLowerCase();
 const progress=document.querySelector('#upload-progress');
 const setStatus=text=>{progress.textContent=text;onStatus&&onStatus(text);};
 let sessionRow;
 try{
  sessionRow=await uploads.create({name,source:file.name,size:file.size,sha256:declared,chunk_size:chunkSize});
 }catch(error){
  if(!/已存在|正在|稍后/.test(error.message))throw error;
  // 同名接入单已存在：视为同一上传单续传
  const list=await uploads.list();
  sessionRow=list.find(u=>u.name===name&&u.size===file.size);
  if(!sessionRow)throw error;
  if(sessionRow.status==='failed')throw new Error(sessionRow.error||'同名接入单已失败，请更换批次名称');
 }
 const total=sessionRow.total_chunks;
 let done=new Set();
 if(sessionRow.received_chunks)done=new Set(sessionRow.received_chunks);
 else{
  // 分片很多时后端只返回区间；按区间判断已经到位的片
  const ranges=sessionRow.received_ranges||[];
  for(const [start,end] of ranges)for(let i=start;i<=end;i++)done.add(i);
 }
 for(let seq=0;seq<total;seq++){
  if(done.has(seq))continue;
  const start=seq*sessionRow.chunk_size,end=Math.min(file.size,start+sessionRow.chunk_size);
  const blob=file.slice(start,end);
  let attempt=0;
  while(true){
   try{await uploads.putChunk(sessionRow.id,seq,blob);done.add(seq);break;}
   catch(error){
    attempt++;
    if(attempt>=3)throw new Error('分片 '+seq+' 上传失败：'+error.message);
    await new Promise(r=>setTimeout(r,400*attempt));
   }
  }
  setStatus('已上传 '+done.size+' / '+total+' 片（'+(file.size?(Math.min(file.size,done.size*sessionRow.chunk_size)/file.size*100).toFixed(0):0)+'%），可随时中断后继续');
 }
 setStatus('全部分片到齐，正在核对原文件指纹…');
 const result=await uploads.complete(sessionRow.id);
 if(result.status==='failed')throw new Error(result.error||'原文件指纹校验失败，整批拒绝解析');
 if(result.status==='completed'){setStatus('该上传单此前已解析完成，最终批次 #'+result.dataset_id+' 可核对。');return result;}
 setStatus('指纹核对通过，后台解析中；可关闭页面，稍后回来查看进度。');
 return result;
}

function bindUpload(statusRefresh){
 const form=document.querySelector('#upload-form');
 if(!form)return;
 let chosen=null;
 form.querySelector('#big-file').addEventListener('change',e=>{chosen=e.target.files[0]||null;if(chosen)document.querySelector('#upload-progress').textContent='已选择 '+chosen.name+' · '+(chosen.size/1048576).toFixed(2)+' MB';});
 submit(form,async f=>{
  if(!chosen)throw new Error('请先选择要接入的文件');
  const result=await uploadFile(chosen,f);
  notify('分片已到齐，开始解析；完成后批次才可分析与核对');
  await statusRefresh();
  pollUploads();
 });
}

let pollTimer=null;
function pollUploads(){
 clearTimeout(pollTimer);
 pollTimer=setTimeout(async()=>{
  if(location.hash.replace(/^#\//,'').split('/')[0]!=='datasets')return;
  try{
   const list=await uploads.list();
   const active=list.some(u=>u.status==='parsing'||u.status==='uploaded');
   const host=document.querySelector('.upload-list');
   if(host){host.innerHTML=list.length?list.map(uploadCard).join(''):'<p class="empty">尚无超大文件接入单。</p>';bindRows();
    // 刷新时不打断正在核对的分页
    for(const [uploadId,page] of openRowPages)showRows(uploadId,page);}
   if(active)pollUploads();
  }catch(e){}
 },POLL_MS);
}

async function showRows(uploadId,page=1){
 const host=document.querySelector('#rows-'+uploadId);
 if(!host)return;
 openRowPages.set(uploadId,page);
 host.innerHTML='<p class="hint">正在读取…</p>';
 try{
  const data=await uploads.rows(uploadId,page,20);
  const columns=workspace.meta.columns;
  host.innerHTML='<div class="section-title"><h2>分页核对</h2><span>第 '+data.page+' 页 · 共 '+data.total+' 条 · 序 '+esc(data.first_seq??'—')+'–'+esc(data.last_seq??'—')+'</span></div>'+table(columns,data.rows)+
   '<div class="pager"><button class="button small" data-page="'+(page-1)+'" '+(page<=1?'disabled':'')+'>上一页</button><span>'+page+' / '+Math.max(1,Math.ceil(data.total/20))+'</span><button class="button small" data-page="'+(page+1)+'" '+(page*20>=data.total?'disabled':'')+'>下一页</button></div>';
  host.querySelectorAll('[data-page]').forEach(b=>b.onclick=()=>showRows(uploadId,Number(b.dataset.page)));
 }catch(error){host.innerHTML='<p class="hint bad">'+esc(error.message)+'</p>';}
}

function bindRows(){
 document.querySelectorAll('[data-rows]').forEach(b=>b.onclick=()=>showRows(b.dataset.rows));
 document.querySelectorAll('[data-preview]').forEach(b=>b.onclick=async()=>{
  try{
   const d=await datasets.get(b.dataset.preview);
   document.querySelector('#preview-name').textContent=d.name;
   document.querySelector('#data-preview').innerHTML=table(workspace.meta.columns,d.records.slice(0,30));
  }catch(error){notify(error.message,true);}
 });
 document.querySelectorAll('[data-compare]').forEach(b=>b.onclick=async()=>{
  try{
   const u=await uploads.get(b.dataset.compare);
   const d=await datasets.get(u.dataset_id);
   // 用整个批次执行原节拍 Welch 比较，结果页可核对参数与区间
   const j=await jobs.create({name:d.name+' · 原节拍比较',dataset_id:d.id,params:{}});
   notify('已提交原节拍比较，正在计算');
   location.hash='#/results/'+j.id;
  }catch(error){notify(error.message,true);}
 });
}

export function bindData(refresh){
 let source='manual.json';
 document.querySelector('#file-input').addEventListener('change',async e=>{try{const file=e.target.files[0];if(!file)return;source=file.name;const text=await file.text(),records=file.name.endsWith('.csv')?parseCSV(text):JSON.parse(text);document.querySelector('[name=records]').value=JSON.stringify(records,null,2);}catch(error){notify(error.message,true);}});
 document.querySelectorAll('[data-dataset]').forEach(button=>button.onclick=async()=>{try{const d=await datasets.get(button.dataset.dataset);document.querySelector('#preview-name').textContent=d.name;document.querySelector('#data-preview').innerHTML=table(workspace.meta.columns,d.records.slice(0,30));}catch(error){notify(error.message,true);}});
 submit(document.querySelector('#dataset-form'),async f=>{await datasets.create({name:f.get('name'),source,records:JSON.parse(f.get('records'))});notify('数据已接入，可前往工作台执行分析');await refresh();});
 bindUpload(refresh);bindRows();pollUploads();
}
